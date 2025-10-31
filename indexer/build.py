from __future__ import annotations

import argparse
import logging
import sys
from dataclasses import dataclass, field
from pathlib import Path
from typing import Callable, Dict, Iterable, Optional, Sequence, List, Tuple
import json

from config_loader import ConfigError, load_yaml_config

logger = logging.getLogger("indexer.build")

from .config import IndexerConfig
from .idf import (
    IDFCalculator,
    IDFRegistry,
    SUPPORTED_IDF_METHODS,
)
from .ingest import DocumentRecord, build_vocabulary
from .store import write_docs, write_manifest, write_postings


def _load_token_counter(model: str) -> Callable[[str], int]:
    try:
        import tiktoken  # type: ignore
    except ImportError as exc:  # pragma: no cover - optional dependency
        raise RuntimeError(
            "tiktoken is not installed; install it or omit --use-tokens"
        ) from exc

    try:
        encoding = tiktoken.get_encoding(model)
    except Exception:  # pragma: no cover - fallback path
        encoding = tiktoken.encoding_for_model(model)

    def counter(text: str) -> int:
        payload = text or ""
        try:
            return len(encoding.encode(payload, disallowed_special=()))
        except ValueError as exc:  # pragma: no cover - defensive fallback
            logging.getLogger("indexer.tiktoken").warning(
                "tiktoken rejected input, falling back to strict token count: %s",
                exc,
            )
            return len(encoding.encode(payload, allowed_special="all"))

    return counter


@dataclass(frozen=True)
class BuildSummary:
    documents: int = 0
    terms: int = 0
    # list of computed IDF methods included in the build
    idf_methods: Sequence[str] = field(default_factory=list)

    def as_dict(self) -> Dict[str, object]:
        return {
            "documents": self.documents,
            "terms": self.terms,
            "idf_methods": list(self.idf_methods),
        }


@dataclass
class BuildOptions:
    input_dir: Path
    output_dir: Path
    limit: Optional[int] = None
    dry_run: bool = False
    use_tokens: bool = False
    token_model: str = "cl100k_base"
    # When set, process documents in chunks of this many documents (streaming).
    # Default 1000: balances memory use and IO for collections of simple HTML/text files.
    # If set to None, existing in-memory behaviour is used.
    chunk_size: Optional[int] = 1000
    methods: Iterable[str] = field(default_factory=lambda: SUPPORTED_IDF_METHODS)


class IndexWriter:
    """Facade over store.py helpers to keep the build pipeline cohesive."""

    def __init__(self, output_dir: Path) -> None:
        self.output_dir = output_dir

    def write(
        self,
        documents: Sequence[DocumentRecord],
        vocabulary: Dict[str, Dict[int, list]],
        idf_tables: Dict[str, Dict[str, float]],
        *,
        available_methods: Iterable[str],
    ) -> None:
        self.output_dir.mkdir(parents=True, exist_ok=True)
        write_docs(self.output_dir, documents)
        write_postings(self.output_dir, vocabulary, idf_tables)
        write_manifest(
            self.output_dir,
            total_docs=len(documents),
            total_terms=len(vocabulary),
            idf_methods=list(available_methods),
        )


class IndexBuilder:
    """High-level coordinator responsible for producing index artefacts."""

    def __init__(self, options: BuildOptions) -> None:
        self.options = options
        self.registry = IDFRegistry()
        self.writer = IndexWriter(options.output_dir)
        self.methods = self.registry.ensure_many(self.options.methods)

    def build(self) -> BuildSummary:
        # Always run the streaming chunked builder to avoid loading all docs.
        return self._build_in_chunks()

    def _load_documents(self) -> list[DocumentRecord]:
        # Deprecated: indexing must be streaming-only. This method is removed.
        raise RuntimeError("_load_documents is deprecated; use chunked indexing only")

    def _build_in_chunks(self) -> BuildSummary:
        """Process documents in chunks and produce final index by merging partials.

        This method streams documents from `ingest.iter_document_records`, writes
        `docs.jsonl` incrementally, writes partial postings per chunk and then
        merges partials into the final `postings.jsonl` using a k-way merge.
        """
        from .ingest import iter_document_records
        from .store import append_docs, write_partial_postings
        import heapq

        # Determine and validate chunk size
        if self.options.chunk_size is None:
            raise ValueError("chunk_size must be set (positive integer) for streaming indexing")
        try:
            chunk_size = int(self.options.chunk_size)
        except Exception:
            raise ValueError("chunk_size must be an integer")
        if chunk_size < 1:
            raise ValueError("chunk_size must be >= 1")

        chunk: List[DocumentRecord] = []
        part_idx = 0
        total_docs = 0
        partial_dir = self.options.output_dir / "partial"
        partial_dir.mkdir(parents=True, exist_ok=True)

        # Ensure we start with a clean docs/postings file for this build
        docs_path = self.options.output_dir / "docs.jsonl"
        if docs_path.exists():
            docs_path.unlink()
        postings_out_path = self.options.output_dir / "postings.jsonl"
        if postings_out_path.exists():
            postings_out_path.unlink()

        token_counter: Optional[Callable[[str], int]] = None
        if self.options.use_tokens:
            token_counter = _load_token_counter(self.options.token_model)

        # Stream documents and write partial postings per chunk
        logger.info(
            "Starting chunked build: input=%s output=%s chunk_size=%d",
            str(self.options.input_dir),
            str(self.options.output_dir),
            chunk_size,
        )

        for record in iter_document_records(self.options.input_dir, limit=self.options.limit, token_counter=token_counter):
            chunk.append(record)
            total_docs += 1
            if len(chunk) >= chunk_size:
                # append docs metadata
                append_docs(self.options.output_dir, chunk)
                logger.info("Appended %d docs to %s/docs.jsonl (total=%d)", len(chunk), str(self.options.output_dir), total_docs)
                # build vocabulary for chunk and write partial
                vocab = build_vocabulary(chunk)
                part_path = partial_dir / f"part_{part_idx:04d}.jsonl"
                write_partial_postings(part_path, vocab)
                logger.info("Wrote partial postings %s (terms=%d)", str(part_path), len(vocab))
                part_idx += 1
                chunk = []

        # Handle final partial chunk
        if chunk:
            append_docs(self.options.output_dir, chunk)
            logger.info("Appended %d docs to %s/docs.jsonl (total=%d)", len(chunk), str(self.options.output_dir), total_docs)
            vocab = build_vocabulary(chunk)
            part_path = partial_dir / f"part_{part_idx:04d}.jsonl"
            write_partial_postings(part_path, vocab)
            logger.info("Wrote partial postings %s (terms=%d)", str(part_path), len(vocab))
            part_idx += 1
            chunk = []

        # If dry-run, we only wanted metadata counts
        summary = BuildSummary(documents=total_docs, terms=0, idf_methods=tuple(self.methods))
        if self.options.dry_run:
            return summary

        # Merge partial postings into final postings.jsonl in a streaming manner
        partial_files = sorted(partial_dir.glob("part_*.jsonl"))
        logger.info("Merging %d partial posting files from %s", len(partial_files), str(partial_dir))
        if not partial_files:
            # no content; write empty manifest and empty postings
            self.writer.write([], {}, {method: {} for method in self.methods}, available_methods=self.methods)
            return summary

        # Open all partials and create iterators over (term, postings_list)
        file_iters = []
        for p in partial_files:
            fh = p.open("r", encoding="utf-8")

            def gen(fh=fh):
                for line in fh:
                    if not line.strip():
                        continue
                    obj = json.loads(line)
                    yield obj.get("term"), obj.get("postings", [])
                fh.close()

            it = gen()
            try:
                first_term, first_postings = next(it)
            except StopIteration:
                continue
            file_iters.append([first_term, first_postings, it])

        # Build a heap keyed by current term for each iterator
        heap: List[Tuple[str, int]] = []
        for idx, (term, postings_list, it) in enumerate(file_iters):
            heap.append((term, idx))
        heapq.heapify(heap)

        postings_out_path = self.options.output_dir / "postings.jsonl"
        out_lines: List[str] = []

        # idf calculator
        idf_calculator = IDFCalculator(total_docs, registry=self.registry, methods=self.methods)
        terms_count = 0

        while heap:
            term, src_idx = heapq.heappop(heap)
            # merged postings across all iterators that currently point to `term`
            merged_postings: Dict[int, List[int]] = {}

            def collect_from_index(i: int) -> None:
                t_i, p_i, it_i = file_iters[i]
                # p_i is list of posting items
                for item in p_i:
                    doc_id = int(item.get("doc_id"))
                    positions = [int(p) for p in item.get("positions", [])]
                    merged_postings.setdefault(doc_id, []).extend(positions)
                # advance iterator i
                try:
                    nxt_term, nxt_postings = next(it_i)
                    file_iters[i][0] = nxt_term
                    file_iters[i][1] = nxt_postings
                    heapq.heappush(heap, (nxt_term, i))
                except StopIteration:
                    file_iters[i][0] = None
                    file_iters[i][1] = None

            # collect from the popped source
            collect_from_index(src_idx)
            # also collect from other iterators that have the same current term
            while heap and heap[0][0] == term:
                _, other_idx = heapq.heappop(heap)
                collect_from_index(other_idx)

            # produce final merged postings for this term
            postings_list_out = [
                {"doc_id": doc_id, "tf": len(sorted(set(positions))), "positions": sorted(set(positions))}
                for doc_id, positions in sorted(merged_postings.items())
            ]
            df = len(postings_list_out)
            idf_map = {method: float(idf_calculator.compute(df, method)) for method in self.methods}
            payload = {"term": term, "df": df, "idf": idf_map, "postings": postings_list_out}
            out_lines.append(json.dumps(payload, ensure_ascii=False))
            terms_count += 1
            # flush periodically
            if len(out_lines) >= 1000:
                with postings_out_path.open("a", encoding="utf-8") as fh:
                    fh.write("\n".join(out_lines) + "\n")
                out_lines = []

            # log progress every 1000 terms merged
            if terms_count % 1000 == 0:
                logger.info("Merged %d terms so far...", terms_count)

        # final flush
        if out_lines:
            with postings_out_path.open("a", encoding="utf-8") as fh:
                fh.write("\n".join(out_lines) + "\n")

        # write manifest
        write_manifest(
            self.options.output_dir,
            total_docs=total_docs,
            total_terms=terms_count,
            idf_methods=list(self.methods),
        )

        # Note: docs.jsonl has been appended incrementally. We don't rewrite docs here.
        return BuildSummary(documents=total_docs, terms=terms_count, idf_methods=tuple(self.methods))


def _parse_args(argv: Optional[list[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Build a lightweight inverted index.")
    registry = IDFRegistry()
    parser.add_argument(
        "--config",
        default="config.yml",
        help="Path to unified configuration file (default: %(default)s).",
    )
    parser.add_argument(
        "--input",
        dest="input_dir",
        default=None,
        help="Directory containing text documents (defaults to indexer.build.input_dir).",
    )
    parser.add_argument(
        "--output",
        dest="output_dir",
        default=None,
        help="Directory where index artefacts will be written (defaults to indexer.build.output_dir).",
    )
    # NOTE: build no longer accepts a default idf method. The index records
    # the set of computed IDF methods and search-time code selects which
    # method to use. This keeps build focused on computing/storing multiple
    # IDF tables and avoids baking a single default into the index.
    parser.add_argument(
        "--limit",
        type=int,
        help="Optional upper bound on number of documents to index (for testing).",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Inspect the dataset without writing index files.",
    )
    parser.add_argument(
        "--use-tokens",
        action="store_true",
        help="Include tiktoken-based counts in docs.jsonl (requires optional dependency).",
    )
    parser.add_argument(
        "--token-model",
        dest="token_model",
        default=None,
        help="Token model identifier used when --use-tokens is enabled (defaults to indexer.build.token_model).",
    )
    parser.add_argument(
        "--chunk-size",
        dest="chunk_size",
        type=int,
        default=1000,
        help="Process documents in chunks of this size (avoids loading all documents into memory). Default: 1000.",
    )
    return parser.parse_args(argv)


def build_index(
    input_dir: Path,
    output_dir: Path,
    *,
    limit: Optional[int] = None,
    dry_run: bool = False,
    use_tokens: bool = False,
    token_model: str = "cl100k_base",
    chunk_size: Optional[int] = None,
) -> Dict[str, object]:
    if not input_dir.exists():
        raise FileNotFoundError(f"Input directory does not exist: {input_dir}")

    registry = IDFRegistry()

    options = BuildOptions(
        input_dir=input_dir,
        output_dir=output_dir,
        limit=limit,
        dry_run=dry_run,
        use_tokens=use_tokens,
        token_model=token_model,
        chunk_size=chunk_size if chunk_size is not None else 1000,
        methods=registry.supported_methods,
    )

    builder = IndexBuilder(options)
    summary = builder.build()
    return summary.as_dict()


def main(argv: Optional[list[str]] = None) -> int:
    args = _parse_args(argv)

    # Ensure basic logging configuration so progress messages are visible by default
    if not logging.getLogger().hasHandlers():
        logging.basicConfig(level=logging.INFO, format="[%(levelname)s] %(message)s")

    try:
        app_config = load_yaml_config(args.config)
    except ConfigError as exc:
        print(f"[error] {exc}", file=sys.stderr)
        return 1

    indexer_config = IndexerConfig.from_app_config(app_config)
    build_cfg = indexer_config.build

    input_dir_str = args.input_dir or build_cfg.input_dir
    output_dir_str = args.output_dir or build_cfg.output_dir
    limit = args.limit if args.limit is not None else build_cfg.limit
    use_tokens = bool(args.use_tokens or build_cfg.use_tokens)
    dry_run = bool(args.dry_run or build_cfg.dry_run)
    token_model = args.token_model or build_cfg.token_model
    chunk_size = args.chunk_size

    input_dir = Path(input_dir_str).resolve()
    output_dir = Path(output_dir_str).resolve()

    try:
        summary = build_index(
            input_dir,
            output_dir,
            limit=limit,
            dry_run=dry_run,
            use_tokens=use_tokens,
            token_model=token_model,
            chunk_size=chunk_size,
        )
    except RuntimeError as exc:
        print(f"[error] {exc}", file=sys.stderr)
        return 1
    except ValueError as exc:
        print(f"[error] {exc}", file=sys.stderr)
        return 1
    computed_methods = summary.get("idf_methods")

    if dry_run:
        print(
            f"[dry-run] Documents: {summary['documents']}, vocabulary size: {summary['terms']}, "
            f"idf_methods={computed_methods}"
        )
    else:
        print(
            f"Index written to {output_dir} "
            f"(documents={summary['documents']}, terms={summary['terms']}, idf_methods={computed_methods})"
        )

    logger.info("Build finished: documents=%d, terms=%d", summary["documents"], summary["terms"]) 

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
