from __future__ import annotations

import argparse
import math
import sys
import logging
from pathlib import Path
from typing import Callable, Dict, Optional

from .ingest import build_vocabulary, load_documents
from .store import write_docs, write_manifest, write_postings

SUPPORTED_IDF_METHODS = {"log", "rsj"}


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


def compute_idf(df: int, total_docs: int, method: str) -> float:
    if total_docs <= 0:
        return 0.0

    df = max(df, 0)

    if method == "log":
        # Traditional smoothed log IDF with additive constant for positive scores.
        return math.log((total_docs + 1.0) / (df + 1.0)) + 1.0

    if method == "rsj":
        numerator = total_docs - df + 0.5
        denominator = df + 0.5
        if numerator <= 0.0:
            numerator = 0.5
        if denominator <= 0.0:
            denominator = 0.5
        return math.log(numerator / denominator)

    raise ValueError(f"Unsupported IDF method: {method}")


def _parse_args(argv: Optional[list[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Build a lightweight inverted index.")
    parser.add_argument(
        "--input",
        default="workspace/store/text",
        help="Directory containing text documents (default: %(default)s).",
    )
    parser.add_argument(
        "--output",
        default="workspace/store/index/default",
        help="Directory where index artefacts will be written (default: %(default)s).",
    )
    parser.add_argument(
        "--idf-method",
        default="log",
        choices=sorted(SUPPORTED_IDF_METHODS),
        help="IDF weighting strategy (default: %(default)s).",
    )
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
        default="cl100k_base",
        help="Token model identifier used when --use-tokens is enabled (default: %(default)s).",
    )
    return parser.parse_args(argv)


def build_index(
    input_dir: Path,
    output_dir: Path,
    *,
    idf_method: str = "log",
    limit: Optional[int] = None,
    dry_run: bool = False,
    use_tokens: bool = False,
    token_model: str = "cl100k_base",
) -> Dict[str, int]:
    if idf_method not in SUPPORTED_IDF_METHODS:
        raise ValueError(f"Unsupported idf_method: {idf_method}")

    if not input_dir.exists():
        raise FileNotFoundError(f"Input directory does not exist: {input_dir}")

    token_counter: Optional[Callable[[str], int]] = None
    if use_tokens:
        token_counter = _load_token_counter(token_model)

    documents = load_documents(input_dir, limit=limit, token_counter=token_counter)
    total_docs = len(documents)

    if total_docs == 0:
        summary = {"documents": 0, "terms": 0}
        if not dry_run:
            output_dir.mkdir(parents=True, exist_ok=True)
            write_docs(output_dir, [])
            write_postings(output_dir, {}, {})
            write_manifest(
                output_dir,
                total_docs=0,
                total_terms=0,
                idf_method=idf_method,
                idf_methods=sorted(SUPPORTED_IDF_METHODS),
            )
        return summary

    vocabulary = build_vocabulary(documents)
    idf_tables = {
        method: {
            term: compute_idf(df=len(postings), total_docs=total_docs, method=method)
            for term, postings in vocabulary.items()
        }
        for method in SUPPORTED_IDF_METHODS
    }

    summary = {"documents": total_docs, "terms": len(vocabulary)}

    if dry_run:
        return summary

    output_dir.mkdir(parents=True, exist_ok=True)
    write_docs(output_dir, documents)
    write_postings(output_dir, vocabulary, idf_tables)
    write_manifest(
        output_dir,
        total_docs=total_docs,
        total_terms=len(vocabulary),
        idf_method=idf_method,
        idf_methods=sorted(SUPPORTED_IDF_METHODS),
    )

    return summary


def main(argv: Optional[list[str]] = None) -> int:
    args = _parse_args(argv)
    input_dir = Path(args.input).resolve()
    output_dir = Path(args.output).resolve()

    try:
        summary = build_index(
            input_dir,
            output_dir,
            idf_method=args.idf_method,
            limit=args.limit,
            dry_run=args.dry_run,
            use_tokens=args.use_tokens,
            token_model=args.token_model,
        )
    except RuntimeError as exc:
        print(f"[error] {exc}", file=sys.stderr)
        return 1

    if args.dry_run:
        print(
            f"[dry-run] Documents: {summary['documents']}, vocabulary size: {summary['terms']}, "
            f"idf_method={args.idf_method}"
        )
    else:
        print(
            f"Index written to {output_dir} "
            f"(documents={summary['documents']}, terms={summary['terms']}, idf={args.idf_method})"
        )

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
