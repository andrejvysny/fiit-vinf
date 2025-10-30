"""Generate side-by-side comparisons for different IDF methods."""

from __future__ import annotations

import argparse
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterable, List, Optional

from .search import InvertedIndex, SearchResult

DEFAULT_OUTPUT = Path("docs/generated/index_comparison.md")
DEFAULT_QUERIES = [
    "github crawler",
    "async http client",
    "repository metadata",
]


def _parse_args(argv: Optional[list[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Compare log vs RSJ IDF rankings and emit a Markdown report."
    )
    parser.add_argument(
        "--index",
        default="workspace/store/index/default",
        help="Path to the index directory (default: %(default)s).",
    )
    parser.add_argument(
        "--query",
        action="append",
        dest="queries",
        help="Add a query to the comparison set (may be specified multiple times).",
    )
    parser.add_argument(
        "--queries-file",
        help="Optional path to a file with one query per line.",
    )
    parser.add_argument(
        "--top",
        type=int,
        default=5,
        help="Number of top documents to compare (default: %(default)s).",
    )
    parser.add_argument(
        "--output",
        default=str(DEFAULT_OUTPUT),
        help="Path for Markdown output (default: %(default)s).",
    )
    return parser.parse_args(argv)


def _load_queries(args: argparse.Namespace) -> List[str]:
    queries: List[str] = []
    if args.queries_file:
        file_path = Path(args.queries_file)
        if not file_path.exists():
            raise FileNotFoundError(f"Queries file not found: {file_path}")
        queries.extend(
            line.strip() for line in file_path.read_text(encoding="utf-8").splitlines() if line.strip()
        )
    if args.queries:
        queries.extend(q.strip() for q in args.queries if q and q.strip())
    if not queries:
        queries = DEFAULT_QUERIES.copy()
    return queries


def _format_doc(result: SearchResult) -> str:
    title = result.title or "<untitled>"
    return f"{result.doc_id} ({title})"


def _markdown_table(
    query: str,
    log_results: List[SearchResult],
    rsj_results: List[SearchResult],
    top_k: int,
) -> List[str]:
    lines = [f"## Query: `{query}`", "", "| Rank | log score | log doc | rsj score | rsj doc |", "| --- | --- | --- | --- | --- |"]

    overlap = 0
    rsj_lookup = {res.doc_id: res for res in rsj_results}

    for rank in range(top_k):
        log_res = log_results[rank] if rank < len(log_results) else None
        rsj_res = rsj_results[rank] if rank < len(rsj_results) else None
        if log_res and rsj_res and log_res.doc_id == rsj_res.doc_id:
            overlap += 1
        lines.append(
            "| {rank} | {log_score} | {log_doc} | {rsj_score} | {rsj_doc} |".format(
                rank=rank + 1,
                log_score=f"{log_res.score:.4f}" if log_res else "-",
                log_doc=_format_doc(log_res) if log_res else "-",
                rsj_score=f"{rsj_res.score:.4f}" if rsj_res else "-",
                rsj_doc=_format_doc(rsj_res) if rsj_res else "-",
            )
        )

    log_ids = {res.doc_id for res in log_results[:top_k]}
    rsj_ids = {res.doc_id for res in rsj_results[:top_k]}
    jaccard = 0.0
    if log_ids or rsj_ids:
        jaccard = len(log_ids & rsj_ids) / len(log_ids | rsj_ids)

    lines.append("")
    lines.append(f"Overlap (same rank): {overlap}/{top_k}")
    lines.append(f"Jaccard (top-{top_k} sets): {jaccard:.2f}")
    lines.append("")
    return lines


def _write_markdown(output_path: Path, lines: Iterable[str]) -> None:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    content = "\n".join(lines)
    output_path.write_text(content.rstrip() + "\n", encoding="utf-8")


def main(argv: Optional[list[str]] = None) -> int:
    args = _parse_args(argv)
    queries = _load_queries(args)
    index_dir = Path(args.index).resolve()
    index = InvertedIndex(index_dir)

    markdown_lines: List[str] = []
    markdown_lines.append("# IDF Comparison")
    markdown_lines.append("")
    timestamp = datetime.now(tz=timezone.utc).isoformat(timespec="seconds")
    markdown_lines.append(f"Generated on {timestamp} using index `{index_dir}`")
    markdown_lines.append("")

    for query in queries:
        log_results = index.search(
            query,
            top_k=args.top,
            idf_method="log",
            use_stored_idf=False,
        )
        rsj_results = index.search(
            query,
            top_k=args.top,
            idf_method="rsj",
            use_stored_idf=False,
        )
        markdown_lines.extend(
            _markdown_table(query, log_results, rsj_results, args.top)
        )

    _write_markdown(Path(args.output), markdown_lines)
    print(f"Comparison written to {args.output}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
