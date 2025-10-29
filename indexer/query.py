"""Interactive CLI for running ad-hoc searches against the index."""

from __future__ import annotations

import argparse
from pathlib import Path
from typing import Optional

from .build import SUPPORTED_IDF_METHODS
from .search import InvertedIndex


IDF_CHOICES = ["manifest", *sorted(SUPPORTED_IDF_METHODS)]


def parse_args(argv: Optional[list[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Query the inverted index.")
    parser.add_argument(
        "--index",
        default="workspace/index/default",
        help="Path to the index directory (default: %(default)s).",
    )
    parser.add_argument(
        "--query",
        required=True,
        help="Search query (plain text).",
    )
    parser.add_argument(
        "--top",
        type=int,
        default=10,
        help="Number of results to return (default: %(default)s).",
    )
    parser.add_argument(
        "--idf-method",
        choices=IDF_CHOICES,
        default="manifest",
        help="IDF weighting strategy. 'manifest' uses stored weights from build time.",
    )
    parser.add_argument(
        "--show-path",
        action="store_true",
        help="Display full file paths in the output.",
    )
    return parser.parse_args(argv)


def main(argv: Optional[list[str]] = None) -> int:
    args = parse_args(argv)
    index_dir = Path(args.index).resolve()
    search_index = InvertedIndex(index_dir)

    use_stored_idf = args.idf_method == "manifest"
    effective_method = None if use_stored_idf else args.idf_method

    results = search_index.search(
        args.query,
        top_k=args.top,
        idf_method=effective_method,
        use_stored_idf=use_stored_idf,
    )

    if not results:
        print("No results found.")
        return 0

    print(
        f"Query: {args.query!r} | IDF: {args.idf_method if args.idf_method != 'manifest' else search_index.default_idf_method}"
    )

    for rank, result in enumerate(results, start=1):
        term_summary = ", ".join(
            f"{term}({tf})" for term, tf in result.matched_terms.items()
        )
        metadata = f"len={result.length}"
        if result.token_count is not None:
            metadata += f", tokens={result.token_count}"
        print(
            f"{rank:>2}. score={result.score:.4f} doc={result.doc_id} {metadata} | {result.title}"
        )
        if term_summary:
            print(f"    terms: {term_summary}")
        if args.show_path:
            print(f"    path: {result.path}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
