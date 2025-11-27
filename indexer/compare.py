"""Compare ranked outputs across IDF strategies."""

from __future__ import annotations

import argparse
from dataclasses import dataclass
from datetime import datetime, timezone
from itertools import combinations
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Sequence, Tuple

from .idf import IDFRegistry
from .search import SearchEngine, SearchResult

DEFAULT_OUTPUT = Path("reports/idf_comparison.tsv")
DEFAULT_QUERIES = [
    "github crawler",
    "async http client",
    "repository metadata",
]
DEFAULT_MD_OUTPUT = Path("reports/idf_comparison.md")


@dataclass(frozen=True)
class RankingRow:
    query: str
    method: str
    rank: int
    doc_id: int
    score: float
    title: str


@dataclass(frozen=True)
class PairwiseComparison:
    method_a: str
    method_b: str
    overlap: int
    jaccard: float


@dataclass
class MethodRanking:
    method: str
    results: List[SearchResult]


@dataclass
class QueryComparison:
    query: str
    rankings: List[MethodRanking]
    pairwise_metrics: List[PairwiseComparison]

    @property
    def methods(self) -> Tuple[str, ...]:
        return tuple(r.method for r in self.rankings)


@dataclass
class ComparisonReport:
    comparisons: List[QueryComparison]
    rows: List[RankingRow]
    top_k: int

    def console_lines(self) -> List[str]:
        lines: List[str] = []
        timestamp = datetime.now(tz=timezone.utc).isoformat(timespec="seconds")
        lines.append(f"IDF comparison report generated {timestamp}")
        lines.append("")

        for comparison in self.comparisons:
            lines.append(f"Query: {comparison.query!r}")
            lines.append(f"Methods: {', '.join(comparison.methods)}")

            for ranking in comparison.rankings:
                if not ranking.results:
                    lines.append(f"  {ranking.method:<14} no results")
                    continue
                summary = ", ".join(
                    f"{idx + 1}:{res.doc_id} ({res.score:.3f})"
                    for idx, res in enumerate(ranking.results[: self.top_k])
                )
                lines.append(f"  {ranking.method:<14} {summary}")

            if comparison.pairwise_metrics:
                lines.append("  Pairwise overlap / Jaccard:")
                for metric in comparison.pairwise_metrics:
                    lines.append(
                        f"    {metric.method_a} vs {metric.method_b}: "
                        f"overlap={metric.overlap}/{self.top_k}, "
                        f"jaccard={metric.jaccard:.2f}"
                    )
            lines.append("")

        return lines

    def write_tsv(self, output_path: Path) -> None:
        output_path = output_path.resolve()
        output_path.parent.mkdir(parents=True, exist_ok=True)
        header = "query\tmethod\trank\tdoc_id\tscore\ttitle"
        rows = [
            f"{row.query}\t{row.method}\t{row.rank}\t{row.doc_id}\t{row.score:.6f}\t{row.title.replace('\t', ' ').replace('\n', ' ')}"
            for row in self.rows
        ]
        output_path.write_text(
            "\n".join([header, *rows]) + ("\n" if rows else "\n"),
            encoding="utf-8",
        )

    def write_markdown(self, output_path: Path) -> None:
        """Emit a human-friendly markdown report summarizing comparisons.

        This produces a readable document suitable for quick review and
        inclusion in the repository `reports/` tree.
        """
        output_path = output_path.resolve()
        output_path.parent.mkdir(parents=True, exist_ok=True)

        lines: List[str] = []
        timestamp = datetime.now(tz=timezone.utc).isoformat(timespec="seconds")
        lines.append(f"# IDF comparison report")
        lines.append("")
        lines.append(f"Generated: {timestamp}")
        lines.append("")

        for comparison in self.comparisons:
            lines.append(f"## Query: {comparison.query!r}")
            lines.append("")
            lines.append(f"**Methods:** {', '.join(comparison.methods)}")
            lines.append("")

            for ranking in comparison.rankings:
                lines.append(f"### {ranking.method}")
                if not ranking.results:
                    lines.append("_no results_")
                    lines.append("")
                    continue
                for idx, res in enumerate(ranking.results[: self.top_k], start=1):
                    title = (res.title or "<untitled>").replace("\n", " ")
                    lines.append(f"{idx}. **{title}** — doc_id: `{res.doc_id}` — score: {res.score:.6f}")
                lines.append("")

            if comparison.pairwise_metrics:
                lines.append("### Pairwise metrics")
                lines.append("")
                lines.append("| Method A | Method B | Overlap | Jaccard |")
                lines.append("|---|---|---:|---:|")
                for metric in comparison.pairwise_metrics:
                    lines.append(
                        f"| {metric.method_a} | {metric.method_b} | {metric.overlap}/{self.top_k} | {metric.jaccard:.2f} |"
                    )
                lines.append("")

        output_path.write_text("\n".join(lines) + "\n", encoding="utf-8")


class RankingComparator:
    """Evaluate ranked outputs across multiple IDF strategies."""

    def __init__(
        self,
        search_engine: SearchEngine,
        *,
        methods: Sequence[str],
        top_k: int,
        use_stored_idf: bool = True,
    ) -> None:
        self.engine = search_engine
        self.methods = tuple(methods)
        self.top_k = max(top_k, 0)
        self.use_stored_idf = use_stored_idf

    def evaluate(self, queries: Iterable[str]) -> ComparisonReport:
        comparisons: List[QueryComparison] = []
        rows: List[RankingRow] = []

        for query in queries:
            rankings = [
                MethodRanking(
                    method=method,
                    results=self.engine.search(
                        query,
                        top_k=self.top_k,
                        idf_method=method,
                        use_stored_idf=self.use_stored_idf,
                    ),
                )
                for method in self.methods
            ]
            pairwise = self._pairwise_metrics(rankings)
            comparisons.append(
                QueryComparison(query=query, rankings=rankings, pairwise_metrics=pairwise)
            )
            rows.extend(self._rows_for(query, rankings))

        return ComparisonReport(comparisons=comparisons, rows=rows, top_k=self.top_k)

    def _rows_for(self, query: str, rankings: List[MethodRanking]) -> List[RankingRow]:
        rows: List[RankingRow] = []
        for ranking in rankings:
            for idx, result in enumerate(ranking.results[: self.top_k], start=1):
                rows.append(
                    RankingRow(
                        query=query,
                        method=ranking.method,
                        rank=idx,
                        doc_id=result.doc_id,
                        score=result.score,
                        title=result.title or "<untitled>",
                    )
                )
        return rows

    def _pairwise_metrics(
        self, rankings: List[MethodRanking]
    ) -> List[PairwiseComparison]:
        metrics: List[PairwiseComparison] = []
        for left, right in combinations(rankings, 2):
            overlap = self._rank_overlap(left.results, right.results)
            jaccard = self._jaccard(left.results, right.results)
            metrics.append(
                PairwiseComparison(
                    method_a=left.method,
                    method_b=right.method,
                    overlap=overlap,
                    jaccard=jaccard,
                )
            )
        return metrics

    def _rank_overlap(
        self, left: Sequence[SearchResult], right: Sequence[SearchResult]
    ) -> int:
        overlap = 0
        for idx in range(self.top_k):
            if idx < len(left) and idx < len(right):
                if left[idx].doc_id == right[idx].doc_id:
                    overlap += 1
        return overlap

    def _jaccard(
        self, left: Sequence[SearchResult], right: Sequence[SearchResult]
    ) -> float:
        left_ids = {res.doc_id for res in left[: self.top_k]}
        right_ids = {res.doc_id for res in right[: self.top_k]}
        union = left_ids | right_ids
        if not union:
            return 0.0
        return len(left_ids & right_ids) / len(union)


def _parse_args(argv: Optional[list[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Compare ranked outputs across IDF strategies and emit a TSV report."
    )
    registry = IDFRegistry()
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
        "--method",
        dest="methods",
        action="append",
        type=lambda value: registry.ensure(value),
        help=(
            "IDF method to compare (may be specified multiple times). "
            f"Defaults to all available methods ({', '.join(registry.supported_methods)})."
        ),
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
        help="Path for TSV output (default: %(default)s).",
    )
    parser.add_argument(
        "--markdown-output",
        default=str(DEFAULT_MD_OUTPUT),
        help="Optional path for markdown output (default: %(default)s). If empty, markdown will not be written.",
    )
    return parser.parse_args(argv)


def _load_queries(args: argparse.Namespace) -> List[str]:
    queries: List[str] = []
    if args.queries_file:
        file_path = Path(args.queries_file)
        if not file_path.exists():
            raise FileNotFoundError(f"Queries file not found: {file_path}")
        queries.extend(
            line.strip()
            for line in file_path.read_text(encoding="utf-8").splitlines()
            if line.strip()
        )
    if args.queries:
        queries.extend(q.strip() for q in args.queries if q and q.strip())
    if not queries:
        queries = DEFAULT_QUERIES.copy()
    return queries


def main(argv: Optional[list[str]] = None) -> int:
    args = _parse_args(argv)
    queries = _load_queries(args)
    index_dir = Path(args.index).resolve()
    engine = SearchEngine(index_dir)

    methods = tuple(args.methods) if args.methods else engine.available_idf_methods
    comparator = RankingComparator(engine, methods=methods, top_k=args.top)
    report = comparator.evaluate(queries)

    report.write_tsv(Path(args.output))
    md_out = getattr(args, "markdown_output", None)
    if md_out:
        try:
            report.write_markdown(Path(md_out))
            print(f"Markdown report written to {Path(md_out).resolve()}")
        except Exception as exc:
            print(f"Warning: failed to write markdown report to {md_out}: {exc}")
    for line in report.console_lines():
        print(line)
    print(f"TSV report written to {Path(args.output).resolve()}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
