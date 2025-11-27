"""
Query Comparison: TF-IDF Index vs PyLucene Index.

Compares search results between the old TF-IDF indexer and the new
PyLucene indexer to validate and demonstrate improvements.

Features:
- Side-by-side result comparison
- Precision/recall metrics at different k values
- Query latency comparison
- Result overlap analysis

Usage:
    python -m lucene_indexer.compare --queries "python web,machine learning,docker"
"""

import argparse
import csv
import json
import logging
import sys
import time
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional, Set, Tuple, Union

logger = logging.getLogger(__name__)


@dataclass
class QueryResult:
    """Results from a single query execution."""
    query: str
    index_type: str  # "tfidf" or "lucene"
    query_type: str  # "simple", "boolean", etc.
    doc_ids: List[str]
    scores: List[float]
    latency_ms: float
    result_count: int


@dataclass
class ComparisonResult:
    """Comparison between two query results."""
    query: str
    query_type: str
    tfidf_result: Optional[QueryResult]
    lucene_result: Optional[QueryResult]

    # Computed metrics
    overlap_at_k: Dict[int, float] = field(default_factory=dict)  # k -> overlap ratio
    rank_correlation: Optional[float] = None
    latency_improvement: Optional[float] = None  # Percentage

    def compute_metrics(self, k_values: List[int] = [5, 10, 20]):
        """Compute comparison metrics."""
        if not self.tfidf_result or not self.lucene_result:
            return

        tfidf_ids = self.tfidf_result.doc_ids
        lucene_ids = self.lucene_result.doc_ids

        # Overlap at different k values
        for k in k_values:
            tfidf_top_k = set(tfidf_ids[:k])
            lucene_top_k = set(lucene_ids[:k])

            if not tfidf_top_k and not lucene_top_k:
                self.overlap_at_k[k] = 1.0
            elif not tfidf_top_k or not lucene_top_k:
                self.overlap_at_k[k] = 0.0
            else:
                overlap = len(tfidf_top_k & lucene_top_k)
                self.overlap_at_k[k] = overlap / max(len(tfidf_top_k), len(lucene_top_k))

        # Latency improvement
        if self.tfidf_result.latency_ms > 0:
            self.latency_improvement = (
                (self.tfidf_result.latency_ms - self.lucene_result.latency_ms)
                / self.tfidf_result.latency_ms * 100
            )

    def to_dict(self) -> Dict:
        """Convert to dictionary for JSON/report output."""
        return {
            "query": self.query,
            "query_type": self.query_type,
            "tfidf": {
                "result_count": self.tfidf_result.result_count if self.tfidf_result else 0,
                "latency_ms": round(self.tfidf_result.latency_ms, 2) if self.tfidf_result else None,
                "top_5_docs": self.tfidf_result.doc_ids[:5] if self.tfidf_result else [],
            } if self.tfidf_result else None,
            "lucene": {
                "result_count": self.lucene_result.result_count if self.lucene_result else 0,
                "latency_ms": round(self.lucene_result.latency_ms, 2) if self.lucene_result else None,
                "top_5_docs": self.lucene_result.doc_ids[:5] if self.lucene_result else [],
            } if self.lucene_result else None,
            "overlap_at_k": {str(k): round(v, 3) for k, v in self.overlap_at_k.items()},
            "latency_improvement_pct": round(self.latency_improvement, 1) if self.latency_improvement else None,
        }


class QueryComparison:
    """
    Compares query results between TF-IDF and PyLucene indexes.
    """

    def __init__(
        self,
        tfidf_index_dir: Optional[Path] = None,
        lucene_index_dir: Optional[Path] = None,
    ):
        """
        Initialize comparison with index directories.

        Args:
            tfidf_index_dir: Path to TF-IDF index (default: workspace/store/index)
            lucene_index_dir: Path to Lucene index (default: workspace/store/lucene_index)
        """
        self.tfidf_index_dir = tfidf_index_dir
        self.lucene_index_dir = lucene_index_dir

        self._tfidf_engine = None
        self._lucene_searcher = None

        self.results: List[ComparisonResult] = []

    def _init_tfidf(self):
        """Initialize TF-IDF search engine."""
        if self._tfidf_engine is not None:
            return

        if not self.tfidf_index_dir or not self.tfidf_index_dir.exists():
            logger.warning(f"TF-IDF index not found: {self.tfidf_index_dir}")
            return

        try:
            # Import from existing indexer module
            sys.path.insert(0, str(Path(__file__).parent.parent))
            from indexer.search import SearchEngine
            self._tfidf_engine = SearchEngine(self.tfidf_index_dir)
            logger.info(f"Loaded TF-IDF index with {self._tfidf_engine.total_docs} docs")
        except Exception as e:
            logger.warning(f"Failed to load TF-IDF index: {e}")

    def _init_lucene(self):
        """Initialize Lucene searcher."""
        if self._lucene_searcher is not None:
            return

        if not self.lucene_index_dir or not self.lucene_index_dir.exists():
            logger.warning(f"Lucene index not found: {self.lucene_index_dir}")
            return

        try:
            from .search import LuceneSearcher
            self._lucene_searcher = LuceneSearcher(self.lucene_index_dir)
            logger.info(f"Loaded Lucene index with {self._lucene_searcher.get_stats()['num_docs']} docs")
        except Exception as e:
            logger.warning(f"Failed to load Lucene index: {e}")

    def _query_tfidf(
        self,
        query: str,
        top_k: int = 20,
    ) -> Optional[QueryResult]:
        """Execute query on TF-IDF index."""
        self._init_tfidf()

        if not self._tfidf_engine:
            return None

        start = time.perf_counter()
        try:
            results = self._tfidf_engine.search(query, top_k=top_k)
            latency = (time.perf_counter() - start) * 1000

            return QueryResult(
                query=query,
                index_type="tfidf",
                query_type="simple",
                doc_ids=[str(r.doc_id) for r in results],
                scores=[r.score for r in results],
                latency_ms=latency,
                result_count=len(results),
            )
        except Exception as e:
            logger.warning(f"TF-IDF query failed: {e}")
            return None

    def _query_lucene(
        self,
        query: str,
        query_type: str = "simple",
        top_k: int = 20,
        **kwargs,
    ) -> Optional[QueryResult]:
        """Execute query on Lucene index."""
        self._init_lucene()

        if not self._lucene_searcher:
            return None

        start = time.perf_counter()
        try:
            from .search import QueryType
            qt = QueryType(query_type)

            results = self._lucene_searcher.search(query, qt, top_k, **kwargs)
            latency = (time.perf_counter() - start) * 1000

            return QueryResult(
                query=query,
                index_type="lucene",
                query_type=query_type,
                doc_ids=[r.doc_id for r in results],
                scores=[r.score for r in results],
                latency_ms=latency,
                result_count=len(results),
            )
        except Exception as e:
            logger.warning(f"Lucene query failed: {e}")
            return None

    def compare_query(
        self,
        query: str,
        query_type: str = "simple",
        top_k: int = 20,
        **lucene_kwargs,
    ) -> ComparisonResult:
        """
        Compare a single query across both indexes.

        Args:
            query: Query string
            query_type: Lucene query type (simple, boolean, phrase, fuzzy)
            top_k: Number of results to retrieve
            **lucene_kwargs: Additional args for Lucene (e.g., field, slop)

        Returns:
            ComparisonResult with metrics
        """
        tfidf_result = self._query_tfidf(query, top_k)
        lucene_result = self._query_lucene(query, query_type, top_k, **lucene_kwargs)

        comparison = ComparisonResult(
            query=query,
            query_type=query_type,
            tfidf_result=tfidf_result,
            lucene_result=lucene_result,
        )
        comparison.compute_metrics()

        self.results.append(comparison)
        return comparison

    def compare_queries(
        self,
        queries: List[str],
        query_types: Optional[List[str]] = None,
        top_k: int = 20,
    ) -> List[ComparisonResult]:
        """
        Compare multiple queries.

        Args:
            queries: List of query strings
            query_types: List of query types (or single type for all)
            top_k: Number of results per query

        Returns:
            List of ComparisonResult objects
        """
        if query_types is None:
            query_types = ["simple"] * len(queries)
        elif len(query_types) == 1:
            query_types = query_types * len(queries)

        results = []
        for query, qtype in zip(queries, query_types):
            logger.info(f"Comparing query: '{query}' ({qtype})")
            result = self.compare_query(query, qtype, top_k)
            results.append(result)

        return results

    def generate_report(self, output_path: Optional[Path] = None) -> str:
        """
        Generate comparison report.

        Args:
            output_path: Optional path to save report

        Returns:
            Report as markdown string
        """
        lines = [
            "# Index Comparison Report",
            "",
            f"Generated: {datetime.now().isoformat()}",
            "",
            "## Summary",
            "",
            f"- Queries compared: {len(self.results)}",
            f"- TF-IDF index: {self.tfidf_index_dir}",
            f"- Lucene index: {self.lucene_index_dir}",
            "",
            "## Query Types Demonstrated",
            "",
            "| Type | Description | Example |",
            "|------|-------------|---------|",
            "| Simple | Full-text search across multiple fields | `python web scraping` |",
            "| Boolean | AND/OR queries | `python AND docker` |",
            "| Range | Numeric range queries | `star_count:[1000 TO *]` |",
            "| Phrase | Exact phrase matching | `\"machine learning\"` |",
            "| Fuzzy | Typo-tolerant matching | `pyhton~2` |",
            "",
            "## Results by Query",
            "",
        ]

        # Aggregate stats
        total_tfidf_latency = 0
        total_lucene_latency = 0
        overlap_sums = {5: 0, 10: 0, 20: 0}
        valid_comparisons = 0

        for result in self.results:
            lines.append(f"### Query: `{result.query}`")
            lines.append(f"**Type**: {result.query_type}")
            lines.append("")

            if result.tfidf_result and result.lucene_result:
                valid_comparisons += 1
                total_tfidf_latency += result.tfidf_result.latency_ms
                total_lucene_latency += result.lucene_result.latency_ms

                lines.append("| Metric | TF-IDF | Lucene |")
                lines.append("|--------|--------|--------|")
                lines.append(f"| Results | {result.tfidf_result.result_count} | {result.lucene_result.result_count} |")
                lines.append(f"| Latency (ms) | {result.tfidf_result.latency_ms:.2f} | {result.lucene_result.latency_ms:.2f} |")
                lines.append("")

                lines.append("**Result Overlap**:")
                for k, overlap in result.overlap_at_k.items():
                    lines.append(f"- Top-{k}: {overlap * 100:.1f}%")
                    overlap_sums[k] += overlap

                if result.latency_improvement:
                    lines.append(f"\n**Latency improvement**: {result.latency_improvement:.1f}%")

            elif result.lucene_result:
                lines.append("*TF-IDF index not available for comparison*")
                lines.append(f"- Lucene results: {result.lucene_result.result_count}")
                lines.append(f"- Lucene latency: {result.lucene_result.latency_ms:.2f}ms")

            elif result.tfidf_result:
                lines.append("*Lucene index not available for comparison*")
                lines.append(f"- TF-IDF results: {result.tfidf_result.result_count}")
                lines.append(f"- TF-IDF latency: {result.tfidf_result.latency_ms:.2f}ms")

            lines.append("")

        # Overall statistics
        if valid_comparisons > 0:
            lines.extend([
                "## Overall Statistics",
                "",
                "| Metric | TF-IDF | Lucene | Improvement |",
                "|--------|--------|--------|-------------|",
            ])

            avg_tfidf_lat = total_tfidf_latency / valid_comparisons
            avg_lucene_lat = total_lucene_latency / valid_comparisons
            lat_improvement = (avg_tfidf_lat - avg_lucene_lat) / avg_tfidf_lat * 100 if avg_tfidf_lat > 0 else 0

            lines.append(f"| Avg Latency (ms) | {avg_tfidf_lat:.2f} | {avg_lucene_lat:.2f} | {lat_improvement:.1f}% |")

            for k in [5, 10, 20]:
                avg_overlap = overlap_sums[k] / valid_comparisons * 100
                lines.append(f"| Avg Overlap@{k} | - | - | {avg_overlap:.1f}% |")

            lines.append("")

        # Lucene advantages section
        lines.extend([
            "## Lucene Index Advantages",
            "",
            "### 1. Query Type Support",
            "- **Boolean queries**: Native AND/OR/NOT support with proper precedence",
            "- **Phrase queries**: Exact phrase matching with configurable slop",
            "- **Fuzzy queries**: Levenshtein distance-based typo tolerance",
            "- **Range queries**: Efficient numeric range filtering (e.g., star_count > 1000)",
            "",
            "### 2. Structured Fields",
            "- Entity fields (topics, languages) as separate indexed fields",
            "- Wiki enrichment fields for enhanced search",
            "- Numeric fields with range query support",
            "",
            "### 3. Scalability",
            "- Efficient inverted index with skip lists",
            "- Compressed postings for reduced disk usage",
            "- Term dictionary with FST encoding",
            "",
            "### 4. Relevance",
            "- BM25 scoring by default (superior to basic TF-IDF)",
            "- Field boosting for relevance tuning",
            "- Multi-field search with configurable weights",
            "",
        ])

        report = "\n".join(lines)

        if output_path:
            output_path = Path(output_path)
            output_path.parent.mkdir(parents=True, exist_ok=True)
            output_path.write_text(report, encoding='utf-8')
            logger.info(f"Report saved to {output_path}")

        return report

    def close(self):
        """Close index connections."""
        if self._lucene_searcher:
            self._lucene_searcher.close()


# =============================================================================
# CLI Interface
# =============================================================================

def load_queries_from_file(file_path: Path) -> List[Tuple[str, str]]:
    """
    Load queries from a file.

    File format:
        # Comment lines start with #
        query_type|query_text
        simple|python web
        boolean|python AND docker

    Returns:
        List of (query_type, query_text) tuples
    """
    queries = []
    with open(file_path, 'r', encoding='utf-8') as f:
        for line in f:
            line = line.strip()
            if not line or line.startswith('#'):
                continue
            if '|' in line:
                query_type, query_text = line.split('|', 1)
                queries.append((query_type.strip(), query_text.strip()))
            else:
                queries.append(('simple', line))
    return queries


def parse_args(argv: Optional[List[str]] = None) -> argparse.Namespace:
    """Parse command-line arguments."""
    parser = argparse.ArgumentParser(
        description="Compare TF-IDF and PyLucene index query results"
    )
    parser.add_argument(
        '--tfidf-index',
        default='workspace/store/index',
        help='Path to TF-IDF index directory'
    )
    parser.add_argument(
        '--lucene-index',
        default='workspace/store/lucene_index',
        help='Path to Lucene index directory'
    )
    parser.add_argument(
        '--queries',
        help='Comma-separated list of queries to compare'
    )
    parser.add_argument(
        '--queries-file',
        help='Path to queries file (format: query_type|query per line)'
    )
    parser.add_argument(
        '--types',
        default='simple',
        help='Comma-separated query types (default: simple for all)'
    )
    parser.add_argument(
        '--top',
        type=int,
        default=20,
        help='Number of results per query (default: 20)'
    )
    parser.add_argument(
        '--output',
        default='reports/index_comparison.md',
        help='Output path for comparison report'
    )
    parser.add_argument(
        '--json',
        action='store_true',
        help='Also output JSON format'
    )
    return parser.parse_args(argv)


def main(argv: Optional[List[str]] = None) -> int:
    """Main entry point."""
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s [%(levelname)s] %(message)s'
    )

    args = parse_args(argv)

    # Load queries from file or command line
    if args.queries_file:
        query_file_path = Path(args.queries_file)
        if not query_file_path.exists():
            logger.error(f"Queries file not found: {query_file_path}")
            return 1
        query_pairs = load_queries_from_file(query_file_path)
        queries = [q[1] for q in query_pairs]
        query_types = [q[0] for q in query_pairs]
        logger.info(f"Loaded {len(queries)} queries from {query_file_path}")
    elif args.queries:
        queries = [q.strip() for q in args.queries.split(',')]
        query_types = [t.strip() for t in args.types.split(',')]
    else:
        logger.error("Either --queries or --queries-file must be provided")
        return 1

    comparison = QueryComparison(
        tfidf_index_dir=Path(args.tfidf_index),
        lucene_index_dir=Path(args.lucene_index),
    )

    try:
        # Run comparisons
        results = comparison.compare_queries(queries, query_types, args.top)

        # Generate report
        report = comparison.generate_report(Path(args.output))
        print(report)

        # JSON output if requested
        if args.json:
            json_path = Path(args.output).with_suffix('.json')
            json_data = {
                "queries": [r.to_dict() for r in results],
                "generated_at": datetime.now().isoformat(),
            }
            with open(json_path, 'w', encoding='utf-8') as f:
                json.dump(json_data, f, indent=2)
            logger.info(f"JSON output saved to {json_path}")

        return 0

    except Exception as e:
        logger.error(f"Comparison failed: {e}")
        return 1

    finally:
        comparison.close()


if __name__ == "__main__":
    sys.exit(main())
