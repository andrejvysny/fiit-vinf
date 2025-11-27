"""
Unified Search Interface: TF-IDF vs PyLucene.

Provides a single API that can switch between TF-IDF and Lucene indexes
based on configuration. This enables easy comparison and flexibility.

Usage:
    python -m lucene_indexer.unified_search --query "python web" --engine lucene
    python -m lucene_indexer.unified_search --query "python web" --engine tfidf
"""

import argparse
import json
import logging
import sys
from dataclasses import dataclass, field, asdict
from pathlib import Path
from typing import Dict, List, Optional

logger = logging.getLogger(__name__)


@dataclass
class UnifiedSearchResult:
    """
    Unified search result format for both TF-IDF and Lucene engines.

    This ensures consistent output format regardless of search engine.
    """
    rank: int
    doc_id: str
    score: float
    title: str
    url: Optional[str] = None
    snippet: Optional[str] = None
    # Entity fields
    topics: List[str] = field(default_factory=list)
    languages: List[str] = field(default_factory=list)
    license: Optional[str] = None
    star_count: Optional[int] = None
    # Wiki fields
    wiki_titles: List[str] = field(default_factory=list)
    wiki_categories: List[str] = field(default_factory=list)
    # Metadata
    path: Optional[str] = None
    engine: str = "unknown"

    def to_dict(self) -> Dict:
        """Convert to dictionary for JSON output."""
        return {
            "rank": self.rank,
            "doc_id": self.doc_id,
            "score": round(self.score, 6),
            "title": self.title,
            "url": self.url,
            "snippet": self.snippet,
            "topics": self.topics,
            "languages": self.languages,
            "license": self.license,
            "star_count": self.star_count,
            "wiki_titles": self.wiki_titles,
            "wiki_categories": self.wiki_categories,
            "path": self.path,
            "engine": self.engine,
        }


class UnifiedSearcher:
    """
    Unified search interface supporting both TF-IDF and Lucene engines.

    Config-driven selection via search.engine setting.
    """

    def __init__(
        self,
        engine: str = "tfidf",
        tfidf_index_dir: Optional[Path] = None,
        lucene_index_dir: Optional[Path] = None,
    ):
        """
        Initialize unified searcher.

        Args:
            engine: "tfidf" or "lucene"
            tfidf_index_dir: Path to TF-IDF index (default: workspace/store/index)
            lucene_index_dir: Path to Lucene index (default: workspace/store/lucene_index)
        """
        self.engine = engine.lower()
        self.tfidf_index_dir = tfidf_index_dir or Path("workspace/store/index")
        self.lucene_index_dir = lucene_index_dir or Path("workspace/store/lucene_index")

        self._tfidf_engine = None
        self._lucene_searcher = None

    def _init_tfidf(self):
        """Initialize TF-IDF search engine."""
        if self._tfidf_engine is not None:
            return True

        try:
            sys.path.insert(0, str(Path(__file__).parent.parent))
            from indexer.search import SearchEngine
            self._tfidf_engine = SearchEngine(self.tfidf_index_dir)
            logger.info(f"Initialized TF-IDF engine with {self._tfidf_engine.total_docs} docs")
            return True
        except Exception as e:
            logger.warning(f"Failed to initialize TF-IDF engine: {e}")
            return False

    def _init_lucene(self):
        """Initialize Lucene searcher."""
        if self._lucene_searcher is not None:
            return True

        try:
            from .search import LuceneSearcher
            self._lucene_searcher = LuceneSearcher(self.lucene_index_dir)
            logger.info(f"Initialized Lucene engine with {self._lucene_searcher.get_stats()['num_docs']} docs")
            return True
        except Exception as e:
            logger.warning(f"Failed to initialize Lucene engine: {e}")
            return False

    def search(
        self,
        query: str,
        top_k: int = 10,
        query_type: str = "simple",
        **kwargs,
    ) -> List[UnifiedSearchResult]:
        """
        Execute search using configured engine.

        Args:
            query: Search query string
            top_k: Number of results to return
            query_type: Query type (simple, boolean, phrase, fuzzy, range)
            **kwargs: Additional engine-specific arguments

        Returns:
            List of UnifiedSearchResult objects
        """
        if self.engine == "lucene":
            return self._search_lucene(query, top_k, query_type, **kwargs)
        else:
            return self._search_tfidf(query, top_k, **kwargs)

    def _search_tfidf(
        self,
        query: str,
        top_k: int = 10,
        **kwargs,
    ) -> List[UnifiedSearchResult]:
        """Execute TF-IDF search."""
        if not self._init_tfidf():
            logger.error("TF-IDF engine not available")
            return []

        try:
            results = self._tfidf_engine.search(query, top_k=top_k, **kwargs)

            unified_results = []
            for rank, result in enumerate(results, start=1):
                unified_results.append(UnifiedSearchResult(
                    rank=rank,
                    doc_id=str(result.doc_id),
                    score=result.score,
                    title=result.title,
                    path=result.path,
                    engine="tfidf",
                ))

            return unified_results

        except Exception as e:
            logger.error(f"TF-IDF search failed: {e}")
            return []

    def _search_lucene(
        self,
        query: str,
        top_k: int = 10,
        query_type: str = "simple",
        **kwargs,
    ) -> List[UnifiedSearchResult]:
        """Execute Lucene search."""
        if not self._init_lucene():
            logger.error("Lucene engine not available")
            return []

        try:
            from .search import QueryType
            qt = QueryType(query_type)
            results = self._lucene_searcher.search(query, qt, top_k, **kwargs)

            unified_results = []
            for rank, result in enumerate(results, start=1):
                # Generate snippet from wiki_abstract or first topic
                snippet = None
                if result.wiki_titles:
                    snippet = f"Wikipedia: {', '.join(result.wiki_titles[:2])}"
                elif result.topics:
                    snippet = f"Topics: {', '.join(result.topics[:3])}"

                unified_results.append(UnifiedSearchResult(
                    rank=rank,
                    doc_id=result.doc_id,
                    score=result.score,
                    title=result.title,
                    url=result.url,
                    snippet=snippet,
                    topics=result.topics,
                    languages=result.languages,
                    license=result.license,
                    star_count=result.star_count,
                    wiki_titles=result.wiki_titles,
                    path=result.path,
                    engine="lucene",
                ))

            return unified_results

        except Exception as e:
            logger.error(f"Lucene search failed: {e}")
            return []

    def close(self):
        """Close search engine resources."""
        if self._lucene_searcher:
            self._lucene_searcher.close()

    @classmethod
    def from_config(cls, config_path: Path) -> 'UnifiedSearcher':
        """
        Create UnifiedSearcher from config file.

        Config format (config.yml):
        ```yaml
        search:
          engine: "lucene"  # or "tfidf"
          tfidf_index_dir: "workspace/store/index"
          lucene_index_dir: "workspace/store/lucene_index"
        ```
        """
        try:
            sys.path.insert(0, str(config_path.parent))
            from config_loader import load_yaml_config
            config = load_yaml_config(config_path)

            search_cfg = config.get('search', {})
            engine = search_cfg.get('engine', 'tfidf')
            tfidf_dir = Path(search_cfg.get('tfidf_index_dir', 'workspace/store/index'))
            lucene_dir = Path(search_cfg.get('lucene_index_dir', 'workspace/store/lucene_index'))

            return cls(engine=engine, tfidf_index_dir=tfidf_dir, lucene_index_dir=lucene_dir)

        except Exception as e:
            logger.warning(f"Failed to load config, using defaults: {e}")
            return cls()


def parse_args(argv: Optional[List[str]] = None) -> argparse.Namespace:
    """Parse command-line arguments."""
    parser = argparse.ArgumentParser(
        description="Unified search interface for TF-IDF and Lucene indexes"
    )
    parser.add_argument(
        '--config',
        default='config.yml',
        help='Path to configuration file'
    )
    parser.add_argument(
        '--engine',
        choices=['tfidf', 'lucene'],
        default=None,
        help='Search engine to use (overrides config)'
    )
    parser.add_argument(
        '--query',
        required=True,
        help='Search query'
    )
    parser.add_argument(
        '--type',
        choices=['simple', 'boolean', 'phrase', 'fuzzy', 'range'],
        default='simple',
        help='Query type for Lucene (default: simple)'
    )
    parser.add_argument(
        '--top',
        type=int,
        default=10,
        help='Number of results (default: 10)'
    )
    parser.add_argument(
        '--tfidf-index',
        default=None,
        help='Path to TF-IDF index (overrides config)'
    )
    parser.add_argument(
        '--lucene-index',
        default=None,
        help='Path to Lucene index (overrides config)'
    )
    parser.add_argument(
        '--json',
        action='store_true',
        help='Output results as JSON'
    )
    return parser.parse_args(argv)


def main(argv: Optional[List[str]] = None) -> int:
    """Main entry point."""
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s [%(levelname)s] %(message)s'
    )

    args = parse_args(argv)

    # Load config or use defaults
    config_path = Path(args.config)
    if config_path.exists():
        searcher = UnifiedSearcher.from_config(config_path)
    else:
        searcher = UnifiedSearcher()

    # Override with CLI arguments
    if args.engine:
        searcher.engine = args.engine
    if args.tfidf_index:
        searcher.tfidf_index_dir = Path(args.tfidf_index)
    if args.lucene_index:
        searcher.lucene_index_dir = Path(args.lucene_index)

    try:
        results = searcher.search(
            args.query,
            top_k=args.top,
            query_type=args.type,
        )

        if args.json:
            print(json.dumps([r.to_dict() for r in results], indent=2))
        else:
            print(f"\n{'=' * 60}")
            print(f"Query: {args.query}")
            print(f"Engine: {searcher.engine}")
            print(f"Type: {args.type}")
            print(f"Results: {len(results)}")
            print(f"{'=' * 60}\n")

            for result in results:
                print(f"{result.rank}. {result.title}")
                print(f"   Score: {result.score:.6f}")
                print(f"   Doc ID: {result.doc_id}")
                if result.url:
                    print(f"   URL: {result.url}")
                if result.snippet:
                    print(f"   Snippet: {result.snippet}")
                if result.topics:
                    print(f"   Topics: {', '.join(result.topics[:5])}")
                if result.star_count:
                    print(f"   Stars: {result.star_count:,}")
                print()

        return 0

    except Exception as e:
        logger.error(f"Search failed: {e}")
        return 1

    finally:
        searcher.close()


if __name__ == "__main__":
    sys.exit(main())
