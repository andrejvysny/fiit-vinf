"""
PyLucene Search Engine with Multiple Query Types.

Supports:
- Boolean queries (AND/OR)
- Range queries (for numeric fields like star_count)
- Phrase queries (exact phrase matching)
- Fuzzy queries (typo-tolerant matching)

Usage:
    python -m lucene_indexer.search --index workspace/store/lucene_index --query "python web"
    python -m lucene_indexer.search --query "python AND docker" --type boolean
    python -m lucene_indexer.search --query "star_count:[1000 TO *]" --type range
    python -m lucene_indexer.search --query '"machine learning"' --type phrase
    python -m lucene_indexer.search --query "pyhton" --type fuzzy
"""

import argparse
import json
import logging
import sys
from dataclasses import dataclass, field
from enum import Enum
from pathlib import Path
from typing import Dict, List, Optional, Tuple

# Try to import PyLucene
try:
    import lucene
    from java.nio.file import Paths as JPaths
    from org.apache.lucene.analysis.standard import StandardAnalyzer
    from org.apache.lucene.index import DirectoryReader, Term
    from org.apache.lucene.queryparser.classic import QueryParser, MultiFieldQueryParser
    from org.apache.lucene.search import (
        IndexSearcher, BooleanQuery, BooleanClause,
        TermQuery, PhraseQuery, FuzzyQuery,
        MatchAllDocsQuery, Sort, SortField
    )
    from org.apache.lucene.search import IntPoint, LongPoint
    from org.apache.lucene.store import FSDirectory
    LUCENE_AVAILABLE = True
except ImportError:
    LUCENE_AVAILABLE = False

from .schema import IndexSchema, FieldType

logger = logging.getLogger(__name__)


class QueryType(Enum):
    """Supported query types."""
    SIMPLE = "simple"       # Basic text query across all text fields
    BOOLEAN = "boolean"     # Boolean AND/OR queries
    RANGE = "range"         # Numeric range queries
    PHRASE = "phrase"       # Exact phrase matching
    FUZZY = "fuzzy"         # Typo-tolerant fuzzy matching
    COMBINED = "combined"   # Combination of multiple query types


@dataclass
class SearchResult:
    """Single search result."""
    doc_id: str
    score: float
    title: str
    path: str
    # Entity fields
    topics: List[str] = field(default_factory=list)
    languages: List[str] = field(default_factory=list)
    license: Optional[str] = None
    star_count: Optional[int] = None
    fork_count: Optional[int] = None
    url: Optional[str] = None
    # Wiki fields
    wiki_titles: List[str] = field(default_factory=list)
    wiki_page_ids: List[int] = field(default_factory=list)
    # Metadata
    content_length: Optional[int] = None

    def to_dict(self) -> Dict:
        """Convert to dictionary for JSON serialization."""
        return {
            "doc_id": self.doc_id,
            "score": round(self.score, 6),
            "title": self.title,
            "path": self.path,
            "topics": self.topics,
            "languages": self.languages,
            "license": self.license,
            "star_count": self.star_count,
            "fork_count": self.fork_count,
            "url": self.url,
            "wiki_titles": self.wiki_titles,
            "wiki_page_ids": self.wiki_page_ids,
            "content_length": self.content_length,
        }


class LuceneSearcher:
    """
    Lucene-based search engine supporting multiple query types.

    Query Types:
    1. SIMPLE: Basic full-text search across title, content, topics, wiki_title
    2. BOOLEAN: Explicit AND/OR queries (e.g., "python AND web")
    3. RANGE: Numeric range queries (e.g., "star_count:[1000 TO 5000]")
    4. PHRASE: Exact phrase matching (e.g., '"machine learning"')
    5. FUZZY: Typo-tolerant matching (e.g., "pyhton~2")
    """

    # Default fields for multi-field search
    DEFAULT_SEARCH_FIELDS = [
        "title",
        "content",
        "topics",
        "languages",
        "wiki_title",
        "wiki_abstract",
    ]

    # Field boosts for relevance tuning
    FIELD_BOOSTS = {
        "title": 2.0,
        "topics": 1.5,
        "wiki_title": 1.5,
        "content": 1.0,
        "languages": 1.2,
        "wiki_abstract": 0.8,
    }

    def __init__(self, index_dir: Path):
        """
        Initialize the searcher.

        Args:
            index_dir: Path to Lucene index directory
        """
        self.index_dir = Path(index_dir)
        self.schema = IndexSchema()

        if not LUCENE_AVAILABLE:
            raise RuntimeError(
                "PyLucene is not installed. See lucene_indexer/build.py for installation instructions."
            )

        # Initialize JVM
        if not lucene.getVMEnv():
            lucene.initVM(vmargs=['-Djava.awt.headless=true'])
        else:
            lucene.getVMEnv().attachCurrentThread()

        # Open index
        if not self.index_dir.exists():
            raise FileNotFoundError(f"Index directory not found: {self.index_dir}")

        index_path = JPaths.get(str(self.index_dir))
        self.directory = FSDirectory.open(index_path)
        self.reader = DirectoryReader.open(self.directory)
        self.searcher = IndexSearcher(self.reader)
        self.analyzer = StandardAnalyzer()

        logger.info(f"Opened index with {self.reader.numDocs()} documents")

    def close(self):
        """Close the searcher and release resources."""
        if hasattr(self, 'reader'):
            self.reader.close()
        if hasattr(self, 'directory'):
            self.directory.close()

    def _extract_result(self, doc, score: float) -> SearchResult:
        """Extract SearchResult from Lucene document."""
        # Helper to get multi-valued fields
        def get_multi(field_name: str) -> List[str]:
            values = doc.getValues(field_name)
            return list(values) if values else []

        def get_single(field_name: str) -> Optional[str]:
            return doc.get(field_name)

        def get_int(field_name: str) -> Optional[int]:
            val = doc.get(field_name)
            if val:
                try:
                    return int(val)
                except ValueError:
                    pass
            return None

        return SearchResult(
            doc_id=get_single("doc_id") or "",
            score=score,
            title=get_single("title") or "",
            path=get_single("path") or "",
            topics=get_multi("topics"),
            languages=get_multi("languages"),
            license=get_single("license"),
            star_count=get_int("star_count"),
            fork_count=get_int("fork_count"),
            url=get_single("url"),
            wiki_titles=get_multi("wiki_title"),
            wiki_page_ids=[int(x) for x in get_multi("wiki_page_id") if x.isdigit()],
            content_length=get_int("content_length"),
        )

    # =========================================================================
    # Query Type Implementations
    # =========================================================================

    def search_simple(
        self,
        query_text: str,
        top_k: int = 10,
        fields: Optional[List[str]] = None,
    ) -> List[SearchResult]:
        """
        Simple full-text search across multiple fields.

        This is the default search mode - tokenizes the query and
        searches across title, content, topics, etc.

        Args:
            query_text: Search query string
            top_k: Maximum number of results
            fields: Fields to search (defaults to DEFAULT_SEARCH_FIELDS)

        Returns:
            List of SearchResult objects
        """
        if not query_text.strip():
            return []

        search_fields = fields or self.DEFAULT_SEARCH_FIELDS

        # Create multi-field query parser with boosts
        parser = MultiFieldQueryParser(
            search_fields,
            self.analyzer,
        )

        # Set default operator to OR for broader matching
        parser.setDefaultOperator(QueryParser.Operator.OR)

        try:
            query = parser.parse(query_text)
        except Exception as e:
            logger.warning(f"Query parse failed: {e}, using escaped query")
            query = parser.parse(QueryParser.escape(query_text))

        return self._execute_search(query, top_k)

    def search_boolean(
        self,
        query_text: str,
        top_k: int = 10,
    ) -> List[SearchResult]:
        """
        Boolean query with explicit AND/OR operators.

        Examples:
            "python AND web"        - Documents with both terms
            "python OR javascript"  - Documents with either term
            "python AND NOT java"   - Python but not Java
            "+python +docker"       - Both required (alternative syntax)

        Args:
            query_text: Boolean query string
            top_k: Maximum number of results

        Returns:
            List of SearchResult objects
        """
        if not query_text.strip():
            return []

        # Use QueryParser which supports AND/OR/NOT
        parser = MultiFieldQueryParser(
            self.DEFAULT_SEARCH_FIELDS,
            self.analyzer,
        )

        # Set default to AND for boolean queries
        parser.setDefaultOperator(QueryParser.Operator.AND)

        try:
            query = parser.parse(query_text)
        except Exception as e:
            logger.warning(f"Boolean query parse failed: {e}")
            return []

        return self._execute_search(query, top_k)

    def search_range(
        self,
        field: str,
        min_value: Optional[int] = None,
        max_value: Optional[int] = None,
        top_k: int = 10,
    ) -> List[SearchResult]:
        """
        Range query for numeric fields (star_count, fork_count, content_length).

        Examples:
            field="star_count", min_value=1000         - >1000 stars
            field="star_count", min_value=100, max_value=1000  - 100-1000 stars
            field="content_length", max_value=10000    - Small documents

        Args:
            field: Numeric field name (star_count, fork_count, content_length)
            min_value: Minimum value (inclusive), None for unbounded
            max_value: Maximum value (inclusive), None for unbounded
            top_k: Maximum number of results

        Returns:
            List of SearchResult objects
        """
        # Validate field is numeric
        numeric_fields = self.schema.get_numeric_fields()
        if field not in numeric_fields:
            logger.warning(f"Field {field} is not a numeric field. Available: {numeric_fields}")
            return []

        # Handle unbounded values
        min_val = min_value if min_value is not None else -2147483648  # Integer.MIN_VALUE
        max_val = max_value if max_value is not None else 2147483647   # Integer.MAX_VALUE

        # Create range query
        query = IntPoint.newRangeQuery(field, min_val, max_val)

        return self._execute_search(query, top_k)

    def search_phrase(
        self,
        phrase: str,
        field: str = "content",
        slop: int = 0,
        top_k: int = 10,
    ) -> List[SearchResult]:
        """
        Phrase query for exact phrase matching.

        Finds documents containing the exact phrase (words in order).
        Slop parameter allows for word distance flexibility.

        Examples:
            phrase="machine learning"          - Exact phrase
            phrase="machine learning", slop=1  - Allow 1 word between

        Args:
            phrase: Phrase to search for
            field: Field to search in (default: content)
            slop: Maximum word distance (0 = exact phrase)
            top_k: Maximum number of results

        Returns:
            List of SearchResult objects
        """
        if not phrase.strip():
            return []

        # Tokenize the phrase
        words = phrase.lower().split()
        if not words:
            return []

        # Build phrase query
        builder = PhraseQuery.Builder()
        builder.setSlop(slop)

        for word in words:
            builder.add(Term(field, word))

        query = builder.build()

        return self._execute_search(query, top_k)

    def search_fuzzy(
        self,
        term: str,
        field: str = "content",
        max_edits: int = 2,
        top_k: int = 10,
    ) -> List[SearchResult]:
        """
        Fuzzy query for typo-tolerant matching.

        Uses Levenshtein edit distance to find similar terms.
        Useful for handling typos and spelling variations.

        Examples:
            term="pyhton"    - Matches "python" (1 edit)
            term="javascrpt" - Matches "javascript" (1 edit)

        Args:
            term: Term to search for (possibly misspelled)
            field: Field to search in
            max_edits: Maximum edit distance (1 or 2)
            top_k: Maximum number of results

        Returns:
            List of SearchResult objects
        """
        if not term.strip():
            return []

        # Cap max edits at 2 (Lucene limit)
        max_edits = min(max_edits, 2)

        # Create fuzzy query
        fuzzy_term = Term(field, term.lower())
        query = FuzzyQuery(fuzzy_term, max_edits)

        return self._execute_search(query, top_k)

    def search_combined(
        self,
        text_query: Optional[str] = None,
        topics: Optional[List[str]] = None,
        languages: Optional[List[str]] = None,
        min_stars: Optional[int] = None,
        max_stars: Optional[int] = None,
        license_type: Optional[str] = None,
        has_wiki: bool = False,
        top_k: int = 10,
    ) -> List[SearchResult]:
        """
        Combined query with multiple filters.

        Combines text search with entity and wiki filters.
        All specified conditions are ANDed together.

        Examples:
            text_query="web framework", topics=["python"], min_stars=1000
            languages=["javascript"], has_wiki=True, license_type="MIT"

        Args:
            text_query: Optional text search query
            topics: List of required topics
            languages: List of required languages
            min_stars: Minimum star count
            max_stars: Maximum star count
            license_type: Exact license match
            has_wiki: If True, only return wiki-matched documents
            top_k: Maximum number of results

        Returns:
            List of SearchResult objects
        """
        builder = BooleanQuery.Builder()

        # Text query (if provided)
        if text_query:
            parser = MultiFieldQueryParser(
                self.DEFAULT_SEARCH_FIELDS,
                self.analyzer,
            )
            try:
                text_q = parser.parse(text_query)
                builder.add(text_q, BooleanClause.Occur.MUST)
            except Exception as e:
                logger.warning(f"Text query parse failed: {e}")

        # Topic filters
        if topics:
            for topic in topics:
                term_q = TermQuery(Term("topics", topic.lower()))
                builder.add(term_q, BooleanClause.Occur.MUST)

        # Language filters
        if languages:
            for lang in languages:
                term_q = TermQuery(Term("languages", lang.lower()))
                builder.add(term_q, BooleanClause.Occur.MUST)

        # Star count range
        if min_stars is not None or max_stars is not None:
            min_val = min_stars if min_stars is not None else 0
            max_val = max_stars if max_stars is not None else 2147483647
            range_q = IntPoint.newRangeQuery("star_count", min_val, max_val)
            builder.add(range_q, BooleanClause.Occur.MUST)

        # License filter
        if license_type:
            license_q = TermQuery(Term("license", license_type))
            builder.add(license_q, BooleanClause.Occur.MUST)

        # Wiki presence filter
        if has_wiki:
            # Match any document with wiki_page_id set
            wiki_q = LongPoint.newRangeQuery("wiki_page_id", 1, 9223372036854775807)
            builder.add(wiki_q, BooleanClause.Occur.MUST)

        query = builder.build()

        # If no clauses were added, match all
        if builder.build().clauses().size() == 0:
            query = MatchAllDocsQuery()

        return self._execute_search(query, top_k)

    def _execute_search(self, query, top_k: int) -> List[SearchResult]:
        """Execute a Lucene query and return results."""
        results = []

        try:
            top_docs = self.searcher.search(query, top_k)

            for score_doc in top_docs.scoreDocs:
                doc = self.searcher.doc(score_doc.doc)
                result = self._extract_result(doc, score_doc.score)
                results.append(result)

        except Exception as e:
            logger.error(f"Search execution failed: {e}")

        return results

    # =========================================================================
    # Convenience Methods
    # =========================================================================

    def search(
        self,
        query: str,
        query_type: QueryType = QueryType.SIMPLE,
        top_k: int = 10,
        **kwargs,
    ) -> List[SearchResult]:
        """
        Unified search interface.

        Args:
            query: Query string
            query_type: Type of query to execute
            top_k: Maximum results
            **kwargs: Additional arguments for specific query types

        Returns:
            List of SearchResult objects
        """
        if query_type == QueryType.SIMPLE:
            return self.search_simple(query, top_k)
        elif query_type == QueryType.BOOLEAN:
            return self.search_boolean(query, top_k)
        elif query_type == QueryType.PHRASE:
            return self.search_phrase(query, top_k=top_k, **kwargs)
        elif query_type == QueryType.FUZZY:
            return self.search_fuzzy(query, top_k=top_k, **kwargs)
        elif query_type == QueryType.RANGE:
            # Range query needs field and bounds from kwargs
            field = kwargs.get('field', 'star_count')
            min_val = kwargs.get('min_value')
            max_val = kwargs.get('max_value')
            return self.search_range(field, min_val, max_val, top_k)
        elif query_type == QueryType.COMBINED:
            return self.search_combined(text_query=query, top_k=top_k, **kwargs)
        else:
            return self.search_simple(query, top_k)

    def get_stats(self) -> Dict:
        """Get index statistics."""
        return {
            "num_docs": self.reader.numDocs(),
            "num_deleted_docs": self.reader.numDeletedDocs(),
            "index_dir": str(self.index_dir),
        }


# =============================================================================
# CLI Interface
# =============================================================================

def parse_args(argv: Optional[List[str]] = None) -> argparse.Namespace:
    """Parse command-line arguments."""
    parser = argparse.ArgumentParser(
        description="Search PyLucene index with various query types"
    )
    parser.add_argument(
        '--index',
        default='workspace/store/lucene_index',
        help='Path to Lucene index directory'
    )
    parser.add_argument(
        '--query',
        required=True,
        help='Search query'
    )
    parser.add_argument(
        '--type',
        choices=['simple', 'boolean', 'range', 'phrase', 'fuzzy', 'combined'],
        default='simple',
        help='Query type (default: simple)'
    )
    parser.add_argument(
        '--field',
        default='content',
        help='Field for phrase/fuzzy queries (default: content)'
    )
    parser.add_argument(
        '--min-value',
        type=int,
        help='Minimum value for range queries'
    )
    parser.add_argument(
        '--max-value',
        type=int,
        help='Maximum value for range queries'
    )
    parser.add_argument(
        '--slop',
        type=int,
        default=0,
        help='Slop for phrase queries (word distance)'
    )
    parser.add_argument(
        '--max-edits',
        type=int,
        default=2,
        help='Max edit distance for fuzzy queries'
    )
    parser.add_argument(
        '--top',
        type=int,
        default=10,
        help='Number of results to return'
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

    try:
        searcher = LuceneSearcher(Path(args.index))
    except (RuntimeError, FileNotFoundError) as e:
        logger.error(str(e))
        return 1

    try:
        query_type = QueryType(args.type)

        if query_type == QueryType.RANGE:
            results = searcher.search_range(
                args.field,
                args.min_value,
                args.max_value,
                args.top,
            )
        elif query_type == QueryType.PHRASE:
            results = searcher.search_phrase(
                args.query,
                args.field,
                args.slop,
                args.top,
            )
        elif query_type == QueryType.FUZZY:
            results = searcher.search_fuzzy(
                args.query,
                args.field,
                args.max_edits,
                args.top,
            )
        else:
            results = searcher.search(
                args.query,
                query_type,
                args.top,
            )

        if args.json:
            print(json.dumps([r.to_dict() for r in results], indent=2))
        else:
            print(f"\n{'=' * 60}")
            print(f"Query: {args.query}")
            print(f"Type: {args.type}")
            print(f"Results: {len(results)}")
            print(f"{'=' * 60}\n")

            for i, result in enumerate(results, 1):
                print(f"{i}. {result.title}")
                print(f"   Score: {result.score:.6f}")
                print(f"   Doc ID: {result.doc_id}")
                if result.topics:
                    print(f"   Topics: {', '.join(result.topics[:5])}")
                if result.languages:
                    print(f"   Languages: {', '.join(result.languages[:5])}")
                if result.star_count:
                    print(f"   Stars: {result.star_count:,}")
                if result.wiki_titles:
                    print(f"   Wiki: {', '.join(result.wiki_titles[:3])}")
                print()

        return 0

    except Exception as e:
        logger.error(f"Search failed: {e}")
        return 1

    finally:
        searcher.close()


if __name__ == "__main__":
    sys.exit(main())
