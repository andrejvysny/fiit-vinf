"""
PyLucene-based indexer for GitHub-Wikipedia enriched documents.

This module provides:
- LuceneIndexBuilder: Build Lucene indexes from extracted documents
- LuceneSearcher: Search with Boolean, Range, Phrase, and Fuzzy queries
- QueryComparison: Compare results between TF-IDF and Lucene indexes
- UnifiedSearcher: Config-driven search that can switch between TF-IDF and Lucene
"""

from .schema import FIELD_DEFINITIONS, IndexSchema
from .build import LuceneIndexBuilder
from .search import LuceneSearcher, QueryType
from .compare import QueryComparison
from .unified_search import UnifiedSearcher, UnifiedSearchResult

__all__ = [
    'FIELD_DEFINITIONS',
    'IndexSchema',
    'LuceneIndexBuilder',
    'LuceneSearcher',
    'QueryType',
    'QueryComparison',
    'UnifiedSearcher',
    'UnifiedSearchResult',
]
