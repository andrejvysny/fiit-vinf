"""
PyLucene Index Schema Definition.

This module defines the index schema with justification for each field.
The schema is designed to support:
- Full-text search on document content
- Entity-based filtering (topics, languages, licenses)
- Range queries on numeric fields (star count, fork count)
- Wiki enrichment fields for enhanced search

Field Types Used:
- TextField: Tokenized, indexed for full-text search
- StringField: Not tokenized, for exact matching and filtering
- IntPoint: For range queries on integers
- StoredField: Values stored but not indexed (retrieval only)
- NumericDocValuesField: For sorting and faceting on numeric fields
"""

from dataclasses import dataclass
from enum import Enum
from typing import Dict, List, Optional


class FieldType(Enum):
    """Lucene field types supported by this schema."""
    TEXT = "TextField"           # Tokenized, full-text searchable
    STRING = "StringField"       # Not tokenized, exact match
    INT_POINT = "IntPoint"       # Range queries on integers
    LONG_POINT = "LongPoint"     # Range queries on longs
    STORED = "StoredField"       # Stored only, not indexed
    NUMERIC_DOC_VALUES = "NumericDocValuesField"  # For sorting


@dataclass
class FieldDefinition:
    """Definition of a single index field."""
    name: str
    field_type: FieldType
    stored: bool
    indexed: bool
    tokenized: bool
    justification: str
    multi_valued: bool = False


# ============================================================================
# INDEX SCHEMA DEFINITION WITH JUSTIFICATIONS
# ============================================================================

FIELD_DEFINITIONS: Dict[str, FieldDefinition] = {
    # -------------------------------------------------------------------------
    # Core Document Fields
    # -------------------------------------------------------------------------
    "doc_id": FieldDefinition(
        name="doc_id",
        field_type=FieldType.STRING,
        stored=True,
        indexed=True,
        tokenized=False,
        justification="""
        Document identifier (SHA256 hash of source URL).
        Stored for retrieval, indexed for exact lookup.
        Not tokenized because it's an opaque identifier.
        Used to link search results back to source documents.
        """
    ),

    "title": FieldDefinition(
        name="title",
        field_type=FieldType.TEXT,
        stored=True,
        indexed=True,
        tokenized=True,
        justification="""
        Document title extracted from HTML <title> tag.
        Tokenized for full-text search (e.g., "React Tutorial" matches "react").
        Stored for display in search results.
        High weight in relevance scoring as titles are highly descriptive.
        """
    ),

    "content": FieldDefinition(
        name="content",
        field_type=FieldType.TEXT,
        stored=False,  # Content is large, store separately
        indexed=True,
        tokenized=True,
        justification="""
        Main document text content (README, description, code comments).
        Tokenized for full-text search and phrase queries.
        NOT stored in index to reduce size - retrieve from source files.
        Primary field for relevance ranking.
        """
    ),

    "path": FieldDefinition(
        name="path",
        field_type=FieldType.STORED,
        stored=True,
        indexed=False,
        tokenized=False,
        justification="""
        Filesystem path to source document.
        Stored only - used to retrieve original content.
        Not indexed as path-based queries are not expected.
        """
    ),

    "url": FieldDefinition(
        name="url",
        field_type=FieldType.STRING,
        stored=True,
        indexed=True,
        tokenized=False,
        justification="""
        Original source URL (GitHub repository URL).
        Stored for display/navigation in results.
        Indexed as StringField for exact URL filtering.
        Not tokenized - URLs are atomic identifiers.
        """
    ),

    # -------------------------------------------------------------------------
    # GitHub Entity Fields
    # -------------------------------------------------------------------------
    "topics": FieldDefinition(
        name="topics",
        field_type=FieldType.TEXT,
        stored=True,
        indexed=True,
        tokenized=True,
        multi_valued=True,
        justification="""
        GitHub repository topics (e.g., "python", "machine-learning").
        Tokenized to allow partial matching (e.g., "learn" matches "machine-learning").
        Multi-valued field - repositories have multiple topics.
        Crucial for topic-based discovery and filtering.
        Supports Boolean queries like "topics:python AND topics:web".
        """
    ),

    "languages": FieldDefinition(
        name="languages",
        field_type=FieldType.TEXT,
        stored=True,
        indexed=True,
        tokenized=True,
        multi_valued=True,
        justification="""
        Programming languages used in repository (from LANG_STATS entity).
        Tokenized for flexible matching (e.g., "c++" tokenizes to searchable form).
        Multi-valued as repositories use multiple languages.
        Supports language-specific searches and filtering.
        """
    ),

    "license": FieldDefinition(
        name="license",
        field_type=FieldType.STRING,
        stored=True,
        indexed=True,
        tokenized=False,
        justification="""
        Software license type (e.g., "MIT", "Apache-2.0", "GPL-3.0").
        StringField (not tokenized) for exact license matching.
        Important for compliance-based filtering (e.g., "show only MIT licensed").
        """
    ),

    "star_count": FieldDefinition(
        name="star_count",
        field_type=FieldType.INT_POINT,
        stored=True,
        indexed=True,
        tokenized=False,
        justification="""
        GitHub star count as integer.
        IntPoint enables range queries: "star_count:[1000 TO *]" (>1000 stars).
        Stored for display and sorting in results.
        Key popularity/quality signal for ranking.
        """
    ),

    "fork_count": FieldDefinition(
        name="fork_count",
        field_type=FieldType.INT_POINT,
        stored=True,
        indexed=True,
        tokenized=False,
        justification="""
        GitHub fork count as integer.
        IntPoint for range queries on fork popularity.
        Indicates community engagement and reusability.
        """
    ),

    # -------------------------------------------------------------------------
    # Wikipedia Enrichment Fields
    # -------------------------------------------------------------------------
    "wiki_page_id": FieldDefinition(
        name="wiki_page_id",
        field_type=FieldType.LONG_POINT,
        stored=True,
        indexed=True,
        tokenized=False,
        multi_valued=True,
        justification="""
        Wikipedia page IDs linked to this document (from entity-wiki join).
        LongPoint for efficient filtering of wiki-enriched documents.
        Multi-valued as one document may link to multiple wiki pages.
        Enables queries like "show only wiki-matched documents".
        """
    ),

    "wiki_title": FieldDefinition(
        name="wiki_title",
        field_type=FieldType.TEXT,
        stored=True,
        indexed=True,
        tokenized=True,
        multi_valued=True,
        justification="""
        Wikipedia article titles matched to entities in this document.
        Tokenized for full-text search on wiki knowledge.
        Enables searching based on authoritative Wikipedia concepts.
        Example: Find repos related to "Python programming language" wiki article.
        """
    ),

    "wiki_categories": FieldDefinition(
        name="wiki_categories",
        field_type=FieldType.TEXT,
        stored=True,
        indexed=True,
        tokenized=True,
        multi_valued=True,
        justification="""
        Wikipedia categories for matched wiki pages.
        Tokenized for category-based discovery.
        Provides semantic enrichment (e.g., "Category:Programming_languages").
        Enables queries like "wiki_categories:frameworks".
        """
    ),

    "wiki_abstract": FieldDefinition(
        name="wiki_abstract",
        field_type=FieldType.TEXT,
        stored=True,
        indexed=True,
        tokenized=True,
        justification="""
        Wikipedia abstract/lead section for matched pages.
        Tokenized for full-text search on Wikipedia content.
        Provides rich context for entity disambiguation.
        Supports phrase queries on authoritative definitions.
        """
    ),

    # -------------------------------------------------------------------------
    # Metadata Fields
    # -------------------------------------------------------------------------
    "indexed_at": FieldDefinition(
        name="indexed_at",
        field_type=FieldType.LONG_POINT,
        stored=True,
        indexed=True,
        tokenized=False,
        justification="""
        Timestamp when document was indexed (epoch milliseconds).
        LongPoint for date range queries.
        Useful for incremental indexing and freshness filtering.
        """
    ),

    "content_length": FieldDefinition(
        name="content_length",
        field_type=FieldType.INT_POINT,
        stored=True,
        indexed=True,
        tokenized=False,
        justification="""
        Document content length in characters.
        IntPoint for filtering by document size.
        Can be used to filter out stub documents or overly long ones.
        """
    ),

    "join_confidence": FieldDefinition(
        name="join_confidence",
        field_type=FieldType.STRING,
        stored=True,
        indexed=True,
        tokenized=False,
        multi_valued=True,
        justification="""
        Confidence level of wiki join (exact+cat, alias+cat, exact+abs, alias+abs).
        StringField for exact filtering on confidence levels.
        Enables filtering like "show only high-confidence wiki matches".
        """
    ),
}


class IndexSchema:
    """
    Index schema manager providing field definitions and Lucene configuration.
    """

    def __init__(self):
        self.fields = FIELD_DEFINITIONS

    def get_field(self, name: str) -> Optional[FieldDefinition]:
        """Get field definition by name."""
        return self.fields.get(name)

    def get_text_fields(self) -> List[str]:
        """Get names of all text (full-text searchable) fields."""
        return [
            name for name, field in self.fields.items()
            if field.field_type == FieldType.TEXT and field.indexed
        ]

    def get_stored_fields(self) -> List[str]:
        """Get names of all stored fields."""
        return [
            name for name, field in self.fields.items()
            if field.stored
        ]

    def get_numeric_fields(self) -> List[str]:
        """Get names of all numeric (range-queryable) fields."""
        return [
            name for name, field in self.fields.items()
            if field.field_type in (FieldType.INT_POINT, FieldType.LONG_POINT)
        ]

    def get_string_fields(self) -> List[str]:
        """Get names of all string (exact match) fields."""
        return [
            name for name, field in self.fields.items()
            if field.field_type == FieldType.STRING and field.indexed
        ]

    def get_schema_documentation(self) -> str:
        """Generate human-readable schema documentation."""
        lines = [
            "# PyLucene Index Schema",
            "",
            "## Field Definitions",
            "",
        ]

        for name, field in self.fields.items():
            lines.append(f"### {name}")
            lines.append(f"- **Type**: {field.field_type.value}")
            lines.append(f"- **Stored**: {field.stored}")
            lines.append(f"- **Indexed**: {field.indexed}")
            lines.append(f"- **Tokenized**: {field.tokenized}")
            if field.multi_valued:
                lines.append(f"- **Multi-valued**: Yes")
            lines.append("")
            lines.append("**Justification**:")
            lines.append(field.justification.strip())
            lines.append("")

        return "\n".join(lines)


# Generate schema documentation on import
if __name__ == "__main__":
    schema = IndexSchema()
    print(schema.get_schema_documentation())
