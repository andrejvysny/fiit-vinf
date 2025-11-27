# Konzultácia 4: PyLucene Index Implementation

**Deadline**: 28. 11. 2025
**Status**: Implementation Complete

---

## Requirements Checklist

| Requirement | Status | Implementation |
|-------------|--------|----------------|
| Complete Spark code (wiki, extracted data, join) | ✅ | `spark/jobs/*.py` |
| PyLucene index design | ✅ | `lucene_indexer/schema.py` |
| PyLucene index implementation | ✅ | `lucene_indexer/build.py` |
| Field justifications | ✅ | See "Index Schema Design" section |
| Integration into application | ✅ | `lucene_indexer/unified_search.py` |
| Query comparison (old vs new) | ✅ | `lucene_indexer/compare.py` |
| Query types documentation | ✅ | See "Query Types" section |

---

## 1. Spark Code Summary (Konzultácia 3 + 4)

### 1.1 HTML Extractor (`spark/jobs/html_extractor.py`)

**CLI**: `bin/spark_extract --config config.yml`

**Outputs**:
- `workspace/store/text/{doc_id}.txt` - Cleaned text files
- `workspace/store/entities/entities.tsv` - Entity annotations

**Entity Types Extracted**:
- `TOPICS` - GitHub repository topics
- `LANG_STATS` - Programming languages
- `LICENSE` - Software license
- `STAR_COUNT` - Star count
- `FORK_COUNT` - Fork count
- `URL` - Repository URLs
- `README` - README content markers

### 1.2 Wikipedia Extractor (`spark/jobs/wiki_extractor.py`)

**CLI**: `bin/spark_wiki_extract --wiki-in wiki_dump --out workspace/store/wiki`

**Outputs** (7 TSV files + text):
| File | Columns | Purpose |
|------|---------|---------|
| `pages.tsv` | page_id, title, norm_title, ns, redirect_to, timestamp | Page metadata |
| `categories.tsv` | page_id, category, norm_category | Category associations |
| `links.tsv` | page_id, link_title, norm_link_title | Internal links |
| `infobox.tsv` | page_id, key, value | Infobox fields |
| `abstract.tsv` | page_id, abstract_text | Lead section text |
| `aliases.tsv` | alias_norm_title, canonical_norm_title | Redirect mappings |
| `wiki_text_metadata.tsv` | page_id, title, content_sha256, content_length, timestamp | Text file metadata |
| `text/*.txt` | Full article text (SHA256-named) | Article content |

**Regex Patterns Used** (`spark/lib/wiki_regexes.py`):
- `TITLE_PATTERN` - Extract page title
- `PAGE_ID_PATTERN` - Extract page ID
- `TEXT_PATTERN` - Extract wikitext content
- `CATEGORY_PATTERN` - Extract `[[Category:X]]`
- `INFOBOX_PATTERN` - Extract infobox fields
- `normalize_title()` - Lowercase, ASCII-fold, punctuation collapse

### 1.3 Entity-Wikipedia Join (`spark/jobs/join_html_wiki.py`)

**CLI**: `bin/spark_join_wiki --entities ... --wiki ... --out ...`

**Outputs**:

| File | Columns | Purpose |
|------|---------|---------|
| `html_wiki.tsv` | doc_id, entity_type, entity_value, norm_value, wiki_page_id, wiki_title, **wiki_abstract**, **wiki_categories**, join_key, confidence | Per-entity join results |
| `html_wiki_agg.tsv` | doc_id, joined_page_ids, num_joined_pages, num_topics_joined, num_licenses_joined, num_langs_joined | Per-document aggregates |
| `join_stats.json` | Statistics including **unique_wiki_pages_joined** | Join metrics |

**Confidence Labels**:
- `exact+cat` - Direct title match + relevant category
- `alias+cat` - Alias/redirect match + relevant category
- `exact+abs` - Direct title match + abstract available
- `alias+abs` - Alias match + abstract available

**Key Metrics**:
- `unique_wiki_pages_joined` - Count of distinct Wikipedia pages matched
- `unique_docs_with_wiki` - Documents with at least one wiki match
- Per-type breakdown (TOPICS, LICENSE, LANG_STATS)

### 1.4 Interesting Wiki Attributes

| Attribute | Source | Purpose in Index |
|-----------|--------|------------------|
| `wiki_title` | pages.tsv | Searchable Wikipedia concept names |
| `wiki_categories` | categories.tsv | Technology-related category filtering |
| `wiki_abstract` | abstract.tsv | Semantic context for phrase queries |
| Infobox type | infobox.tsv | Detect `Infobox programming language`, etc. |

---

## 2. PyLucene Index Design

### 2.1 Index Schema (`lucene_indexer/schema.py`)

| Field | Lucene Type | Stored | Indexed | Justification |
|-------|-------------|--------|---------|---------------|
| `doc_id` | StringField | Yes | Yes | Stable document key for cross-stage linking |
| `title` | TextField | Yes | Yes | Full-text search on document titles |
| `content` | TextField | No | Yes | Main body text (not stored to save space) |
| `path` | StoredField | Yes | No | File path for content retrieval |
| `url` | StringField | Yes | Yes | GitHub URL for exact matching |
| `topics` | TextField (multi) | Yes | Yes | GitHub topics, tokenized for partial matching |
| `languages` | TextField (multi) | Yes | Yes | Programming languages |
| `license` | StringField | Yes | Yes | Exact license matching (MIT, Apache-2.0) |
| `star_count` | IntPoint + DV | Yes | Yes | Range queries, sorting |
| `fork_count` | IntPoint + DV | Yes | Yes | Range queries |
| `wiki_page_id` | LongPoint | Yes | Yes | Filter wiki-matched documents |
| `wiki_title` | TextField (multi) | Yes | Yes | Search by Wikipedia concepts |
| `wiki_categories` | TextField (multi) | Yes | Yes | Category-based filtering |
| `wiki_abstract` | TextField | Yes | Yes | Semantic search, phrase queries |
| `join_confidence` | StringField | Yes | Yes | Filter by match quality |
| `indexed_at` | LongPoint | Yes | Yes | Date range queries |
| `content_length` | IntPoint | Yes | Yes | Filter by document size |

### 2.2 Field Type Rationale

**TextField** (tokenized):
- Used for full-text search on natural language content
- Enables phrase queries and fuzzy matching
- Applied to: title, content, topics, languages, wiki_title, wiki_categories, wiki_abstract

**StringField** (not tokenized):
- Used for exact matching and filtering
- Preserves original value exactly
- Applied to: doc_id, url, license, join_confidence

**IntPoint/LongPoint** (numeric):
- Enables efficient range queries
- Used with DocValues for sorting
- Applied to: star_count, fork_count, wiki_page_id, indexed_at, content_length

---

## 3. Query Types Implementation

### 3.1 Simple Query
```bash
python -m lucene_indexer.search --query "python web framework" --type simple
```
Multi-field text search across title, content, topics, wiki_title, wiki_abstract.

### 3.2 Boolean Query (AND/OR/NOT)
```bash
python -m lucene_indexer.search --query "python AND docker" --type boolean
python -m lucene_indexer.search --query "javascript OR typescript" --type boolean
python -m lucene_indexer.search --query "machine learning AND NOT tensorflow" --type boolean
```

### 3.3 Range Query
```bash
python -m lucene_indexer.search --field star_count --min-value 1000 --type range
python -m lucene_indexer.search --field star_count --min-value 100 --max-value 1000 --type range
```

### 3.4 Phrase Query
```bash
python -m lucene_indexer.search --query "neural network" --type phrase
python -m lucene_indexer.search --query "dependency injection" --type phrase --slop 1
```

### 3.5 Fuzzy Query
```bash
python -m lucene_indexer.search --query "pyhton" --type fuzzy --max-edits 2
python -m lucene_indexer.search --query "javascrpt" --type fuzzy
```

---

## 4. Config-Level Search Engine Toggle

### 4.1 Configuration (`config.yml`)

```yaml
search:
  engine: "lucene"  # or "tfidf"
  tfidf_index_dir: "workspace/store/index"
  lucene_index_dir: "workspace/store/lucene_index"
```

### 4.2 Unified Search Interface

```bash
# Use Lucene
python -m lucene_indexer.unified_search --query "python web" --engine lucene

# Use TF-IDF
python -m lucene_indexer.unified_search --query "python web" --engine tfidf

# Use config default
python -m lucene_indexer.unified_search --query "python web" --config config.yml
```

### 4.3 Unified Output Format

Both engines return the same JSON structure:
```json
{
  "rank": 1,
  "doc_id": "abc123...",
  "score": 12.34,
  "title": "Repository Name",
  "url": "https://github.com/...",
  "snippet": "Topics: python, web...",
  "topics": ["python", "web"],
  "languages": ["Python", "JavaScript"],
  "license": "MIT",
  "star_count": 1500,
  "wiki_titles": ["Python (programming language)"],
  "path": "/path/to/doc.txt",
  "engine": "lucene"
}
```

---

## 5. Query Comparison: TF-IDF vs Lucene

### 5.1 Comparison Tool

```bash
# Using queries file
python -m lucene_indexer.compare \
  --queries-file queries.txt \
  --tfidf-index workspace/store/index \
  --lucene-index workspace/store/lucene_index \
  --output reports/index_comparison.md

# Using inline queries
python -m lucene_indexer.compare \
  --queries "python web,machine learning,docker" \
  --types "simple,simple,simple" \
  --output reports/comparison.md
```

### 5.2 Sample Queries File (`queries.txt`)

```
# Simple queries
simple|python web framework
simple|machine learning

# Boolean queries
boolean|python AND docker
boolean|javascript OR typescript

# Phrase queries
phrase|"neural network"
phrase|"web scraping"

# Fuzzy queries
fuzzy|pyhton
fuzzy|javascrpt

# Range queries
range|star_count:[1000 TO *]
```

### 5.3 Comparison Metrics

| Metric | Description |
|--------|-------------|
| Overlap@K | Percentage of shared results in top-K |
| Jaccard | Jaccard similarity of result sets |
| Latency | Query execution time comparison |

---

## 6. Implementation Files

| File | Purpose |
|------|---------|
| `lucene_indexer/__init__.py` | Module exports |
| `lucene_indexer/schema.py` | Index schema with field justifications |
| `lucene_indexer/build.py` | Index builder (text + entities + wiki) |
| `lucene_indexer/search.py` | Search engine with 5 query types |
| `lucene_indexer/compare.py` | TF-IDF vs Lucene comparison |
| `lucene_indexer/unified_search.py` | Config-driven search interface |
| `queries.txt` | Sample queries for comparison |

---

## 7. Usage Examples

### Build Pipeline
```bash
# 1. Extract HTML
bin/spark_extract --config config.yml

# 2. Extract Wikipedia
bin/spark_wiki_extract --wiki-in wiki_dump --out workspace/store/wiki

# 3. Join entities with Wikipedia
bin/spark_join_wiki \
  --entities workspace/store/entities/entities.tsv \
  --wiki workspace/store/wiki \
  --out workspace/store/join

# 4. Build PyLucene index
python -m lucene_indexer.build \
  --text-dir workspace/store/text \
  --entities workspace/store/entities/entities.tsv \
  --wiki-join workspace/store/join/html_wiki.tsv \
  --output workspace/store/lucene_index
```

### Search Examples
```bash
# Simple search
python -m lucene_indexer.search --query "python web" --type simple

# Boolean with AND
python -m lucene_indexer.search --query "python AND docker" --type boolean

# Range query (popular repos)
python -m lucene_indexer.search --field star_count --min-value 1000 --type range

# Phrase query
python -m lucene_indexer.search --query "machine learning" --type phrase

# Fuzzy (typo-tolerant)
python -m lucene_indexer.search --query "pyhton" --type fuzzy

# Compare indexes
python -m lucene_indexer.compare --queries-file queries.txt --output reports/comparison.md
```

---

## 8. Summary

This implementation provides:

1. **Complete Spark Pipeline**:
   - HTML extraction with entity detection
   - Wikipedia dump extraction with regex patterns
   - Entity-Wikipedia join with `unique_wiki_pages_joined` metric

2. **Rich PyLucene Index**:
   - 16 fields with documented justifications
   - Multi-valued fields for topics, languages, wiki data
   - Numeric fields for range queries and sorting

3. **Five Query Types**:
   - Simple (full-text)
   - Boolean (AND/OR/NOT)
   - Range (numeric filtering)
   - Phrase (exact matching)
   - Fuzzy (typo tolerance)

4. **Unified Search Interface**:
   - Config-driven engine selection
   - Consistent output format
   - Easy comparison between TF-IDF and Lucene

5. **Comparison Tools**:
   - Query file support
   - Overlap and latency metrics
   - Markdown report generation
