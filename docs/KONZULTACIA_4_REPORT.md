# Konzultácia 4: Spark + PyLucene Implementation Report

**Student**: Andrej Vyšný
**GitHub**: https://github.com/andrejvysny/fiit-vinf
**Deadline**: 28. 11. 2025
**Status**: Complete

---

## 1. Spark Code Implementation

### 1.1 HTML Extractor (`spark/jobs/html_extractor.py`)

The HTML extractor uses PySpark DataFrame API with `mapInPandas` for efficient parallel processing of HTML files.

**Key Features**:
- DataFrame API with Catalyst optimizer for better execution plans
- `mapInPandas` for partition-level processing with pandas efficiency
- Single-pass architecture: text + entities written in one pass
- Kryo serialization and Arrow for efficient data transfer

**Entity Types Extracted**:
| Entity Type | Description | Example |
|------------|-------------|---------|
| `TOPICS` | GitHub repository topics | python, machine-learning |
| `LANG_STATS` | Programming languages | Python, JavaScript |
| `LICENSE` | Software license | MIT, Apache-2.0 |
| `STAR_COUNT` | Star count | 15000 |
| `FORK_COUNT` | Fork count | 3200 |
| `URL` | Repository URLs | https://github.com/... |
| `README` | README content markers | - |
| `EMAIL` | Email addresses | - |

**Output Files**:
- `workspace/store/text/{doc_id}.txt` - Cleaned text content
- `workspace/store/entities/entities.tsv` - TSV with columns: `doc_id`, `type`, `value`, `offsets_json`

**Execution**:
```bash
bin/cli extract                    # Full extraction
bin/cli extract --sample 100       # Test with 100 files
```

### 1.2 Wikipedia Extractor (`spark/jobs/wiki_extractor.py`)

Processes 100GB+ Wikipedia XML dumps using streaming architecture with NO caching.

**Key Features**:
- DataFrame API with `mapInPandas` (no RDD usage)
- Streaming page extraction to handle large XML files
- Deduplication by content SHA256 hash
- Adaptive partitioning for large files (>50GB → 256+ partitions)

**Regex Patterns** (`spark/lib/wiki_regexes.py`):
| Pattern | Purpose |
|---------|---------|
| `PAGE_PATTERN` | Extract `<page>...</page>` blocks |
| `TITLE_PATTERN` | Extract `<title>` |
| `PAGE_ID_PATTERN` | Extract `<id>` |
| `TEXT_PATTERN` | Extract wikitext content |
| `CATEGORY_PATTERN` | Extract `[[Category:X]]` |
| `INFOBOX_PATTERN` | Extract infobox key-value pairs |
| `LINK_PATTERN` | Extract internal `[[link]]` |

**Output Files** (7 TSV + text):
| File | Columns | Records |
|------|---------|---------|
| `pages.tsv` | page_id, title, norm_title, ns, redirect_to, timestamp | Page metadata |
| `categories.tsv` | page_id, category, norm_category | Category associations |
| `links.tsv` | page_id, link_title, norm_link_title | Internal wiki links |
| `infobox.tsv` | page_id, key, value | Infobox structured data |
| `abstract.tsv` | page_id, abstract_text | Lead section text |
| `aliases.tsv` | alias_norm_title, canonical_norm_title | Redirect mappings |
| `wiki_text_metadata.tsv` | page_id, title, content_sha256, content_length, timestamp | Text metadata |
| `text/*.txt` | Full article text | SHA256-named files |

**Execution**:
```bash
bin/cli wiki                       # Full extraction
bin/cli wiki --wiki-max-pages 1000 # Test with 1000 pages
```

### 1.3 Entity-Wikipedia Join (`spark/jobs/join_html_wiki.py`)

Joins HTML entities with Wikipedia canonical data using pure DataFrame API.

**Key Features**:
- Canonical mapping: direct pages + redirects (aliases)
- Title normalization for matching (lowercase, ASCII-fold, punctuation collapse)
- Confidence scoring with category and abstract signals
- Per-document aggregation with page counts

**Join Logic**:
1. Load entities from HTML extraction
2. Load Wikipedia pages, aliases, categories, abstracts
3. Build canonical mapping (direct + alias entries)
4. Normalize entity values using same function as Wikipedia titles
5. Left join entities with canonical mapping
6. Add category signals and abstracts
7. Calculate confidence labels

**Confidence Labels**:
| Label | Meaning |
|-------|---------|
| `exact+cat` | Direct title match + relevant category (e.g., "programming", "software") |
| `alias+cat` | Redirect/alias match + relevant category |
| `exact+abs` | Direct title match + abstract available |
| `alias+abs` | Alias match + abstract available |
| `exact` | Direct match only |
| `alias` | Alias match only |

**Output Files**:
| File | Columns |
|------|---------|
| `html_wiki.tsv` | doc_id, entity_type, entity_value, norm_value, wiki_page_id, wiki_title, wiki_abstract, wiki_categories, join_key, confidence |
| `html_wiki_agg.tsv` | doc_id, joined_page_ids, num_joined_pages, num_topics_joined, num_licenses_joined, num_langs_joined |
| `join_stats.json` | Statistics including `unique_wiki_pages_joined`, `unique_docs_with_wiki` |

**Execution**:
```bash
bin/cli join                           # Full join
bin/cli join --entities-max-rows 1000  # Test with 1000 entities
```

---

## 2. PyLucene Index Design and Implementation

### 2.1 Index Schema (`lucene_indexer/schema.py`)

The index uses 16 fields designed for different query types:

| Field | Lucene Type | Stored | Indexed | Tokenized | Multi-valued |
|-------|-------------|--------|---------|-----------|--------------|
| `doc_id` | StringField | Yes | Yes | No | No |
| `title` | TextField | Yes | Yes | Yes | No |
| `content` | TextField | No | Yes | Yes | No |
| `path` | StoredField | Yes | No | No | No |
| `url` | StringField | Yes | Yes | No | No |
| `topics` | TextField | Yes | Yes | Yes | Yes |
| `languages` | TextField | Yes | Yes | Yes | Yes |
| `license` | StringField | Yes | Yes | No | No |
| `star_count` | IntPoint + NumericDocValues | Yes | Yes | No | No |
| `fork_count` | IntPoint | Yes | Yes | No | No |
| `wiki_page_id` | LongPoint | Yes | Yes | No | Yes |
| `wiki_title` | TextField | Yes | Yes | Yes | Yes |
| `wiki_categories` | TextField | Yes | Yes | Yes | Yes |
| `wiki_abstract` | TextField | Yes | Yes | Yes | No |
| `join_confidence` | StringField | Yes | Yes | No | Yes |
| `indexed_at` | LongPoint | Yes | Yes | No | No |
| `content_length` | IntPoint | Yes | Yes | No | No |

### 2.2 Field Justifications

#### Core Document Fields

**doc_id** (StringField):
- SHA256 hash of source URL
- Not tokenized - opaque identifier for exact lookup
- Used to link search results back to source documents

**title** (TextField):
- Document title from HTML `<title>` tag
- Tokenized for full-text search (e.g., "React Tutorial" matches "react")
- High weight in relevance scoring as titles are descriptive
- Stored for display in search results

**content** (TextField):
- Main document text (README, description, code comments)
- Tokenized for full-text search and phrase queries
- NOT stored to reduce index size - retrieve from source files
- Primary field for relevance ranking

**path** (StoredField):
- Filesystem path to source document
- Stored only - used to retrieve original content
- Not indexed as path-based queries are not expected

#### GitHub Entity Fields

**topics** (TextField, multi-valued):
- GitHub repository topics (e.g., "python", "machine-learning")
- Tokenized to allow partial matching
- Multi-valued - repositories have multiple topics
- Supports Boolean queries: `topics:python AND topics:web`

**languages** (TextField, multi-valued):
- Programming languages from LANG_STATS entity
- Tokenized for flexible matching (e.g., "c++" tokenizes properly)
- Multi-valued - repositories use multiple languages

**license** (StringField):
- Software license type (e.g., "MIT", "Apache-2.0", "GPL-3.0")
- Not tokenized for exact license matching
- Important for compliance-based filtering

**star_count** (IntPoint + NumericDocValuesField):
- GitHub star count as integer
- IntPoint enables range queries: `star_count:[1000 TO *]`
- DocValues for sorting by popularity
- Key popularity/quality signal for ranking

**fork_count** (IntPoint):
- GitHub fork count as integer
- Range queries on fork popularity
- Indicates community engagement and reusability

#### Wikipedia Enrichment Fields

**wiki_page_id** (LongPoint, multi-valued):
- Wikipedia page IDs linked to document from entity-wiki join
- Efficient filtering of wiki-enriched documents
- Multi-valued - one document may link to multiple wiki pages
- Enables queries: "show only wiki-matched documents"

**wiki_title** (TextField, multi-valued):
- Wikipedia article titles matched to entities
- Tokenized for full-text search on wiki knowledge
- Search by authoritative Wikipedia concepts
- Example: Find repos related to "Python programming language" wiki article

**wiki_categories** (TextField, multi-valued):
- Wikipedia categories for matched wiki pages
- Tokenized for category-based discovery
- Semantic enrichment (e.g., "Category:Programming_languages")
- Enables queries: `wiki_categories:frameworks`

**wiki_abstract** (TextField):
- Wikipedia abstract/lead section for matched pages
- Tokenized for full-text search on Wikipedia content
- Rich context for entity disambiguation
- Supports phrase queries on authoritative definitions

#### Metadata Fields

**indexed_at** (LongPoint):
- Timestamp when document was indexed (epoch milliseconds)
- Date range queries for freshness filtering
- Useful for incremental indexing

**content_length** (IntPoint):
- Document content length in characters
- Filter by document size
- Can filter out stub documents or overly long ones

**join_confidence** (StringField, multi-valued):
- Confidence level of wiki join (exact+cat, alias+cat, etc.)
- Exact filtering on confidence levels
- Filter: "show only high-confidence wiki matches"

### 2.3 Index Building (`lucene_indexer/build.py`)

The index builder combines three data sources:
1. **Text documents** (`workspace/store/text/`) - Main content
2. **Entity annotations** (`workspace/store/entities/entities.tsv`) - GitHub metadata
3. **Wiki join results** (`workspace/store/join/html_wiki.tsv`) - Wikipedia enrichment

**Process**:
1. Initialize PyLucene JVM
2. Load entities into cache (keyed by doc_id)
3. Load wiki joins into cache (keyed by doc_id)
4. Iterate text files and create Lucene documents
5. Commit index and write manifest

**Execution**:
```bash
bin/cli lucene-build                # Full index
bin/cli lucene-build --limit 1000   # Test with 1000 docs
```

---

## 3. Query Types Implementation

### 3.1 Query Type Summary

| Type | Description | Lucene Class | Example |
|------|-------------|--------------|---------|
| Simple | Full-text multi-field | MultiFieldQueryParser | `python web framework` |
| Boolean | AND/OR/NOT operators | BooleanQuery | `python AND docker` |
| Range | Numeric range filter | IntPoint.newRangeQuery | `star_count:[1000 TO *]` |
| Phrase | Exact phrase match | PhraseQuery | `"machine learning"` |
| Fuzzy | Typo-tolerant | FuzzyQuery | `pyhton` → matches `python` |
| Combined | Multiple filters | BooleanQuery + all above | text + topics + stars |

### 3.2 Simple Query

Multi-field full-text search across: title, content, topics, languages, wiki_title, wiki_abstract.

```bash
python -m lucene_indexer.search --query "python web framework" --type simple
```

**Implementation**:
- Uses `MultiFieldQueryParser` with default OR operator
- Field boosts: title (2.0), topics (1.5), wiki_title (1.5), content (1.0)

### 3.3 Boolean Query (AND/OR/NOT)

Explicit Boolean operators with proper precedence.

```bash
python -m lucene_indexer.search --query "python AND docker" --type boolean
python -m lucene_indexer.search --query "javascript OR typescript" --type boolean
python -m lucene_indexer.search --query "machine learning AND NOT tensorflow" --type boolean
```

**Implementation**:
- Uses `QueryParser` with default AND operator
- Supports: `AND`, `OR`, `NOT`, `+`, `-`

### 3.4 Range Query

Numeric range queries on integer fields.

```bash
python -m lucene_indexer.search --field star_count --min-value 1000 --type range
python -m lucene_indexer.search --field star_count --min-value 100 --max-value 1000 --type range
python -m lucene_indexer.search --field content_length --max-value 10000 --type range
```

**Implementation**:
- Uses `IntPoint.newRangeQuery` for efficient numeric filtering
- Supported fields: star_count, fork_count, content_length

### 3.5 Phrase Query

Exact phrase matching with optional slop (word distance).

```bash
python -m lucene_indexer.search --query "machine learning" --type phrase
python -m lucene_indexer.search --query "dependency injection" --type phrase --slop 1
```

**Implementation**:
- Uses `PhraseQuery.Builder` with configurable slop
- slop=0 means exact adjacent words
- slop=1 allows one word between

### 3.6 Fuzzy Query

Typo-tolerant matching using Levenshtein edit distance.

```bash
python -m lucene_indexer.search --query "pyhton" --type fuzzy
python -m lucene_indexer.search --query "javascrpt" --type fuzzy --max-edits 2
```

**Implementation**:
- Uses `FuzzyQuery` with max 2 edits (Lucene limit)
- Useful for handling spelling variations and typos

### 3.7 Combined Query

Multiple filters combined with Boolean logic.

```python
# API usage
results = searcher.search_combined(
    text_query="web framework",
    topics=["python"],
    min_stars=1000,
    has_wiki=True,
    top_k=10
)
```

**Implementation**:
- Combines text query + term filters + range filters
- All conditions are ANDed together

---

## 4. Old Index vs New Index Comparison

### 4.1 TF-IDF Index (Old)

**Location**: `indexer/` module

**Features**:
- Custom TF-IDF implementation
- Multiple IDF methods: classic, smoothed, probabilistic, max
- Simple text-only indexing
- JSONL-based postings with term index

**Limitations**:
- No entity fields (topics, languages, stars)
- No Wikipedia enrichment
- No range queries
- No phrase queries
- No fuzzy matching
- Simple tokenization only

### 4.2 PyLucene Index (New)

**Location**: `lucene_indexer/` module

**Features**:
- Full Lucene feature set (BM25 scoring by default)
- 16 fields including entities and Wikipedia data
- Boolean, Range, Phrase, Fuzzy query support
- Field boosting for relevance tuning
- Multi-valued fields
- DocValues for sorting
- Efficient term dictionary with FST encoding

### 4.3 Comparison Tool (`lucene_indexer/compare.py`)

```bash
python -m lucene_indexer.compare \
  --queries "python web,machine learning,docker" \
  --tfidf-index workspace/store/index \
  --lucene-index workspace/store/lucene_index \
  --output reports/index_comparison.md
```

**Metrics Computed**:
| Metric | Description |
|--------|-------------|
| Overlap@5 | % of shared results in top 5 |
| Overlap@10 | % of shared results in top 10 |
| Overlap@20 | % of shared results in top 20 |
| Latency | Query execution time (ms) |

### 4.4 Key Advantages of PyLucene Index

| Aspect | TF-IDF | PyLucene |
|--------|--------|----------|
| Query Types | Simple text only | Boolean, Range, Phrase, Fuzzy |
| Scoring | Custom TF-IDF | BM25 (superior default) |
| Entity Fields | None | topics, languages, license, stars |
| Wikipedia Data | None | wiki_title, wiki_categories, wiki_abstract |
| Numeric Range | Not supported | IntPoint/LongPoint queries |
| Phrase Match | Not supported | PhraseQuery with slop |
| Typo Tolerance | Not supported | FuzzyQuery (Levenshtein) |
| Sorting | By score only | By any numeric field |
| Multi-valued | Not supported | Native support |

---

## 5. Usage Examples

### Complete Pipeline

```bash
# 1. Crawl GitHub (if not done)
python -m crawler --config config.yml

# 2. Extract HTML → text + entities
bin/cli extract

# 3. Extract Wikipedia dump
bin/cli wiki --wiki-in /path/to/dump

# 4. Join entities with Wikipedia
bin/cli join

# 5. Build PyLucene index
bin/cli lucene-build

# 6. Search
bin/cli lucene-search "python web"
```

### Sample Queries

```bash
# Simple full-text search
bin/cli lucene-search "python web framework"

# Boolean query
bin/cli lucene-search --query "python AND docker" --type boolean

# Range query (popular repos)
bin/cli lucene-search --type range --field star_count --min-value 1000

# Phrase query
bin/cli lucene-search --query "neural network" --type phrase

# Fuzzy query (typo)
bin/cli lucene-search --query "pyhton" --type fuzzy

# Compare TF-IDF vs Lucene
bin/cli lucene-compare
```

---

## 6. Files Summary

| Component | File | Purpose |
|-----------|------|---------|
| HTML Extractor | `spark/jobs/html_extractor.py` | Extract text + entities from HTML |
| Wiki Extractor | `spark/jobs/wiki_extractor.py` | Extract structured data from Wikipedia |
| Wiki Regexes | `spark/lib/wiki_regexes.py` | MediaWiki parsing patterns |
| Entity-Wiki Join | `spark/jobs/join_html_wiki.py` | Join entities with Wikipedia |
| Index Schema | `lucene_indexer/schema.py` | Field definitions + justifications |
| Index Builder | `lucene_indexer/build.py` | Build Lucene index |
| Search Engine | `lucene_indexer/search.py` | Query execution (5 types) |
| Unified Search | `lucene_indexer/unified_search.py` | Config-driven TF-IDF/Lucene switch |
| Comparison | `lucene_indexer/compare.py` | Old vs new index comparison |

---

## 7. GitHub Repository

**URL**: https://github.com/andrejvysny/fiit-vinf

**Branch**: `konzultacia-3` (current development branch)

**Key Directories**:
```
fiit-vinf/
├── spark/
│   ├── jobs/
│   │   ├── html_extractor.py      # HTML → text + entities
│   │   ├── wiki_extractor.py      # Wikipedia dump extraction
│   │   └── join_html_wiki.py      # Entity-Wiki join
│   └── lib/
│       └── wiki_regexes.py        # Wikipedia parsing patterns
├── lucene_indexer/
│   ├── schema.py                  # Index schema + field justifications
│   ├── build.py                   # Index builder
│   ├── search.py                  # Search engine (5 query types)
│   ├── unified_search.py          # TF-IDF/Lucene switch
│   └── compare.py                 # Index comparison tool
├── indexer/                       # Old TF-IDF indexer
├── extractor/                     # Entity extraction patterns
└── docs/
    ├── KONZULTACIA_4.md           # Implementation summary
    └── KONZULTACIA_4_REPORT.md    # This report
```
