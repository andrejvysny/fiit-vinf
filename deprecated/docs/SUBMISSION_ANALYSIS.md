# VINF Project: Second Submission Analysis Report

**Generated**: 2025-12-11
**Analyzed by**: Claude Code
**Repository**: https://github.com/andrejvysny/fiit-vinf

---

## Executive Summary

| Category | Status | Completion |
|----------|--------|------------|
| Konzultácia 3 (Spark + Wiki) | ✅ Complete | 100% |
| Konzultácia 4 (PyLucene) | ✅ Complete | 100% |
| Konzultácia 5 (Final + Evaluation) | ⚠️ Partial | ~70% |
| Second Submission Document | ⚠️ Partial | ~80% |

**Critical Missing Item**: Comparison with Google/GitHub search (required for Konzultácia 5 and final submission)

---

# Part 1: Konzultácia 3 - Distributed Processing (Apache Spark)

## Requirements Checklist

| Requirement | Status | Evidence |
|-------------|--------|----------|
| Extend code for distributed processing | ✅ | `spark/jobs/*.py` |
| Extract data using Spark | ✅ | HTML + Wiki extraction |
| Process Wiki dump with Spark | ✅ | `wiki_extractor.py` |
| Work with Wiki data (REGEX) | ✅ | `spark/lib/wiki_regexes.py` |
| Justify and show JOIN | ✅ | `join_html_wiki.py` + docs |
| Count unique wiki pages joined | ✅ | 51 unique pages |
| List interesting wiki attributes | ✅ | 6 TSV files |

## 1.1 Spark Jobs Implementation

### HTML Extractor (`spark/jobs/html_extractor.py`)

**Architecture**:
- DataFrame API with `mapInPandas` for partition-level processing
- Single-pass: text + entities extracted simultaneously
- Kryo serialization + Arrow for efficient data transfer
- DISK_ONLY persistence to avoid OOM

**Entity Types Extracted**:
| Type | Description | Example |
|------|-------------|---------|
| `STAR_COUNT` | Repository stars | 15000 |
| `FORK_COUNT` | Repository forks | 3200 |
| `LANG_STATS` | Programming languages | Python, JavaScript |
| `LICENSE` | Software license | MIT, Apache-2.0 |
| `TOPICS` | GitHub topics | python, machine-learning |
| `URL` | Repository URLs | https://github.com/... |
| `README` | README markers | - |
| `EMAIL` | Email addresses | - |

**Output**:
- `workspace/store/text/{doc_id}.txt` - Clean text
- `workspace/store/spark/entities/entities.tsv` - Entity annotations

**CLI**:
```bash
bin/cli extract                    # Full extraction
bin/cli extract --sample 100       # Test mode
bin/cli extract --force            # Overwrite
```

### Wikipedia Extractor (`spark/jobs/wiki_extractor.py`)

**Architecture**:
- Dual-mode: Native Python streaming (≤50K pages) or Spark DataFrame (larger)
- Streaming XML parsing - never loads full dump into memory
- NO `.cache()` calls - critical constraint
- SHA256 deduplication for full text

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
| `REDIRECT_PATTERN` | Extract redirect targets |

**Output Files (7 TSV + text)**:
| File | Columns | Purpose |
|------|---------|---------|
| `pages.tsv` | page_id, title, norm_title, ns, redirect_to, timestamp | Page metadata |
| `categories.tsv` | page_id, category, norm_category | Category assignments |
| `links.tsv` | page_id, link_title, norm_link_title | Internal wiki links |
| `infobox.tsv` | page_id, key, value | Structured infobox data |
| `abstract.tsv` | page_id, abstract_text | Lead section text |
| `aliases.tsv` | alias_norm_title, canonical_norm_title | Redirect mappings |
| `wiki_text_metadata.tsv` | page_id, title, content_sha256, ... | Text file metadata |
| `text/*.txt` | Full article text | SHA256-named files |

**CLI**:
```bash
bin/cli wiki                       # Full extraction
bin/cli wiki --wiki-max-pages 1000 # Test mode
bin/cli wiki --no-text             # Skip full text
```

### Entity-Wiki Join (`spark/jobs/join_html_wiki.py`)

**Join Logic**:
1. Load entities from HTML extraction
2. Load Wikipedia pages, aliases, categories, abstracts
3. Build canonical mapping (direct pages + redirects via aliases)
4. Normalize entity values using same function as wiki titles
5. LEFT JOIN entities with canonical mapping on `norm_value = norm_key`
6. Attach categories and abstracts
7. Calculate confidence labels

**Normalization Function**:
```python
normalize_title(text):
    - lowercase
    - ASCII-fold (remove accents: é → e)
    - collapse punctuation and spaces
    - remove parenthetical suffixes: "Python (programming language)" → "python"
```

**Confidence Labels**:
| Label | Meaning |
|-------|---------|
| `exact+cat` | Direct title match + relevant category |
| `alias+cat` | Redirect match + relevant category |
| `exact+abs` | Direct title match + abstract available |
| `alias+abs` | Alias match + abstract available |
| `exact` | Direct match only |
| `alias` | Alias match only |

**Output Files**:
| File | Columns |
|------|---------|
| `html_wiki.tsv` | doc_id, entity_type, entity_value, norm_value, wiki_page_id, wiki_title, wiki_abstract, wiki_categories, join_key, confidence |
| `html_wiki_agg.tsv` | doc_id, joined_page_ids, num_joined_pages, num_topics_joined, num_licenses_joined, num_langs_joined |
| `join_stats.json` | Statistics including unique_wiki_pages_joined, match_rate |

**CLI**:
```bash
bin/cli join                           # Full join
bin/cli join --entities-max-rows 1000  # Test mode
```

## 1.2 Actual Statistics (from pipeline run)

```json
{
  "wiki_extraction": {
    "pages_processed": 9913,
    "categories": 66318,
    "links": 1780735,
    "infobox_fields": 53440,
    "abstracts": 6308,
    "aliases": 2446,
    "text_files": 7135,
    "duration_seconds": 46.05
  },
  "html_extraction": {
    "files_processed": 30,
    "total_entities": 10130,
    "entity_types": ["URL", "LICENSE", "STAR_COUNT", "TOPICS", "FORK_COUNT", "README"]
  },
  "join": {
    "total_entities": 12454,
    "matched_entities": 129,
    "match_rate": 1.04,
    "unique_wiki_pages_joined": 51,
    "unique_docs_with_wiki": 30
  }
}
```

**Join Statistics by Entity Type**:
| Entity Type | Count | Matched | Unique Pages | Match Rate |
|-------------|-------|---------|--------------|------------|
| TOPICS | 2,338 | 97 | 48 | 4.15% |
| URL | 9,950 | 32 | 3 | 0.32% |
| LICENSE | 118 | 0 | 0 | 0.0% |
| STAR_COUNT | 27 | 0 | 0 | 0.0% |
| FORK_COUNT | 13 | 0 | 0 | 0.0% |
| README | 8 | 0 | 0 | 0.0% |

---

# Part 2: Konzultácia 4 - PyLucene Index

## Requirements Checklist

| Requirement | Status | Evidence |
|-------------|--------|----------|
| Finished Spark code | ✅ | `spark/jobs/*.py` |
| PyLucene index design | ✅ | `lucene_indexer/schema.py` |
| Field justifications | ✅ | `docs/KONZULTACIA_4_REPORT.md` |
| Index implementation | ✅ | `lucene_indexer/build.py` |
| Old vs new comparison | ✅ | `lucene_indexer/compare.py` |
| Query types documented | ✅ | 5 types implemented |
| GitHub link | ✅ | https://github.com/andrejvysny/fiit-vinf |

## 2.1 Index Schema (`lucene_indexer/schema.py`)

**17 Fields Total**:

### Core Document Fields
| Field | Type | Stored | Indexed | Tokenized | Justification |
|-------|------|--------|---------|-----------|---------------|
| `doc_id` | StringField | Yes | Yes | No | SHA256 hash - opaque identifier for exact lookup |
| `title` | TextField | Yes | Yes | Yes | High weight in relevance - titles are descriptive |
| `content` | TextField | No | Yes | Yes | Primary field for ranking, NOT stored (retrieve from files) |
| `path` | StoredField | Yes | No | No | Filesystem path to source document |
| `url` | StoredField | Yes | No | No | GitHub repository URL |

### GitHub Entity Fields
| Field | Type | Stored | Indexed | Tokenized | Multi-valued | Justification |
|-------|------|--------|---------|-----------|--------------|---------------|
| `topics` | TextField | Yes | Yes | Yes | Yes | GitHub topics for filtering, boost 1.5x |
| `languages` | TextField | Yes | Yes | Yes | Yes | Programming languages, boost 1.2x |
| `license` | StringField | Yes | Yes | No | No | Exact license matching (MIT, Apache-2.0) |
| `star_count` | IntPoint+DocValues | Yes | Yes | No | No | Range queries + sorting by popularity |
| `fork_count` | IntPoint | Yes | Yes | No | No | Range queries on fork count |

### Wikipedia Enrichment Fields
| Field | Type | Stored | Indexed | Tokenized | Multi-valued | Justification |
|-------|------|--------|---------|-----------|--------------|---------------|
| `wiki_page_id` | LongPoint | Yes | Yes | No | Yes | Filter wiki-enriched documents |
| `wiki_title` | TextField | Yes | Yes | Yes | Yes | Search by Wikipedia concepts, boost 1.5x |
| `wiki_categories` | TextField | Yes | Yes | Yes | Yes | Semantic enrichment from Wikipedia |
| `wiki_abstract` | TextField | Yes | Yes | Yes | No | Rich context for disambiguation, boost 0.8x |
| `join_confidence` | StringField | Yes | Yes | No | Yes | Filter by join quality |

### Metadata Fields
| Field | Type | Stored | Indexed | Tokenized | Justification |
|-------|------|--------|---------|-----------|---------------|
| `indexed_at` | LongPoint | Yes | Yes | No | Date range queries for freshness |
| `content_length` | IntPoint | Yes | Yes | No | Filter by document size |

## 2.2 Query Types Implementation (`lucene_indexer/search.py`)

### 5 Query Types

| Type | Class | Description | Example |
|------|-------|-------------|---------|
| **Simple** | MultiFieldQueryParser | Full-text across multiple fields | `python web framework` |
| **Boolean** | QueryParser | AND/OR/NOT operators | `python AND docker` |
| **Range** | IntPoint.newRangeQuery | Numeric range filtering | `star_count:[1000 TO *]` |
| **Phrase** | PhraseQuery | Exact phrase with slop | `"machine learning"` |
| **Fuzzy** | FuzzyQuery | Typo-tolerant (Levenshtein) | `pyhton` → matches `python` |

**CLI Examples**:
```bash
# Simple search
bin/cli lucene-search "python web framework"

# Boolean search
bin/cli lucene-search --query "python AND docker" --type boolean
bin/cli lucene-search --query "web OR api" --type boolean
bin/cli lucene-search --query "python AND NOT java" --type boolean

# Range search
bin/cli lucene-search --type range --field star_count --min-value 1000
bin/cli lucene-search --type range --field fork_count --min-value 100 --max-value 500

# Phrase search
bin/cli lucene-search --query "machine learning" --type phrase
bin/cli lucene-search --query "dependency injection" --type phrase --slop 1

# Fuzzy search
bin/cli lucene-search --query "pyhton" --type fuzzy
bin/cli lucene-search --query "javascrpt" --type fuzzy --max-edits 2
```

**Field Boosts**:
- title: 2.0x
- topics: 1.5x
- wiki_title: 1.5x
- languages: 1.2x
- content: 1.0x (baseline)
- wiki_abstract: 0.8x

## 2.3 Old vs New Index Comparison

### Comparison Tool (`lucene_indexer/compare.py`)

**Metrics Computed**:
- Overlap@5, @10, @20 (Jaccard similarity of top-k results)
- Latency (ms)
- Query type support

**CLI**:
```bash
bin/cli lucene-compare
python -m lucene_indexer.compare --queries "python web,machine learning" --output reports/index_comparison.md
```

### Comparison Results (from `reports/index_comparison.md`)

**14 Queries Compared**:

| Query | Type | TF-IDF Results | Lucene Results | Overlap@20 |
|-------|------|----------------|----------------|------------|
| python web | simple | 20 | 20 | 5.0% |
| machine learning | simple | 20 | 20 | 10.0% |
| docker container | simple | 20 | 20 | 40.0% |
| javascript framework | simple | 20 | 20 | 10.0% |
| python AND docker | boolean | 20 | 20 | 45.0% |
| web OR api | boolean | 20 | 20 | 5.0% |
| machine learning | phrase | 20 | 20 | 30.0% |
| pyhton | fuzzy | 20 | 20 | 0.0% |
| javascrpt | fuzzy | 1 | 20 | 0.0% |

**Overall Statistics**:
| Metric | TF-IDF | Lucene |
|--------|--------|--------|
| Avg Latency (ms) | 26.28 | 37.43 |
| Avg Overlap@5 | - | 1.4% |
| Avg Overlap@10 | - | 5.7% |
| Avg Overlap@20 | - | 14.6% |

### Feature Comparison

| Feature | TF-IDF (Old) | PyLucene (New) |
|---------|--------------|----------------|
| Query Types | Simple text only | Boolean, Range, Phrase, Fuzzy |
| Scoring | Custom TF-IDF | BM25 (superior) |
| Entity Fields | None | topics, languages, license, stars, forks |
| Wikipedia Data | None | wiki_title, wiki_categories, wiki_abstract |
| Numeric Range | Not supported | IntPoint queries |
| Phrase Match | Not supported | PhraseQuery with slop |
| Typo Tolerance | Not supported | FuzzyQuery |
| Sorting | By score only | By any numeric field |
| Multi-valued | Not supported | Native support |

---

# Part 3: Konzultácia 5 - Final Software + Evaluation

## Requirements Checklist

| Requirement | Status | Notes |
|-------------|--------|-------|
| Finished software on GitHub | ✅ | Full pipeline works |
| Present final solution | ✅ | CLI + documentation |
| Evaluate search results | ⚠️ | Only TF-IDF vs Lucene |
| **Compare with Google** | ❌ MISSING | Not implemented |
| **Compare with domain search** | ❌ MISSING | GitHub search not compared |
| Results in text form | ⚠️ | Only index comparison |

## 3.1 What's Implemented

### Full Pipeline
```bash
bin/cli pipeline                              # Run everything
bin/cli pipeline --sample 100 --wiki-max-pages 1000  # Test mode
```

### Individual Stages
```bash
bin/cli extract      # HTML → text + entities
bin/cli wiki         # Wikipedia dump → TSV
bin/cli join         # Entity-Wiki join
bin/cli lucene-build # Build PyLucene index
bin/cli lucene-search "query"  # Search
bin/cli lucene-compare         # TF-IDF vs Lucene comparison
bin/cli stats        # Pipeline statistics
```

### Documentation
- `README.md` - Quick start guide
- `CLAUDE.md` - Developer reference
- `docs/KONZULTACIA_4_REPORT.md` - Konzultácia 4 detailed report
- `docs/FinalSubmission.txt` - First submission content
- `reports/index_comparison.md` - TF-IDF vs Lucene comparison

## 3.2 What's MISSING

### ❌ Google/Domain Comparison

**Required**: Compare your search results with:
1. Google search results
2. GitHub's own search (the crawled domain)

**Format required**: Text table (not screenshots) with at least 5 queries

**Example format needed**:
```
| Query | Rank | Your Result | Google Result | GitHub Result |
|-------|------|-------------|---------------|---------------|
| python web framework | 1 | repo-A | result-X | result-Y |
| ... | 2 | repo-B | result-Z | result-W |
```

---

# Part 4: Second Submission Document Requirements

## Requirements vs Implementation Status

| Section | Status | Location |
|---------|--------|----------|
| **Spark Architecture** | | |
| - Transition description | ✅ | `docs/SPARK_MIGRATION.md` |
| **Wikipedia Processing** | | |
| - Extraction description | ✅ | `docs/WIKI_EXTRACTION.md`, code |
| - Cleaning description | ✅ | `spark/lib/wiki_regexes.py` |
| **Data Integration (Join)** | | |
| - Join logic justification | ✅ | `docs/JOIN.md`, `docs/KONZULTACIA_4_REPORT.md` |
| - Unique wiki pages count | ✅ | 51 pages (`stats/join.json`) |
| - Wiki attributes list | ✅ | 6 attributes in TSV files |
| **PyLucene Indexing** | | |
| - Schema design | ✅ | `lucene_indexer/schema.py` |
| - Field justifications | ✅ | `docs/KONZULTACIA_4_REPORT.md` |
| - Query types | ✅ | 5 types documented |
| **Evaluation & Comparison** | | |
| - Old vs New index | ✅ | `reports/index_comparison.md` |
| - **Google comparison** | ❌ MISSING | Not implemented |
| - **Domain comparison** | ❌ MISSING | Not implemented |
| - 5+ queries in table | ❌ MISSING | Need text tables |
| **Submission Materials** | | |
| - GitHub link | ✅ | https://github.com/andrejvysny/fiit-vinf |
| - ZIP with code | ⚠️ TODO | Need to create |

---

# Part 5: Test Coverage

## Implemented Tests

| Test File | Coverage |
|-----------|----------|
| `tests/test_regexes.py` | HTML entity extraction (stars, forks, licenses, topics, URLs, etc.) |
| `tests/test_wiki_regexes.py` | Wikipedia parsing (pages, categories, links, infobox, abstract) |
| `tests/test_link_extractor.py` | URL extraction from HTML |
| `tests/test_spark_extractor.py` | Spark job testing |

### test_regexes.py Coverage

- `TestHTMLCleaningRegexes` - HTML comment, doctype, self-closing tags, script/style removal
- `TestGitHubMetadataRegexes` - Star counter, fork counter, language extraction
- `TestLicenseRegexes` - SPDX license detection
- `TestTopicRegex` - GitHub topic extraction
- `TestImportRegexes` - Python, JavaScript, Rust, C imports
- `TestURLRegexes` - HTTP URLs, markdown links, HTML hrefs
- `TestVersionRegex` - Semantic version extraction
- `TestEmailRegex` - Email extraction
- `TestCodeFenceRegex` - Code fence language detection
- `TestIssueReferenceRegexes` - GitHub issue references

### test_wiki_regexes.py Coverage

- `TestWikiRegexes` - Page XML extraction, redirect handling, title normalization
- `TestWikiJoinNormalization` - Entity-Wiki normalization consistency

**Run tests**:
```bash
python -m unittest discover tests
python -m unittest tests.test_regexes
python -m unittest tests.test_wiki_regexes
```

---

# Part 6: File Structure Summary

```
vinf/
├── bin/cli                          # Unified CLI entry point
├── crawler/                         # GitHub HTML crawler
│   ├── service.py                   # Main crawler service
│   ├── crawl_policy.py              # Robots.txt policy
│   └── extractor.py                 # Link extraction
├── extractor/                       # HTML entity extraction
│   ├── entity_extractors.py         # Entity extraction logic
│   ├── regexes.py                   # Regex patterns
│   └── html_clean.py                # HTML cleaning
├── indexer/                         # TF-IDF indexer (old)
│   ├── build.py                     # Index builder
│   ├── search.py                    # Search engine
│   ├── idf.py                       # IDF methods
│   └── compare.py                   # IDF comparison
├── lucene_indexer/                  # PyLucene indexer (new)
│   ├── schema.py                    # Index schema
│   ├── build.py                     # Index builder
│   ├── search.py                    # Search engine (5 query types)
│   ├── compare.py                   # TF-IDF vs Lucene comparison
│   └── unified_search.py            # Config-driven engine switch
├── spark/
│   ├── jobs/
│   │   ├── html_extractor.py        # HTML → text + entities
│   │   ├── wiki_extractor.py        # Wikipedia dump extraction
│   │   ├── join_html_wiki.py        # Entity-Wiki batch join
│   │   └── join_html_wiki_topics.py # Streaming topics join
│   └── lib/
│       ├── wiki_regexes.py          # Wikipedia parsing patterns
│       └── stats.py                 # Pipeline statistics
├── tests/
│   ├── test_regexes.py              # HTML regex tests
│   ├── test_wiki_regexes.py         # Wiki regex tests
│   ├── test_link_extractor.py       # URL extraction tests
│   └── test_spark_extractor.py      # Spark job tests
├── docs/
│   ├── KONZULTACIA_4_REPORT.md      # Konzultácia 4 report
│   ├── FinalSubmission.txt          # First submission
│   ├── SPARK_MIGRATION.md           # Spark transition docs
│   └── ...
├── reports/
│   ├── index_comparison.md          # TF-IDF vs Lucene
│   ├── index_comparison.json        # Comparison data
│   └── idf_comparison.md            # IDF methods comparison
├── stats/
│   ├── pipeline_summary.json        # Aggregated stats
│   ├── join.json                    # Join statistics
│   ├── wiki_extraction.json         # Wiki extraction stats
│   └── lucene_index.json            # Index build stats
├── workspace/store/
│   ├── html/                        # Crawled HTML files
│   ├── text/                        # Extracted text
│   ├── spark/entities/              # Entity TSV
│   ├── wiki/                        # Wikipedia TSV files
│   ├── join/                        # Join results
│   ├── index/                       # TF-IDF index
│   └── lucene_index/                # PyLucene index
├── config.yml                       # Configuration
├── docker-compose.yml               # Docker orchestration
├── README.md                        # Quick start
└── CLAUDE.md                        # Developer reference
```

---

# Part 7: Next Steps and TODOs

## Critical (Required for Submission)

### TODO 1: Implement Google/GitHub Comparison

**Priority**: 🔴 HIGH - Required for Konzultácia 5 and final submission

**Tasks**:
1. Select 5+ representative queries (see below)
2. Run queries on:
   - Your Lucene index: `bin/cli lucene-search "query"`
   - Google: `site:github.com query` or general search
   - GitHub search: https://github.com/search?q=query
3. Record top 5 results from each
4. Create text table comparison (NOT screenshots)
5. Write brief analysis for each query

---

## Example Queries for Comparison

### Query Set 1: Simple Text Queries

These test basic relevance ranking:

| # | Query | Why This Query |
|---|-------|----------------|
| 1 | `python web framework` | Common search, tests language + domain matching |
| 2 | `machine learning library` | Popular topic, many relevant repos |
| 3 | `docker container` | Infrastructure topic |
| 4 | `javascript testing` | Frontend testing tools |
| 5 | `react components` | UI library ecosystem |

### Query Set 2: Specific Technology Queries

These test entity matching (topics, languages):

| # | Query | Why This Query |
|---|-------|----------------|
| 6 | `flask` | Specific framework name |
| 7 | `tensorflow tutorial` | ML framework + learning |
| 8 | `kubernetes deployment` | Container orchestration |
| 9 | `node.js api` | Runtime + use case |
| 10 | `rust programming` | Language-specific |

### Query Set 3: Use Case Queries

These test semantic understanding:

| # | Query | Why This Query |
|---|-------|----------------|
| 11 | `REST API authentication` | Specific use case |
| 12 | `database migration tool` | DevOps tooling |
| 13 | `image processing library` | Domain-specific |
| 14 | `web scraping python` | Task + language |
| 15 | `open source license MIT` | License filtering |

---

## Recommended 5 Queries for Submission

Pick these 5 for a balanced comparison:

```
1. python web framework        (general, high volume)
2. machine learning            (popular topic)
3. docker kubernetes           (infrastructure)
4. react typescript            (frontend stack)
5. REST API                    (use case)
```

---

## How to Run Comparison

### Step 1: Run on Your Index
```bash
# For each query
bin/cli lucene-search "python web framework" --top 5
bin/cli lucene-search "machine learning" --top 5
bin/cli lucene-search "docker kubernetes" --top 5
bin/cli lucene-search "react typescript" --top 5
bin/cli lucene-search "REST API" --top 5
```

### Step 2: Run on Google
Search: `site:github.com python web framework`
Record top 5 repository results.

### Step 3: Run on GitHub
Go to: https://github.com/search?q=python+web+framework&type=repositories
Record top 5 results.

---

## Output Template (Copy & Fill)

```markdown
# Search Comparison: Your Index vs Google vs GitHub

## Query 1: "python web framework"

| Rank | Our Result (Lucene) | Google (site:github.com) | GitHub Search |
|------|---------------------|--------------------------|---------------|
| 1 | owner/repo-name | owner/repo-name | owner/repo-name |
| 2 | owner/repo-name | owner/repo-name | owner/repo-name |
| 3 | owner/repo-name | owner/repo-name | owner/repo-name |
| 4 | owner/repo-name | owner/repo-name | owner/repo-name |
| 5 | owner/repo-name | owner/repo-name | owner/repo-name |

**Analysis**:
- Overlap: X/5 results appear in all three
- Our index ranks Y higher because of Wikipedia enrichment
- Google favors Z due to PageRank/popularity signals

---

## Query additional:

site:github.com python webscraping framework

1. https://github.com/scrapy/scrapy
2. https://github.com/topics/web-scraping-python
3. https://github.com/lorien/grab
4. https://github.com/luminati-io/Python-scraping-libraries
5. https://github.com/luminati-io/Python-web-scraping


Direct Github.com

1.
2.
3.
4.
5.

## Query 2: "machine learning"

| Rank | Our Result (Lucene) | Google (site:github.com) | GitHub Search |
|------|---------------------|--------------------------|---------------|
| 1 | |https://github.com/topics/machine-learning | |
| 2 | |https://github.com/mikeroyal/Machine-Learning-Guide | |
| 3 | |https://github.com/microsoft/ML-For-Beginners | |
| 4 | |https://github.com/josephmisiti/awesome-machine-learning | |
| 5 | |https://github.com/harvard-edge/cs249r_book | |

**Analysis**:
- ...

---

## Query 3: "docker kubernetes"

| Rank | Our Result (Lucene) | Google (site:github.com) | GitHub Search |
|------|---------------------|--------------------------|---------------|
| 1 | | | |
| 2 | | | |
| 3 | | | |
| 4 | | | |
| 5 | | | |

**Analysis**:
- ...

---

## Query 4: "react typescript"

| Rank | Our Result (Lucene) | Google (site:github.com) | GitHub Search |
|------|---------------------|--------------------------|---------------|
| 1 | | https://github.com/typescript-cheatsheets/react| |
| 2 | | https://github.com/topics/react-typescript| |
| 3 | | https://github.com/academind/react-typescript-course-resources| |
| 4 | | https://github.com/total-typescript/react-typescript-tutorial| |
| 5 | | https://github.com/piotrwitek/react-redux-typescript-guide| |

**Analysis**:
- ...

---

## Query 5: "REST API"

| Rank | Our Result (Lucene) | Google (site:github.com) | GitHub Search |
|------|---------------------|--------------------------|---------------|
| 1 | | | |
| 2 | | | |
| 3 | | | |
| 4 | | | |
| 5 | | | |

**Analysis**:
- ...

---

## Overall Summary

| Metric | Our Index | Google | GitHub |
|--------|-----------|--------|--------|
| Avg result overlap with ours | 100% | X% | Y% |
| Strengths | Wikipedia enrichment, entity fields | PageRank, freshness | Stars, activity |
| Weaknesses | Limited crawl scope | Not domain-specific | No Wikipedia context |

**Key Findings**:
1. Our index excels at... because...
2. Google provides better results for... because...
3. GitHub search ranks by... which differs from our approach...

**Conclusion**:
Our search system provides [comparable/different] results to Google/GitHub
because [reasons related to BM25 scoring, Wikipedia enrichment, entity fields].
```

---

## Analysis Points to Consider

When writing analysis, address:

1. **Result Overlap**
   - How many results appear in all three?
   - Which unique results does your index find?

2. **Ranking Differences**
   - Why might Google rank differently? (PageRank, freshness, user signals)
   - Why might GitHub rank differently? (stars, recent activity, trending)
   - Why does your index rank the way it does? (BM25, field boosts, Wikipedia)

3. **Wikipedia Enrichment Impact**
   - Do wiki-enriched documents rank higher?
   - Does category matching improve relevance?

4. **Entity Field Impact**
   - Do topic/language filters improve results?
   - Does star_count correlation exist?

5. **Missing Results**
   - What relevant repos does your index miss?
   - Why? (not crawled, different URL structure, etc.)

### TODO 2: Create Final Submission Report

**Priority**: 🔴 HIGH

Consolidate into single document:
1. Spark architecture transition (from `docs/SPARK_MIGRATION.md`)
2. Wikipedia processing description
3. Join logic + statistics (from `stats/join.json`)
4. PyLucene schema + justifications (from `docs/KONZULTACIA_4_REPORT.md`)
5. Query types documentation
6. Old vs New comparison (from `reports/index_comparison.md`)
7. **NEW**: Google/GitHub comparison

### TODO 3: Create ZIP Archive

**Priority**: 🟡 MEDIUM

```bash
# Create submission ZIP
zip -r vinf_submission.zip . \
  -x "*.git*" \
  -x "workspace/store/*" \
  -x "venv/*" \
  -x "__pycache__/*" \
  -x "*.pyc"
```

## Recommended Improvements

### TODO 4: Improve Join Match Rate

**Priority**: 🟡 MEDIUM

Current match rate: 1.04% (129/12,454 entities)

**Possible improvements**:
- Add more alias mappings
- Improve normalization (handle more edge cases)
- Add fuzzy matching for entity-wiki join
- Process larger Wikipedia dump

### TODO 5: Add More Query Examples

**Priority**: 🟢 LOW

Document more complex query examples:
- Combined queries (text + filters)
- Multi-field Boolean queries
- Range + text combinations

### TODO 6: Performance Benchmarks

**Priority**: 🟢 LOW

Add benchmarks for:
- Index build time
- Query latency (p50, p95, p99)
- Memory usage

---

# Appendix A: CLI Command Reference

## Full Pipeline
```bash
bin/cli pipeline                                    # Full run
bin/cli pipeline --sample 100 --wiki-max-pages 1000 # Test
bin/cli pipeline --skip-extract --skip-wiki         # Only join + index
```

## Individual Stages
```bash
# HTML Extraction
bin/cli extract
bin/cli extract --sample 100
bin/cli extract --force
bin/cli extract --dry-run

# Wikipedia Extraction
bin/cli wiki
bin/cli wiki --wiki-max-pages 1000
bin/cli wiki --no-text

# Join
bin/cli join
bin/cli join --entities-max-rows 1000

# Lucene Index
bin/cli lucene-build
bin/cli lucene-build --limit 1000

# Search
bin/cli lucene-search "python web"
bin/cli lucene-search --query "python AND docker" --type boolean
bin/cli lucene-search --type range --field star_count --min-value 1000
bin/cli lucene-search --query "machine learning" --type phrase
bin/cli lucene-search --query "pyhton" --type fuzzy

# Comparison
bin/cli lucene-compare

# Statistics
bin/cli stats
bin/cli stats --json
bin/cli stats --markdown
```

## Docker Compose
```bash
docker compose run --rm spark-extract
docker compose run --rm spark-wiki
docker compose run --rm spark-join
docker compose run --rm lucene-build
docker compose run --rm lucene-search
```

---

# Appendix B: Statistics Summary

## Pipeline Summary (from stats/pipeline_summary.json)

| Stage | Status | Duration | Key Metrics |
|-------|--------|----------|-------------|
| Wiki Extraction | ✅ | 46.05s | 9,913 pages, 66,318 categories |
| HTML Extraction | ✅ | 0.04s | 30 files, 10,130 entities |
| Join | ✅ | ~0s | 129 matched, 51 wiki pages |
| Lucene Index | ✅ | ~0s | 28,353 docs indexed |
| Comparison | ✅ | 3.09s | 14 queries compared |

## Join Statistics (from stats/join.json)

| Entity Type | Count | Matched | Match Rate |
|-------------|-------|---------|------------|
| TOPICS | 2,338 | 97 | 4.15% |
| URL | 9,950 | 32 | 0.32% |
| LICENSE | 118 | 0 | 0.0% |
| STAR_COUNT | 27 | 0 | 0.0% |
| FORK_COUNT | 13 | 0 | 0.0% |
| README | 8 | 0 | 0.0% |
| **Total** | **12,454** | **129** | **1.04%** |

---

# Appendix C: Unresolved Questions

1. **Google comparison queries**: Which 5+ queries should be used for the comparison?
2. **Comparison metrics**: Should we include precision/relevance metrics or just result lists?
3. **Match rate**: Is 1.04% join match rate acceptable? (51 wiki pages from ~10K entities)
4. **Full Wikipedia dump**: Should we process full dump for better coverage?
5. **License matching**: Why are LICENSE entities not matching Wikipedia? (normalization issue?)
