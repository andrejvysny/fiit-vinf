# Submission Summary: Distributed Data Processing with Apache Spark

## Spark Implementation

**Architecture**:
- Docker container: `apache/spark-py:latest`
- Execution mode: `local[*]` (uses all CPU cores)
- API: Spark DataFrame API
- Partitions: Configurable 64-1024 based on data size
- Entry points: `bin/spark_extract`, `bin/spark_wiki_extract`, `bin/spark_join_wiki`

**Performance Benchmarks**:

| Job | Dataset Size | Duration | Driver | Executor | Partitions | Speedup |
|-----|--------------|----------|--------|----------|------------|---------|
| HTML Extract | 500 files | ~10s | 2g | 1g | 32 | +13% |
| HTML Extract | 1,000 files | ~19s | 2g | 1g | 32 | +41% |
| HTML Extract | ~28K files (~10GB) | 5-10 min | 6-8g | 4g | 256 | - |
| Wiki Extract | 50-100 pages | ~20-25s | 4g | 2g | 8 | - |
| Wiki Extract | 1,000 pages | 3-5 min | 4g | 2g | 32 | - |
| Wiki Extract | Full dump (~7M) | 1.5-3h | 12-16g | 6-8g | 512-1024 | - |
| JOIN | 1,000 entities | 37s | 6g | 3g | 64 | - |
| JOIN | 400K entities | 15-30 min | 6g | 3g | 64 | - |

## HTML Data Extraction

**Job**: `spark/jobs/html_extractor.py`

**Process**:
- Discovers HTML files recursively from `workspace/store/html`
- Partitions files across Spark workers (default: 64 partitions)
- Each partition processes files using `extractor.html_clean.html_to_text()` and `extractor.entity_extractors.extract_all_entities()`

**Extracted Data**:
- **Text files**: Raw text extracted from HTML (written to `workspace/store/spark/text/`)
- **Entities TSV**: GitHub metadata entities (`workspace/store/spark/entities/entities.tsv`)
  - Entity types: `STAR_COUNT`, `FORK_COUNT`, `LANG_STATS`, `README`, `LICENSE`, `TOPICS`, `URL`, `EMAIL`
  - Format: `doc_id`, `type`, `value`, `offsets_json`
  - Full dataset: ~400K entity rows

## Wikipedia Dump Extraction

**Job**: `spark/jobs/wiki_extractor.py`

**Process**:
1. Streaming read of Wikipedia XML dump (supports `.xml` and `.xml.bz2`)
2. Parse `<page>` blocks without excessive buffering (OOM prevention)
3. Extract structured data using REGEX patterns from `spark/lib/wiki_regexes.py`
4. Auto-scale partitions: 64 → 256 → 1024 based on file size
5. Write to 7 TSV files + text directory

**Extracted Data** (`workspace/store/wiki/`):

| File | Rows (full) | Description |
|------|-------------|-------------|
| `pages.tsv` | ~7M | Page metadata (page_id, title, norm_title, namespace, redirects) |
| `aliases.tsv` | ~8M | Redirect mappings (aliases → canonical titles) |
| `categories.tsv` | ~20M | Page→category assignments |
| `abstract.tsv` | ~7M | First paragraph of each page |
| `infobox.tsv` | ~10M | Structured infobox fields (key-value pairs) |
| `links.tsv` | ~100M | Internal wiki links |
| `wiki_text_metadata.tsv` | ~7M | Mapping page_id → SHA256 → text file |
| `text/*.txt` | ~7M files | Full article text (deduplicated by SHA256) |

**Statistics** (full dump):
- Input size: 104GB XML (uncompressed)
- Total output: ~160M rows across all TSV files

**REGEX Patterns** (`spark/lib/wiki_regexes.py`):
- XML parsing: `PAGE_PATTERN`, `TITLE_PATTERN`, `ID_PATTERN`, `REDIRECT_PATTERN`
- Wikitext cleaning: `TEMPLATE_PATTERN`, `LINK_PATTERN`, `CATEGORY_PATTERN`, `REF_PATTERN`, `HTML_TAG_PATTERN`
- Title normalization: `normalize_title()` (lowercase + ASCII-fold + collapse punctuation)
- Infobox extraction: `INFOBOX_PATTERN`, `INFOBOX_FIELD_PATTERN`

## JOIN Implementation

### Batch JOIN (`spark/jobs/join_html_wiki.py`)

**Purpose**: Join all GitHub entities with Wikipedia canonical pages

**Input Data**:
- GitHub entities: `workspace/store/spark/entities/entities.tsv` (400K rows)
- Wikipedia pages: `workspace/store/wiki/pages.tsv` (7M pages)
- Wikipedia aliases: `workspace/store/wiki/aliases.tsv` (8M redirects)
- Wikipedia categories: `workspace/store/wiki/categories.tsv` (20M assignments)

**JOIN Process**:
1. **Build canonical mapping**: Union direct pages (7M) + aliases (8M) = 15M possible match keys
2. **Normalize titles**: Apply `normalize_title()` UDF (lowercase, ASCII-fold, collapse punctuation)
3. **Normalize entities**: Explode TOPICS (`"react,python,docker"` → 3 rows), apply normalization
   - Input: 400K rows → 1.2M entity rows (after TOPICS explosion)
4. **LEFT JOIN**: Join on `normalized_entity_value` ⟷ `normalized_wiki_title`
   - Preserves all entities (including unmatched)
5. **Confidence scoring**:
   - Base: 0.6
   - Direct match: +0.2
   - Alias match: +0.1
   - Exact case match: +0.1
   - Relevant category: +0.1
   - Range: [0.6, 1.0]

**Output** (`workspace/store/join/`):
- `html_wiki.tsv`: Complete join results (`doc_id`, `entity_type`, `entity_value`, `wiki_page_id`, `wiki_title`, `confidence`)
- `join_stats.json`: Aggregated statistics
- `html_wiki_agg.tsv`: Per-document aggregates

**Supported Entity Types**: `LANG_STATS`, `LICENSE`, `TOPICS`, `README`

### Streaming JOIN (`spark/jobs/join_html_wiki_topics.py`)

**Purpose**: High-quality TOPICS matching with relevance filtering

**Multi-hop JOIN**:
1. Entities → Aliases (LEFT): Resolve redirects
2. Canonical title → Pages (INNER): Match to Wikipedia
3. Pages → Categories (LEFT): Aggregate categories
4. Pages → Abstracts (LEFT): Add context
5. Relevance filter: Keep only if (relevant_category OR abstract_contains_entity)

**Relevance keywords**: `programming`, `software`, `computer`, `library`, `framework`, `license`

**Architecture**: Spark Structured Streaming with bounded memory (`maxFilesPerTrigger=16`)

**Output**: `workspace/store/wiki/join/html_wiki_topics_output/` (streaming CSV parts)

## JOIN Results

**Test Dataset**:
- GitHub entities: 1,000 → 1,220 (after TOPICS explosion)
- Wikipedia pages: 50 (namespace 0)
- Matches: 0 (disjoint datasets - GitHub tech topics vs general knowledge pages)

**Expected Results (Full Dataset)**:

| Entity Type | GitHub Entities | Expected Matches | Match Rate | Expected Unique Pages |
|-------------|-----------------|------------------|------------|----------------------|
| TOPICS | 200K (after explosion) | 80K-120K | 40-60% | 50K-80K |
| LANG_STATS | 100K | 60K-80K | 60-80% | 200-300 |
| LICENSE | 50K | 40K-45K | 80-90% | 20-30 |
| **Total** | 400K | 180K-245K | 45-60% | **50K-100K unique** |