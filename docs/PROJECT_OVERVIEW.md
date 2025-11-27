# Project Overview: GitHub-Wikipedia Data Pipeline

Multi-stage PySpark pipeline for extracting, enriching, and linking GitHub repository data with Wikipedia knowledge.

## Docker Setup

**Image**: `apache/spark-py:latest` (linux/amd64 for macOS ARM64 compatibility)

**Execution**: Local mode (`local[*]`) using all available CPU cores

**Key Configuration**:
- Driver Memory: 4-12g (auto-scaled by dataset size)
- Executor Memory: 2-6g
- Python dependencies installed via `requirements.txt`
- Workspace directories mounted as volumes

**Entry Points**: Wrapper scripts (`bin/spark_extract`, `bin/spark_wiki_extract`, `bin/spark_join_wiki`)

## HTML Extraction Pipeline

**Job**: `spark/jobs/html_extractor.py`

**Input**: HTML files from GitHub crawler (`workspace/store/html/`)

**Output**:
- Text files: `workspace/store/spark/text/`
- Entities TSV: `workspace/store/spark/entities/entities.tsv`

**Entity Types Extracted**:
- `TOPICS`: Repository topics/tags (comma-separated)
- `LANG_STATS`: Programming language statistics
- `LICENSE`: Software license names
- `STAR_COUNT`, `FORK_COUNT`: Repository metrics
- `URL`: Links extracted from HTML

**Process**:
- Discovers HTML files recursively
- Distributes across 64 partitions (default)
- Cleans HTML and extracts text via regex patterns
- Aggregates entities into single TSV file

**Performance** (28K files, ~10GB):
- Duration: 5-10 minutes
- Memory: 6-8g driver, 4g executor, 256 partitions
- Output: ~400K entities extracted

**Speedup vs Single-Process**:
- 500 files: +13% faster
- 1,000 files: +41% faster
- Overhead dominates for <100 files

## Wikipedia Dump Extraction Pipeline

**Job**: `spark/jobs/wiki_extractor.py`

**Input**: Wikipedia XML dump files (`wiki_dump/*.xml` or `.xml.bz2`)

**Outputs** (7 TSV files in `workspace/store/wiki/`):
- `pages.tsv`: Page metadata (page_id, title, norm_title, namespace, redirects)
- `aliases.tsv`: Redirect mappings (11 aliases in test, ~8M in full dump)
- `categories.tsv`: Page→category relationships (439 in test)
- `abstract.tsv`: First paragraph extracts
- `infobox.tsv`: Structured key-value pairs
- `links.tsv`: Internal wiki links
- `wiki_text_metadata.tsv`: Page→SHA256 mappings for full text
- `text/`: Directory with SHA256-named plaintext files

**Process**:
- Streaming XML parsing (no caching to prevent OOM)
- Auto-scales partitions: 64 for small files, 256-1024 for >50GB files
- Normalizes titles (lowercase, ASCII-fold, collapse punctuation)
- Deduplicates full text via SHA256 content addressing
- Cleans wikitext markup (templates, refs, HTML tags)

**Performance**:
- Test (100 pages): 20-25 seconds, 4g driver, 8 partitions
- Medium (1,000 pages): 3-5 minutes, 4g driver, 32 partitions
- Full dump (~7M pages, 104GB): 1.5-3 hours, 12-16g driver, 512-1024 partitions

**Output Statistics** (test dataset):
- 50 pages (namespace 0)
- 11 redirect aliases
- 439 category assignments

## Entity-Wikipedia JOIN Pipeline

### Batch JOIN

**Job**: `spark/jobs/join_html_wiki.py`

**Purpose**: Link all GitHub entities with Wikipedia canonical pages

**Process**:
- Builds canonical mapping (pages + aliases, ~39 entries in test, ~8M in full)
- Normalizes entity values using shared normalization function
- Explodes comma-separated TOPICS into individual rows
- LEFT JOIN on normalized keys (preserves unmatched entities)
- Calculates confidence scores [0.6-1.0] based on:
  - Match type: direct (+0.2) vs alias (+0.1)
  - Case exactness (+0.1)
  - Category relevance (+0.1)

**Outputs** (`workspace/store/join/`):
- `html_wiki.tsv`: Full join results
- `join_stats.json`: Aggregate match statistics
- `html_wiki_agg.tsv`: Per-document aggregates

**Performance** (1,000 entity rows):
- Duration: ~37 seconds
- Memory: 6g driver, 3g executor, 8 partitions
- Result: 1,220 entities after TOPICS explosion

**Test Results**:
- Total entities: 1,220 (after explosion)
- Matched: 0 (0%)
- Reason: Disjoint datasets (GitHub tech topics vs general Wikipedia sample)

### Streaming JOIN

**Job**: `spark/jobs/join_html_wiki_topics.py`

**Purpose**: High-quality TOPICS-to-Wikipedia join with relevance filtering

**Key Features**:
- Structured Streaming with bounded memory (`maxFilesPerTrigger=16`)
- Multi-hop join: entities → aliases → pages → categories + abstracts
- Relevance filtering: Matches only programming-related categories or abstracts
- Checkpoint-based resumability

**Relevance Keywords**: `programming`, `software`, `computer`, `library`, `framework`, `license`

**Confidence Levels**: `exact+cat`, `alias+cat`, `exact+abs`, `alias+abs`

**Outputs** (`workspace/store/wiki/join/`):
- `html_wiki_topics_output/`: Streaming results (CSV parts)
- `html_wiki_topics_stats.tsv`: Per-batch statistics

**Performance**:
- Small dataset: 2-5 minutes
- Memory: 4g driver, 2g executor, 128 partitions

## Expected Performance on Full Dataset

**HTML Extraction** (28K files):
- Duration: 5-10 minutes
- Entities: ~400K rows

**Wikipedia Extraction** (7M pages):
- Duration: 1.5-3 hours
- Pages: 7M canonical + 8M aliases
- Categories: ~20M assignments

**JOIN** (400K entities × 7M Wikipedia pages):
- Duration: 15-30 minutes (batch) or 5-10 minutes (streaming)
- Expected match rate: 40-60% for TOPICS entities
- Unique Wikipedia pages linked: ~50K-100K

## Key Statistics

| Metric | HTML Extract | Wiki Extract | JOIN |
|--------|-------------|--------------|------|
| **Input Size** | 10GB (28K files) | 104GB XML | 400K entities |
| **Output Size** | 400K entities | 7M pages + 8M aliases | ~200K matches |
| **Duration** | 5-10 min | 1.5-3 hours | 15-30 min |
| **Memory** | 6-8g driver | 12-16g driver | 6g driver |
| **Partitions** | 256 | 512-1024 | 64 |
| **Speedup** | 1.4x @ 1K files | N/A (no baseline) | N/A |
