# Run Guide: Konzultácia 4 - Full Pipeline Execution

This guide covers how to run the complete extraction, indexing, and search pipeline for Konzultácia 4.

## Prerequisites

- Docker and Docker Compose installed
- At least 16GB RAM for full Wikipedia extraction
- ~200GB disk space for full Wikipedia dump processing

## Quick Start

```bash
# 1. Pull required Docker images
docker compose pull

# 2. Test PyLucene availability
docker compose run --rm lucene

# 3. Build Lucene index (with existing data)
docker compose run --rm lucene-build

# 4. Search the index
QUERY="python web framework" docker compose run --rm lucene-search
```

---

## Complete Pipeline Steps

### Step 1: HTML Extraction (Spark)

Extract entities from crawled GitHub HTML pages.

```bash
# Test with small sample
SPARK_ARGS="--sample 100 --force" docker compose run --rm spark-extract

# Full extraction (requires crawled HTML in workspace/store/html/)
docker compose run --rm spark-extract
```

**Outputs:**
- `workspace/store/spark/text/*.txt` - Extracted text documents
- `workspace/store/spark/entities/entities.tsv` - Entity annotations (TOPICS, LANG_STATS, LICENSE, etc.)

### Step 2: Wikipedia Extraction (Spark)

Extract structured data from Wikipedia XML dump.

```bash
# Prerequisites: Download Wikipedia dump to wiki_dump/ directory
# wget https://dumps.wikimedia.org/enwiki/latest/enwiki-latest-pages-articles.xml.bz2
# bunzip2 enwiki-latest-pages-articles.xml.bz2

# Test with small sample (100 pages)
WIKI_ARGS="--wiki-max-pages 100" docker compose run --rm spark-wiki

# Full extraction (2-3 hours for ~7M pages)
SPARK_DRIVER_MEMORY=12g SPARK_EXECUTOR_MEMORY=6g docker compose run --rm spark-wiki
```

**Outputs:**
- `workspace/store/wiki/pages.tsv` - Page metadata
- `workspace/store/wiki/categories.tsv` - Category associations
- `workspace/store/wiki/links.tsv` - Internal links
- `workspace/store/wiki/infobox.tsv` - Infobox fields
- `workspace/store/wiki/abstract.tsv` - Lead section text
- `workspace/store/wiki/aliases.tsv` - Redirect mappings
- `workspace/store/wiki/text/*.txt` - Full article text

### Step 3: Entity-Wikipedia Join (Spark)

Join extracted HTML entities with Wikipedia data.

```bash
# Test with sample
JOIN_ARGS="--entities-max-rows 10000" docker compose run --rm spark-join

# Full join
docker compose run --rm spark-join
```

**Outputs:**
- `workspace/store/join/html_wiki.tsv` - Per-entity join results with:
  - `doc_id, entity_type, entity_value, norm_value`
  - `wiki_page_id, wiki_title, wiki_abstract, wiki_categories`
  - `join_key, confidence`
- `workspace/store/join/html_wiki_agg.tsv` - Per-document aggregates
- `workspace/store/join/join_stats.json` - Join statistics including `unique_wiki_pages_joined`

**Confidence Labels:**
- `exact+cat` - Direct title match + relevant category
- `alias+cat` - Alias/redirect match + relevant category
- `exact+abs` - Direct title match + abstract available
- `alias+abs` - Alias match + abstract available

### Step 4: Build PyLucene Index

Build the Lucene index with all enrichment data.

```bash
# Build index
docker compose run --rm lucene-build

# Build with limit (for testing)
docker compose run --rm lucene python -m lucene_indexer.build \
  --text-dir /opt/app/workspace/store/text \
  --entities /opt/app/workspace/store/spark/entities/entities.tsv \
  --wiki-join /opt/app/workspace/store/join/html_wiki.tsv \
  --output /opt/app/workspace/store/lucene_index \
  --limit 1000
```

**Outputs:**
- `workspace/store/lucene_index/` - Lucene index directory
- `workspace/store/lucene_index/manifest.json` - Index metadata

### Step 5: Search with PyLucene

Run various query types against the Lucene index.

#### Simple Query (Full-text)
```bash
QUERY="python web framework" docker compose run --rm lucene-search
```

#### Boolean Query (AND/OR/NOT)
```bash
QUERY="python AND docker" QUERY_TYPE="boolean" docker compose run --rm lucene-search
QUERY="javascript OR typescript" QUERY_TYPE="boolean" docker compose run --rm lucene-search
QUERY="machine learning AND NOT tensorflow" QUERY_TYPE="boolean" docker compose run --rm lucene-search
```

#### Range Query (Numeric fields)
```bash
# Using interactive container
docker compose run --rm lucene python -m lucene_indexer.search \
  --index /opt/app/workspace/store/lucene_index \
  --type range \
  --field star_count \
  --min-value 1000
```

#### Phrase Query (Exact phrase)
```bash
QUERY="neural network" QUERY_TYPE="phrase" docker compose run --rm lucene-search
```

#### Fuzzy Query (Typo-tolerant)
```bash
QUERY="pyhton" QUERY_TYPE="fuzzy" docker compose run --rm lucene-search
QUERY="javascrpt" QUERY_TYPE="fuzzy" docker compose run --rm lucene-search
```

### Step 6: Compare TF-IDF vs Lucene

Compare search results between old TF-IDF and new Lucene indexes.

```bash
docker compose run --rm lucene-compare
```

**Output:**
- `reports/index_comparison.md` - Markdown report with:
  - Overlap metrics (Overlap@K, Jaccard)
  - Latency comparison
  - Query-by-query results

### Step 7: Unified Search Interface

Use the config-driven search interface.

```bash
# Search with Lucene engine
QUERY="python web" ENGINE="lucene" docker compose run --rm unified-search

# Search with TF-IDF engine
QUERY="python web" ENGINE="tfidf" docker compose run --rm unified-search
```

---

## Index Schema

| Field | Type | Stored | Indexed | Purpose |
|-------|------|--------|---------|---------|
| `doc_id` | StringField | Yes | Yes | Stable document key |
| `title` | TextField | Yes | Yes | Full-text search |
| `content` | TextField | No | Yes | Main body text |
| `path` | StoredField | Yes | No | File path |
| `url` | StringField | Yes | Yes | GitHub URL |
| `topics` | TextField (multi) | Yes | Yes | GitHub topics |
| `languages` | TextField (multi) | Yes | Yes | Programming languages |
| `license` | StringField | Yes | Yes | Exact license matching |
| `star_count` | IntPoint + DV | Yes | Yes | Range queries, sorting |
| `fork_count` | IntPoint | Yes | Yes | Range queries |
| `wiki_page_id` | LongPoint | Yes | Yes | Filter wiki-matched docs |
| `wiki_title` | TextField (multi) | Yes | Yes | Wikipedia concepts |
| `wiki_categories` | TextField (multi) | Yes | Yes | Category filtering |
| `wiki_abstract` | TextField | Yes | Yes | Semantic search |
| `join_confidence` | StringField | Yes | Yes | Match quality filter |
| `indexed_at` | LongPoint | Yes | Yes | Date range queries |
| `content_length` | IntPoint | Yes | Yes | Document size filter |

---

## Query Types Summary

| Type | Example | Description |
|------|---------|-------------|
| `simple` | "python web framework" | Full-text across all text fields |
| `boolean` | "python AND docker" | Explicit AND/OR/NOT |
| `range` | star_count:[1000 TO *] | Numeric field ranges |
| `phrase` | "neural network" | Exact phrase matching |
| `fuzzy` | "pyhton" | Typo-tolerant (Levenshtein) |

---

## Memory Configuration

| Job | Driver Memory | Executor Memory | Notes |
|-----|---------------|-----------------|-------|
| HTML Extract (sample) | 2g | 1g | Small samples |
| HTML Extract (full) | 4g | 2g | Default |
| Wiki Extract (test) | 4g | 2g | ≤1000 pages |
| Wiki Extract (full) | 12g | 6g | 100GB+ dumps |
| Wiki Join | 6g | 3g | Default |
| Lucene Build | N/A | N/A | Uses JVM heap |

Override with environment variables:
```bash
SPARK_DRIVER_MEMORY=16g SPARK_EXECUTOR_MEMORY=8g docker compose run --rm spark-wiki
```

---

## Troubleshooting

### Spark maxResultSize Error
```
Total size of serialized results is bigger than spark.driver.maxResultSize
```
**Solution:** Add `--conf spark.driver.maxResultSize=2g` to spark-submit command.

### PyLucene Import Error
```
ImportError: No module named 'lucene'
```
**Solution:** Use Docker container `coady/pylucene:9` which has PyLucene pre-installed.

### OOM During Wikipedia Extraction
**Solution:**
1. Increase driver memory: `SPARK_DRIVER_MEMORY=16g`
2. Reduce partition size: `PARTITIONS=512`
3. Use `--wiki-max-pages` to limit processing

### No Wiki Matches in Join
**Cause:** Small Wikipedia sample doesn't contain matching pages.
**Solution:** Run full Wikipedia extraction or use a larger sample.

---

## File Structure

```
workspace/
├── store/
│   ├── html/           # Crawled HTML (input)
│   ├── text/           # Legacy extracted text
│   ├── entities/       # Legacy entities
│   ├── spark/
│   │   ├── text/       # Spark-extracted text
│   │   └── entities/   # Spark-extracted entities
│   ├── wiki/
│   │   ├── pages.tsv
│   │   ├── categories.tsv
│   │   ├── links.tsv
│   │   ├── infobox.tsv
│   │   ├── abstract.tsv
│   │   ├── aliases.tsv
│   │   └── text/       # Wikipedia article text
│   ├── join/
│   │   ├── html_wiki.tsv
│   │   ├── html_wiki_agg.tsv
│   │   └── join_stats.json
│   ├── index/          # TF-IDF index
│   └── lucene_index/   # PyLucene index
├── logs/
│   ├── wiki_extract.jsonl
│   └── wiki_join.jsonl
└── runs/               # Run manifests
```

---

## Verification Commands

```bash
# Check entity counts
wc -l workspace/store/spark/entities/entities.tsv

# Check join statistics
cat workspace/store/join/join_stats.json | python -m json.tool

# Check Lucene index manifest
cat workspace/store/lucene_index/manifest.json | python -m json.tool

# Count documents in Lucene index
docker compose run --rm lucene python -c "
import lucene
lucene.initVM()
from org.apache.lucene.store import FSDirectory
from org.apache.lucene.index import DirectoryReader
from java.nio.file import Paths
d = FSDirectory.open(Paths.get('/opt/app/workspace/store/lucene_index'))
r = DirectoryReader.open(d)
print(f'Documents: {r.numDocs()}')
r.close()
"
```
