# Full Pipeline Extraction and Index Build Guide

This guide covers running the complete VINF pipeline with full Wikipedia dump and HTML data.

## Prerequisites

1. **Docker** installed and running
2. **Disk space**: ~150GB for Wikipedia dump + processed data
3. **RAM**: Minimum 8GB, recommended 16GB+

## Step 1: Prepare Data

### 1.1 Download Wikipedia Dump

```bash
# Create data directory
mkdir -p workspace/source/wiki

# Download Slovak Wikipedia dump (~2GB) or English (~20GB)
# Slovak:
wget -P workspace/source/wiki https://dumps.wikimedia.org/skwiki/latest/skwiki-latest-pages-articles.xml.bz2

# English (large, ~20GB compressed):
# wget -P workspace/source/wiki https://dumps.wikimedia.org/enwiki/latest/enwiki-latest-pages-articles.xml.bz2
```

### 1.2 Prepare HTML Data

Place your HTML files in:
```bash
workspace/source/html/
├── file1.html
├── file2.html
└── ...
```

Or use the existing scraped GitHub pages if available.

## Step 2: Run All Stages Manually

```bash
# 1. Extract entities from HTML files
bin/cli extract

# 2. Process Wikipedia dump (preconfigured Spark settings)
bin/cli wiki

# 3. Join HTML entities with Wikipedia
bin/cli join

# 4. Build PyLucene index
bin/cli lucene-build

# 5. View statistics
bin/cli stats
```

## Step 3: Test Searches

### 3.1 Simple Queries

```bash
# Basic full-text search
bin/cli lucene-search "python web framework"
bin/cli lucene-search "machine learning"
bin/cli lucene-search "docker kubernetes"
bin/cli lucene-search "javascript react"
```

### 3.2 Boolean Queries

```bash
# AND - both terms must match
bin/cli lucene-search "python AND django" --type boolean
bin/cli lucene-search "machine AND learning AND neural" --type boolean

# OR - either term matches
bin/cli lucene-search "python OR javascript" --type boolean
bin/cli lucene-search "docker OR kubernetes OR container" --type boolean

# NOT - exclude terms
bin/cli lucene-search "python AND NOT django" --type boolean
```

### 3.3 Phrase Queries

```bash
# Exact phrase matching
bin/cli lucene-search "machine learning" --type phrase
bin/cli lucene-search "web application" --type phrase
bin/cli lucene-search "open source" --type phrase
bin/cli lucene-search "deep neural network" --type phrase
```

### 3.4 Fuzzy Queries (Typo Tolerance)

```bash
# With typos - should still find results
bin/cli lucene-search "pyhton" --type fuzzy          # python
bin/cli lucene-search "javscript" --type fuzzy       # javascript
bin/cli lucene-search "machin lerning" --type fuzzy  # machine learning
bin/cli lucene-search "kuberntes" --type fuzzy       # kubernetes
```

### 3.5 Range Queries

```bash
# By star count
bin/cli lucene-search "star_count:[1000 TO *]" --type range
bin/cli lucene-search "star_count:[100 TO 500]" --type range

# By fork count
bin/cli lucene-search "fork_count:[50 TO *]" --type range
```

### 3.6 Field-Specific Queries

```bash
# Search in specific fields
bin/cli lucene-search "python" --field topics
bin/cli lucene-search "MIT" --field license
bin/cli lucene-search "TypeScript" --field languages
```

## Step 4: Compare TF-IDF vs Lucene

### 4.1 Create Test Queries File

Edit `queries.txt`:
```
# Format: type|query
simple|python web
simple|machine learning
simple|docker container
simple|javascript framework
simple|database optimization
simple|react native mobile
simple|api rest graphql

boolean|python AND docker
boolean|web OR api
boolean|machine AND learning AND neural
boolean|javascript AND NOT jquery
boolean|database AND (mysql OR postgresql)

phrase|machine learning
phrase|web application
phrase|open source
phrase|deep learning
phrase|natural language processing

fuzzy|pyhton
fuzzy|machin lerning
fuzzy|javascrpt
fuzzy|kuberntes
fuzzy|postgrsql
```

### 4.2 Run Comparison

```bash
bin/cli lucene-compare
```

### 4.3 View Results

Results are saved to:
- `reports/index_comparison.md` - Markdown report
- `reports/index_comparison.json` - JSON data
- `stats/index_comparison.json` - Pipeline statistics

## Step 5: Verify Results

### 5.1 Check Statistics

```bash
bin/cli stats
```

Expected output for full run:
```
=== Pipeline Statistics ===

Wiki Extraction:
  - Pages processed: 500,000+
  - Categories: 1,000,000+
  - Links: 10,000,000+

HTML Extraction:
  - Files processed: 28,353
  - Entities extracted: 10,130

Join:
  - Matched entities: varies by wiki size

Lucene Index:
  - Documents indexed: 28,353
  - With entities: 30
  - With wiki: depends on matches
```

### 5.2 Validate Search Quality

```bash
# Check that searches return relevant results
bin/cli lucene-search "python" --top 5

# Verify fuzzy matching works
bin/cli lucene-search "pyhton" --type fuzzy --top 5

# Compare with TF-IDF
bin/cli lucene-compare
```

## Troubleshooting

### Out of Memory

```bash
# Increase Docker memory limit in Docker Desktop settings
# Or use smaller Wikipedia dump (Slovak instead of English)
```

### Slow Wikipedia Processing

```bash
# Process in batches
bin/cli wiki --sample 10000  # First 10k pages
bin/cli wiki --sample 50000  # More pages
```

### Index Build Fails

```bash
# Check PyLucene container
docker compose run --rm lucene-build

# Verify text files exist
ls workspace/store/text/ | head
```

## Performance Tips

1. **SSD recommended** - Wikipedia processing is I/O intensive
2. **Parallel processing** - Spark uses available cores automatically
3. **Incremental builds** - Pipeline skips already processed files

## Expected Timings (on 8-core machine)

| Stage | Small (500 pages) | Medium (50k) | Full (~6M) |
|-------|-------------------|--------------|------------|
| Wiki extraction | 2s | 5min | 2-4 hours |
| HTML extraction | 1s | 1s | 1s |
| Join | <1s | 1min | 10-30min |
| Lucene build | 20s | 20s | 20s |
| Total | ~25s | ~7min | 3-5 hours |
