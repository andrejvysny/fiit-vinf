# Index Comparison Report

Generated: 2025-11-28T21:29:34.021076

## Summary

- Queries compared: 14
- TF-IDF index: /opt/app/workspace/store/index/default
- Lucene index: /opt/app/workspace/store/lucene_index

## Query Types Demonstrated

| Type | Description | Example |
|------|-------------|---------|
| Simple | Full-text search across multiple fields | `python web scraping` |
| Boolean | AND/OR queries | `python AND docker` |
| Range | Numeric range queries | `star_count:[1000 TO *]` |
| Phrase | Exact phrase matching | `"machine learning"` |
| Fuzzy | Typo-tolerant matching | `pyhton~2` |

## Results by Query

### Query: `python web`
**Type**: simple

| Metric | TF-IDF | Lucene |
|--------|--------|--------|
| Results | 20 | 20 |
| Latency (ms) | 50.12 | 70.07 |

**Result Overlap**:
- Top-5: 20.0%
- Top-10: 10.0%
- Top-20: 5.0%

**Latency improvement**: -39.8%

### Query: `machine learning`
**Type**: simple

| Metric | TF-IDF | Lucene |
|--------|--------|--------|
| Results | 20 | 20 |
| Latency (ms) | 46.39 | 36.12 |

**Result Overlap**:
- Top-5: 0.0%
- Top-10: 0.0%
- Top-20: 10.0%

**Latency improvement**: 22.2%

### Query: `docker container`
**Type**: simple

| Metric | TF-IDF | Lucene |
|--------|--------|--------|
| Results | 20 | 20 |
| Latency (ms) | 10.23 | 27.06 |

**Result Overlap**:
- Top-5: 0.0%
- Top-10: 10.0%
- Top-20: 40.0%

**Latency improvement**: -164.5%

### Query: `javascript framework`
**Type**: simple

| Metric | TF-IDF | Lucene |
|--------|--------|--------|
| Results | 20 | 20 |
| Latency (ms) | 27.57 | 43.93 |

**Result Overlap**:
- Top-5: 0.0%
- Top-10: 20.0%
- Top-20: 10.0%

**Latency improvement**: -59.3%

### Query: `database optimization`
**Type**: simple

| Metric | TF-IDF | Lucene |
|--------|--------|--------|
| Results | 20 | 20 |
| Latency (ms) | 10.36 | 28.35 |

**Result Overlap**:
- Top-5: 0.0%
- Top-10: 10.0%
- Top-20: 15.0%

**Latency improvement**: -173.8%

### Query: `python AND docker`
**Type**: boolean

| Metric | TF-IDF | Lucene |
|--------|--------|--------|
| Results | 20 | 20 |
| Latency (ms) | 46.31 | 34.53 |

**Result Overlap**:
- Top-5: 0.0%
- Top-10: 20.0%
- Top-20: 45.0%

**Latency improvement**: 25.4%

### Query: `web OR api`
**Type**: boolean

| Metric | TF-IDF | Lucene |
|--------|--------|--------|
| Results | 20 | 20 |
| Latency (ms) | 53.88 | 28.41 |

**Result Overlap**:
- Top-5: 0.0%
- Top-10: 0.0%
- Top-20: 5.0%

**Latency improvement**: 47.3%

### Query: `machine AND learning AND neural`
**Type**: boolean

| Metric | TF-IDF | Lucene |
|--------|--------|--------|
| Results | 20 | 20 |
| Latency (ms) | 38.35 | 17.09 |

**Result Overlap**:
- Top-5: 0.0%
- Top-10: 0.0%
- Top-20: 30.0%

**Latency improvement**: 55.4%

### Query: `machine learning`
**Type**: phrase

| Metric | TF-IDF | Lucene |
|--------|--------|--------|
| Results | 20 | 20 |
| Latency (ms) | 27.72 | 41.87 |

**Result Overlap**:
- Top-5: 0.0%
- Top-10: 10.0%
- Top-20: 30.0%

**Latency improvement**: -51.1%

### Query: `web application`
**Type**: phrase

| Metric | TF-IDF | Lucene |
|--------|--------|--------|
| Results | 20 | 20 |
| Latency (ms) | 17.37 | 35.82 |

**Result Overlap**:
- Top-5: 0.0%
- Top-10: 0.0%
- Top-20: 10.0%

**Latency improvement**: -106.3%

### Query: `open source`
**Type**: phrase

| Metric | TF-IDF | Lucene |
|--------|--------|--------|
| Results | 20 | 20 |
| Latency (ms) | 59.58 | 33.30 |

**Result Overlap**:
- Top-5: 0.0%
- Top-10: 0.0%
- Top-20: 5.0%

**Latency improvement**: 44.1%

### Query: `pyhton`
**Type**: fuzzy

| Metric | TF-IDF | Lucene |
|--------|--------|--------|
| Results | 20 | 20 |
| Latency (ms) | 0.43 | 104.63 |

**Result Overlap**:
- Top-5: 0.0%
- Top-10: 0.0%
- Top-20: 0.0%

**Latency improvement**: -24440.5%

### Query: `machin lerning`
**Type**: fuzzy

| Metric | TF-IDF | Lucene |
|--------|--------|--------|
| Results | 19 | 20 |
| Latency (ms) | 0.62 | 44.38 |

**Result Overlap**:
- Top-5: 0.0%
- Top-10: 0.0%
- Top-20: 0.0%

**Latency improvement**: -7019.5%

### Query: `javascrpt`
**Type**: fuzzy

| Metric | TF-IDF | Lucene |
|--------|--------|--------|
| Results | 1 | 20 |
| Latency (ms) | 0.34 | 26.98 |

**Result Overlap**:
- Top-5: 0.0%
- Top-10: 0.0%
- Top-20: 0.0%

**Latency improvement**: -7903.0%

## Overall Statistics

| Metric | TF-IDF | Lucene | Improvement |
|--------|--------|--------|-------------|
| Avg Latency (ms) | 27.80 | 40.90 | -47.1% |
| Avg Overlap@5 | - | - | 1.4% |
| Avg Overlap@10 | - | - | 5.7% |
| Avg Overlap@20 | - | - | 14.6% |

## Lucene Index Advantages

### 1. Query Type Support
- **Boolean queries**: Native AND/OR/NOT support with proper precedence
- **Phrase queries**: Exact phrase matching with configurable slop
- **Fuzzy queries**: Levenshtein distance-based typo tolerance
- **Range queries**: Efficient numeric range filtering (e.g., star_count > 1000)

### 2. Structured Fields
- Entity fields (topics, languages) as separate indexed fields
- Wiki enrichment fields for enhanced search
- Numeric fields with range query support

### 3. Scalability
- Efficient inverted index with skip lists
- Compressed postings for reduced disk usage
- Term dictionary with FST encoding

### 4. Relevance
- BM25 scoring by default (superior to basic TF-IDF)
- Field boosting for relevance tuning
- Multi-field search with configurable weights
