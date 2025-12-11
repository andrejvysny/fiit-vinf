# Index Comparison Report

Generated: 2025-11-28T22:54:16.073078

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
| Latency (ms) | 45.69 | 58.58 |

**Result Overlap**:
- Top-5: 20.0%
- Top-10: 10.0%
- Top-20: 5.0%

**Latency improvement**: -28.2%

### Query: `machine learning`
**Type**: simple

| Metric | TF-IDF | Lucene |
|--------|--------|--------|
| Results | 20 | 20 |
| Latency (ms) | 43.83 | 28.40 |

**Result Overlap**:
- Top-5: 0.0%
- Top-10: 0.0%
- Top-20: 10.0%

**Latency improvement**: 35.2%

### Query: `docker container`
**Type**: simple

| Metric | TF-IDF | Lucene |
|--------|--------|--------|
| Results | 20 | 20 |
| Latency (ms) | 9.99 | 25.65 |

**Result Overlap**:
- Top-5: 0.0%
- Top-10: 10.0%
- Top-20: 40.0%

**Latency improvement**: -156.7%

### Query: `javascript framework`
**Type**: simple

| Metric | TF-IDF | Lucene |
|--------|--------|--------|
| Results | 20 | 20 |
| Latency (ms) | 26.84 | 39.80 |

**Result Overlap**:
- Top-5: 0.0%
- Top-10: 20.0%
- Top-20: 10.0%

**Latency improvement**: -48.3%

### Query: `database optimization`
**Type**: simple

| Metric | TF-IDF | Lucene |
|--------|--------|--------|
| Results | 20 | 20 |
| Latency (ms) | 10.22 | 25.31 |

**Result Overlap**:
- Top-5: 0.0%
- Top-10: 10.0%
- Top-20: 15.0%

**Latency improvement**: -147.6%

### Query: `python AND docker`
**Type**: boolean

| Metric | TF-IDF | Lucene |
|--------|--------|--------|
| Results | 20 | 20 |
| Latency (ms) | 43.40 | 38.37 |

**Result Overlap**:
- Top-5: 0.0%
- Top-10: 20.0%
- Top-20: 45.0%

**Latency improvement**: 11.6%

### Query: `web OR api`
**Type**: boolean

| Metric | TF-IDF | Lucene |
|--------|--------|--------|
| Results | 20 | 20 |
| Latency (ms) | 49.62 | 19.94 |

**Result Overlap**:
- Top-5: 0.0%
- Top-10: 0.0%
- Top-20: 5.0%

**Latency improvement**: 59.8%

### Query: `machine AND learning AND neural`
**Type**: boolean

| Metric | TF-IDF | Lucene |
|--------|--------|--------|
| Results | 20 | 20 |
| Latency (ms) | 34.62 | 22.38 |

**Result Overlap**:
- Top-5: 0.0%
- Top-10: 0.0%
- Top-20: 30.0%

**Latency improvement**: 35.4%

### Query: `machine learning`
**Type**: phrase

| Metric | TF-IDF | Lucene |
|--------|--------|--------|
| Results | 20 | 20 |
| Latency (ms) | 26.50 | 37.10 |

**Result Overlap**:
- Top-5: 0.0%
- Top-10: 10.0%
- Top-20: 30.0%

**Latency improvement**: -40.0%

### Query: `web application`
**Type**: phrase

| Metric | TF-IDF | Lucene |
|--------|--------|--------|
| Results | 20 | 20 |
| Latency (ms) | 16.20 | 23.71 |

**Result Overlap**:
- Top-5: 0.0%
- Top-10: 0.0%
- Top-20: 10.0%

**Latency improvement**: -46.4%

### Query: `open source`
**Type**: phrase

| Metric | TF-IDF | Lucene |
|--------|--------|--------|
| Results | 20 | 20 |
| Latency (ms) | 59.67 | 30.91 |

**Result Overlap**:
- Top-5: 0.0%
- Top-10: 0.0%
- Top-20: 5.0%

**Latency improvement**: 48.2%

### Query: `pyhton`
**Type**: fuzzy

| Metric | TF-IDF | Lucene |
|--------|--------|--------|
| Results | 20 | 20 |
| Latency (ms) | 0.55 | 106.27 |

**Result Overlap**:
- Top-5: 0.0%
- Top-10: 0.0%
- Top-20: 0.0%

**Latency improvement**: -19117.9%

### Query: `machin lerning`
**Type**: fuzzy

| Metric | TF-IDF | Lucene |
|--------|--------|--------|
| Results | 19 | 20 |
| Latency (ms) | 0.50 | 38.18 |

**Result Overlap**:
- Top-5: 0.0%
- Top-10: 0.0%
- Top-20: 0.0%

**Latency improvement**: -7520.1%

### Query: `javascrpt`
**Type**: fuzzy

| Metric | TF-IDF | Lucene |
|--------|--------|--------|
| Results | 1 | 20 |
| Latency (ms) | 0.31 | 29.40 |

**Result Overlap**:
- Top-5: 0.0%
- Top-10: 0.0%
- Top-20: 0.0%

**Latency improvement**: -9239.7%

## Overall Statistics

| Metric | TF-IDF | Lucene | Improvement |
|--------|--------|--------|-------------|
| Avg Latency (ms) | 26.28 | 37.43 | -42.4% |
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
