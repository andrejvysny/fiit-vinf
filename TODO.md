# VINF Pipeline - Comprehensive Testing & Analysis

## 1. Extract All HTML Data Using Spark
- [ ] Verify Docker Compose setup for spark-extract
- [ ] Run full HTML extraction pipeline: `bin/cli extract`
- [ ] Monitor Spark job and memory usage
- [ ] Validate output in `workspace/store/text/` and `workspace/store/spark/entities/`
- [ ] Confirm entities.tsv contains expected entity types (stars, forks, language, license, etc.)
- [ ] Check extraction statistics and log files

## 2. Extract All WIKI Data and Entities
- [ ] Run Wikipedia extraction: `bin/cli wiki`
- [ ] Validate all 6 Wikipedia TSV files generated:
  - `pages.tsv` (page_id, title, redirect_target)
  - `categories.tsv` (page_id, category)
  - `links.tsv` (source_id, target_title)
  - `infobox.tsv` (page_id, field, value)
  - `abstract.tsv` (page_id, abstract)
  - `aliases.tsv` (alias, target_title)
- [ ] Verify Wikipedia text extraction in `workspace/store/wiki/text/`
- [ ] Check extraction logs and row counts

## 3. Join HTML Entities with Wikipedia Data
- [ ] Run join operation: `bin/cli join`
- [ ] Validate output in `workspace/store/join/html_wiki.tsv`
- [ ] Count successful JOINs (target: thousands)
- [ ] Analyze JOIN statistics:
  - Total entities
  - Matched entities
  - Match rate by entity type
  - Most common Wikipedia pages matched
- [ ] Document any unmatched entities (debugging)

## 4. Build PyLucene Index and Validate
- [ ] Build full index: `bin/cli lucene-build`
- [ ] Validate index structure in `workspace/store/lucene_index/`
- [ ] Check index parameters:
  - Total documents indexed
  - Field counts (title, abstract, categories, content, etc.)
  - Index size (MB)
  - Field analyzers configuration
- [ ] Verify index is searchable: `bin/cli lucene-search "test"`

## 5. Implement Boosting for Important Fields
- [ ] Analyze current Lucene schema in `lucene_indexer/schema.py`
- [ ] Identify important fields (title, abstract, categories, infobox)
- [ ] Implement field-level boost factors:
  - Title: 10x
  - Abstract: 5x
  - Categories: 3x
  - Infobox: 2x
  - Body text: 1x
- [ ] Implement query-time boosting for relevant query terms
- [ ] Test boosting impact on search results
- [ ] Document boost strategy and rationale

## 6. Thorough Comparison of Search Methods
- [ ] Implement query variations:
  - Exact phrase matches
  - Boolean (AND, OR, NOT)
  - Fuzzy matching (edit distance)
  - Wildcard queries
  - Range queries
  - Proximity queries
- [ ] Create test query set (20-30 representative queries)
- [ ] Compare across search methods:
  - Precision (relevant results in top-10)
  - Recall (% of relevant docs found)
  - Search latency
  - Result ranking quality
- [ ] Run comparison: `bin/cli lucene-compare` (if available, or create script)
- [ ] Document findings

## 7. Thorough Analysis & Metrics Comparison
- [ ] Analyze current implementation:
  - Index structure effectiveness
  - Query processing pipeline
  - Ranking algorithm
  - Performance bottlenecks
- [ ] Define comparison metrics:
  - **Precision@k** (k=5,10,20)
  - **Recall@k**
  - **Mean Reciprocal Rank (MRR)**
  - **Normalized Discounted Cumulative Gain (NDCG)**
  - **Query latency** (p50, p95, p99)
  - **Index memory footprint**
- [ ] Compare vs. baseline search engines:
  - Elasticsearch (if available)
  - Solr (if available)
  - Simple TF-IDF (in `indexer/`)
- [ ] Create summary report:
  - Method comparison table
  - Performance analysis
  - Recommendations for best approach
  - Trade-offs (accuracy vs. speed vs. memory)
- [ ] Document outcomes in `ANALYSIS.md`

---

## Status Summary
- [ ] All tasks completed
- [ ] Report generated
- [ ] Recommendations documented

## Notes
- Use Docker Compose for all Spark jobs
- Monitor Spark UI at http://localhost:4040 during jobs
- Save all metrics and comparison results for final report
- Document any issues encountered
