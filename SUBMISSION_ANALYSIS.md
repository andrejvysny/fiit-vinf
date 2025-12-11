# Phase Two Submission Analysis

Status snapshot: repository at `/Users/andrejvysny/fiit/vinf` inspected against Phase 2 requirements (Konzultácia 3–5 + second submission deliverables). Evidence is drawn from code and artefacts committed to git and the `workspace/` tree.

## Legend
- ✅ Met: implemented with evidence in repo
- ⚠️ Partial: code exists but artefacts/docs missing or incomplete
- ❌ Missing: no implementation or evidence found

## Wiki Structure & Admin Preconditions
- ❌ Required wiki page tree (`start`, numbered konzultácie/odovzdania) not present in repo; no wiki exports or markdown substitutes discovered.
- ❌ Consultation booking/rules (slot time, “všetko musí byť na wiki”, contact) not documented in repo.

## Konzultácia 3 – Spark + Wiki Join
- ✅ Spark HTML extraction implemented with DataFrame + `mapPartitions`, disk-first writes, and stats (`spark/jobs/html_extractor.py`; runtime stats saved to `stats/html_extraction.json` and `stats/history/html_extraction_20251211_200423.json`).
- ✅ Wikipedia dump extraction implemented with DataFrame + regex UDFs, DISK_ONLY persistence, optional full-text export (`spark/jobs/wiki_extractor.py`; outputs under `workspace/store/spark/wiki/*.tsv` and mirrored into `workspace/store/wiki`).
- ✅ Regex-based parsing of wiki entities present (`spark/lib/wiki_regexes.py`).
- ⚠️ Entity–Wiki join logic implemented with normalization, alias support, confidence labels, and category/abstract enrichment (`spark/jobs/join_html_wiki.py`), but no materialized outputs: `workspace/store/join/html_wiki.tsv/` is empty and no `join_stats.json` exists. Entities default path is `/workspace/store/spark/entities/entities.tsv`, yet the only large entities file is `workspace/store/entities/entities2.tsv` (~2.1 GB) which the join job will not pick up without reconfiguration.
- ❌ No documented counts of unique joined wiki pages or joined documents; `stats/` lacks join metrics.

## Konzultácia 4 – PyLucene Index
- ✅ PyLucene indexer implemented with explicit schema and field justifications (`lucene_indexer/schema.py`) and builder (`lucene_indexer/build.py`) integrating entities + wiki joins.
- ✅ Search engine supports multiple query types (simple, boolean, range, phrase, fuzzy, combined) (`lucene_indexer/search.py`). CLI wiring present in `bin/cli` and Docker Compose services (`docker-compose.yml`).
- ⚠️ No built Lucene index in `workspace/store/lucene_index/`; builder not executed (no manifest or stats). Without a concrete index, search/compare cannot be validated.
- ⚠️ Boosting requirements (title 10x, abstract 5x, categories 3x, infobox 2x) are not applied. Current search only uses mild boosts in code and index-time fields are unweighted (`lucene_indexer/build.py`, `lucene_indexer/search.py`).
- ⚠️ TF-IDF vs Lucene comparison script exists (`lucene_indexer/compare.py`), but no comparison reports or metrics are present.

## Konzultácia 5 – Final Software & Evaluation
- ⚠️ Software pipeline pieces exist (crawler in `01_block/crawler`, Spark jobs, PyLucene indexer), but there is no evidence of an end-to-end run producing searchable results; Lucene index and join outputs are missing.
- ❌ No evaluation of search quality vs Google/crawled-domain queries; no tables with at least 5 queries.
- ❌ No presentation slides or run logs for final demo.

## Second Submission Report (11.12)
- ❌ No 3-page wiki/report covering Spark transition, wiki processing, join rationale, PyLucene schema, query types, evaluations, and comparisons. Existing docs in `deprecated/docs/` are older and not updated for current state.
- ❌ No ZIP of current code or explicit GitHub link for submission (only legacy `01_block/VINF_Vysny.zip` remains).
- ❌ No documented wiki attributes indexed, join statistics, or token counts; no TF/IDF method comparisons on new data.

## Data & Compliance Notes
- Dataset volume: entities file `workspace/store/entities/entities2.tsv` is ~2.1 GB, and wiki TSVs total ~65 MB, but size/completeness vs 500 MB+/1M-record requirement is not documented.
- Big-data rules: Spark code avoids `.collect()` on full datasets, uses DISK_ONLY persistence and streaming writes; no disallowed libs (`requirements.txt` only lists `httpx`, `pyyaml`, `tiktoken`). PyLucene code operates file-by-file and does not rely on pandas/numpy.
- Ops: `docker-compose.yml` and `bin/cli` wrap Spark jobs, but only HTML extraction stats are recorded; wiki/join/index stages lack manifests/stats in `runs/` or `stats/`.

## Immediate Gaps to Address
1) Run entity–wiki join with correct entities path and persist outputs (`html_wiki.tsv`, `join_stats.json`, aggregates).  
2) Build PyLucene index from text + entities + join outputs; add field/query boosting per requirements.  
3) Produce comparison of TF-IDF vs PyLucene results and evaluation vs Google/domain queries (≥5 queries, text tables).  
4) Publish required wiki/report structure with consultations, QA pairs, architecture diagrams, headers/timeouts rationale, and second-submission sections.  
5) Package deliverables (updated ZIP/GitHub link) and ensure stats/runs captured for wiki + join + index steps.
