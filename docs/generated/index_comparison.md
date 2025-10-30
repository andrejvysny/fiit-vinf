# IDF Comparison

This report shows how the same search queries rank documents when scored with two common inverse document frequency (IDF) formulas. Each entry lists the top results under the **log** IDF (the default TF-IDF variant) and the **RSJ** IDF (Robertson/Sparck Jones weighting used in probabilistic retrieval). Higher scores mean the document is considered more relevant for the query.

- **Rank**: position within the top results for that weighting scheme.
- **log / rsj score**: TF-IDF style score; values are not probabilities, only relative weights.
- **log / rsj doc**: internal numeric document ID followed by the document title (the first non-empty line of the file, or the file stem).
- **Overlap** shows how many documents appear at the same rank in both lists; **Jaccard** summarises how similar the two top-k result sets are (1.0 = identical, 0.0 = no shared documents).

Generated on 2025-10-30T09:07:05+00:00 using index `/Users/andrejvysny/fiit/vinf/workspace/store/index/default`

## Query: `github crawler`

| Rank | log score | log doc | rsj score | rsj doc |
| --- | --- | --- | --- | --- |
| 1 | 27.8478 | 14558 (:root {) | -11.3665 | 14558 (:root {) |
| 2 | 26.8188 | 6827 (:root {) | -12.1840 | 6827 (:root {) |
| 3 | 26.6225 | 650 (:root {) | -12.9229 | 650 (:root {) |
| 4 | 25.0612 | 18808 (:root {) | -13.9471 | 25240 (Explore) |
| 5 | 24.5381 | 7529 (:root {) | -14.0955 | 5984 (:root {) |

Overlap (same rank): 3/5
Jaccard (top-5 sets): 0.43

The two methods agree on the top three results, but differ afterwards: the RSJ weighting brings in `doc 25240` while the log scoring keeps `doc 7529`. The partial overlap indicates the queries are largely driven by very frequent terms that log-IDF treats as more informative.

## Query: `async http client`

| Rank | log score | log doc | rsj score | rsj doc |
| --- | --- | --- | --- | --- |
| 1 | 46.5926 | 1146 (:root {) | 28.8809 | 1146 (:root {) |
| 2 | 44.5243 | 7875 (:root {) | 28.3499 | 7875 (:root {) |
| 3 | 43.4540 | 24045 (:root {) | 27.8492 | 24045 (:root {) |
| 4 | 40.4020 | 13708 (:root {) | 25.8950 | 13708 (:root {) |
| 5 | 38.6749 | 9958 (:root {) | 24.3844 | 9958 (:root {) |

Overlap (same rank): 5/5
Jaccard (top-5 sets): 1.00

All top documents match under both IDF strategies, so this query is stable: the relevant terms are sufficiently rare that both formulas assign consistent relative weights.

## Query: `repository metadata`

| Rank | log score | log doc | rsj score | rsj doc |
| --- | --- | --- | --- | --- |
| 1 | 24.0270 | 27565 (:root {) | 11.8861 | 11332 (:root {) |
| 2 | 19.7539 | 20164 (:root {) | 11.8861 | 14108 (:root {) |
| 3 | 19.6636 | 11332 (:root {) | 10.4110 | 20164 (:root {) |
| 4 | 19.6636 | 14108 (:root {) | 10.1356 | 27565 (:root {) |
| 5 | 19.3697 | 12824 (:root {) | 9.9717 | 26051 (:root {) |

Overlap (same rank): 0/5
Jaccard (top-5 sets): 0.67

Here the IDF formulas diverge noticeably—every rank changes even though the sets still share most documents. RSJ penalises the very frequent hits more aggressively, surfacing documents (like `doc 26051`) that log-IDF leaves outside the top five.
