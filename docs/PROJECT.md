Below is a sequential, deep design for a GitHub-focused Crawler + Scraper with a Manager orchestrating both. I keep it simple but robust, highlight key parts, and focus on architecture + functions (no code).

⸻

Step 1 — Analysis of the project idea

Goal:
	•	Discover and collect public GitHub repository pages (HTML only).
	•	Respect robots.txt, stay on-domain (github.com), and avoid disallowed paths.
	•	Store full HTML of target pages, plus one CSV with per-page metadata.
	•	Maintain a trajectory map (who linked to what, when) and rich metrics.
	•	Handle rate limits by dynamic proxy switching and adaptive throttling.
	•	Decouple responsibilities:
	•	Crawler = discovery + policy enforcement + link graph + frontier.
	•	Scraper = fetching + persistence of HTML + metadata CSV.
	•	Manager = orchestration, backpressure, proxy pool, monitoring, lifecycle.

Constraints & implications:
	•	Only github.com host (not raw.githubusercontent.com, api.github.com, etc.).
	•	robots.txt & path policy must be enforced before queueing (to avoid accidental fetches).
	•	Trajectory map requires referrer → target edges at time of discovery.
	•	CSV must remain a single source of truth for per-page metadata (append-only lines).
	•	10+ GB HTML scale implies:
	•	Low per-host concurrency, wide breadth across many repos.
	•	Deduplication (URL + content hash) to avoid wasting quota/disk.
	•	Crash-safe frontier and resumable runs.

⸻

Step 2 — Architecture (simple, modular, production-minded)

Top-level modules
	1.	Crawler (Discovery Engine)
	2.	Scraper (Acquisition & Persistence Engine)
	3.	Manager (Orchestrator & Control Plane)
	4.	Shared Libraries (policy, URL tools, metrics, storage, proxy pool)

They run as separate processes (or services) and can be used independently. All share a common on-disk workspace.

⸻

2.1 Crawler — responsibilities & internal components

Purpose: Discover eligible GitHub HTML pages and maintain the frontier and trajectory map.

Responsibilities:
	•	Enforce scope: host == github.com only.
	•	Enforce robots.txt and deny/allow rules strictly.
	•	Seed from safe entry points (e.g., /topics, /trending homepage, curated repo list).
	•	Parse HTML only as needed to extract eligible links (avoid deep content fetches).
	•	Maintain Frontier (persistent queue) with priorities (breadth-first bias).
	•	Emit Discoveries (URL + referrer + attributes) to the Scraper intake queue.
	•	Build Trajectory Map (edges) and crawl metrics.

Internal components (Crawler):
	•	C1. URL Policy & Robots Gate
	•	Inputs: raw URL candidates.
	•	Functions:
	•	Canonicalization (scheme/host normalize, drop fragments, drop/bucketize query params).
	•	Scope filter (host == github.com); subdomain blacklist (e.g., api., raw., gist.).
	•	Path matcher (allowlist: repo root, blob views, limited issues/PR detail; denylist: /search, /tree/, /commits/, /graphs/, ?q=, ?tab=, ?since= etc.).
	•	robots.txt cache + evaluation (agent name, crawl-delay, disallow).
	•	Output: ACCEPT | REJECT + reason.
	•	C2. Frontier Store (Persistent Queue)
	•	Backed by NoSQL KV (e.g., LMDB) or append-only files + index.
	•	Data: url, priority, depth, discovered_at, referrer, site shard key (to spread load).
	•	Functions:
	•	Pop respecting per-host tokens and global rate (no more than N active per host).
	•	De-duplication on canonical URL (Bloom filter + KV index).
	•	Requeue on soft failures with backoff.
	•	C3. Fetch (Light-HTML)
	•	Purpose: only fetch light pages necessary for further discovery (e.g., repo root, topics pages).
	•	Functions: GET with conservative headers, timeouts, small concurrency, conditional headers (If-Modified-Since/ETag) to reduce bytes.
	•	Yields: Status, headers, HTML body (for parsing), latency, bytes, proxy used.
	•	C4. HTML Link Extractor
	•	Functions:
	•	Extract <a href> and relevant navigation links.
	•	Resolve relative URLs, canonicalize, run through C1 again.
	•	Classify page-type (topic, repo-root, blob, issue, PR, profile-root).
	•	Emit DISCOVERY events to Scraper Intake.
	•	C5. Trajectory Map Builder
	•	Append edges: src_url, dst_url, src_timestamp, anchor_text?, depth.
	•	Store as CSV (edges.csv) and compact adjacency files per shard for later graph analysis.
	•	C6. Crawler Metrics
	•	Capture: pages_fetched, eligible_links_found, duplicates_skipped, robots_rejected, errors_by_code, avg_latency, bytes_in, per-host concurrency, proxy utilization.
	•	Flush to metrics.csv at intervals; expose a lightweight status JSON for the Manager.

Outputs (Crawler):
	•	Discoveries Queue (disk-backed): items = {url, referrer, depth, page_type, discovered_at}.
	•	edges.csv (trajectory).
	•	crawler_metrics.csv.
	•	frontier state (for resume).

Notes:
	•	The Crawler may fetch some HTML for discovery, but does not persist full pages—that is the Scraper’s job.
	•	To reduce duplication, when the Crawler has already fetched a page that is also a target to save, it can optionally hand off the already-fetched body to the Scraper via a shared spool (zero-copy handover). This is an optimization, not a dependency.

⸻

2.2 Scraper — responsibilities & internal components

Purpose: Fetch and persist full HTML for eligible GitHub pages and record per-page metadata in one CSV.

Responsibilities:
	•	Consume from Discoveries Queue; optionally also accept direct URL lists (standalone mode).
	•	Fetch full HTML, enforce content-type starts with text/html.
	•	Persist HTML as-is (optionally compressed) in a deterministic content-addressed path.
	•	Append one CSV record per page with metadata + metrics.
	•	Enforce proxy/rate/backoff policies; rotate proxies on rate-limit/bans.
	•	Emit Scraper metrics and health.

Internal components (Scraper):
	•	S1. Intake
	•	Read items from Discoveries Queue (or input file).
	•	Coalesce duplicates (content-hash + URL canonicalization).
	•	Track depth, referrer, page_type for metadata.
	•	S2. Fetch (Full-HTML)
	•	Functions:
	•	GET with conservative User-Agent, Accept-Language, Accept-Encoding.
	•	Strict timeouts, retries with exponential backoff on 429/5xx.
	•	Honor Retry-After, reduce concurrency, and signal Manager when throttled.
	•	Proxy aware (uses P3 pool via Manager’s Proxy Client).
	•	Record: status, latency_ms, response_bytes, content_type, encoding, etag, last_modified, proxy_id.
	•	S3. Persist (HTML Store)
	•	Pathing: store_root/aa/bb/<sha256>.html[.zst] (content-addressed by body hash).
	•	Save original HTML (no modification). Compression optional (e.g., zstd) to cut disk.
	•	Idempotent writes: check if file exists by hash before write.
	•	S4. Metadata Writer (Single CSV)
	•	Schema (example, append-only):
	•	timestamp, url, canonical_url, referrer, depth, page_type, http_status, content_type, encoding, content_sha256, content_bytes, stored_path, etag, last_modified, fetch_latency_ms, retries, proxy_id, robots_allowed, policy_rule, error?
	•	Atomic append: write via line-buffered temp → atomic rename to avoid corruption.
	•	One file for entire run (or daily rollover if size is huge; still “one CSV per run” from user’s POV).
	•	S5. Scraper Metrics
	•	Capture: pages_saved, MB_saved, avg_latency, by_status, by_page_type, by_proxy, retries, throttle events.
	•	Export to scraper_metrics.csv and live status JSON.

Outputs (Scraper):
	•	Full HTML files under store_root.
	•	pages.csv (the one CSV with metadata).
	•	scraper_metrics.csv.

⸻

2.3 Manager — responsibilities & internal components

Purpose: Orchestrate Crawler and Scraper lifecycles, enforce global policies, and maintain proxy health, backpressure, and observability.

Responsibilities:
	•	Start/stop Crawler and Scraper; pass config; reload policies on the fly.
	•	Maintain Proxy Pool; swap/rotate proxies dynamically on 429/ban; health-check and backoff.
	•	Apply global rate controls (tokens per host, global concurrency caps).
	•	Watch queues for backpressure; throttle Crawler when Scraper lags.
	•	Aggregate metrics and expose dashboard (simple TUI/JSON).
	•	Ensure all artifacts are written to disk (no SQL DB).

Internal components (Manager):
	•	M1. Configuration Service
	•	Single YAML/TOML for:
	•	Scope: host allowlist (github.com), path allow/deny rules, query param policy.
	•	Robots: agent name, cache TTL.
	•	Concurrency: global, per-host, per-proxy.
	•	Timeouts & retries.
	•	Proxy pool (static list + provider API, if any).
	•	Storage roots (frontier, queues, HTML store, CSV paths).
	•	Metrics frequency & rollover.
	•	M2. Proxy Pool & Rate Control
	•	Proxy descriptors: proxy_id, endpoint, weight, state (healthy/throttled/banned), cooldown_until, counters.
	•	Selection policy: weighted round-robin with stickiness (keep using same proxy until degraded).
	•	Degradation triggers:
	•	429 or Retry-After → cooldown proxy; lower concurrency; increase backoff.
	•	Spike in timeouts/5xx → suspect; test with health probe URL; quarantine if needed.
	•	Auto-recovery after cooldown; blacklist if repeatedly failing.
	•	M3. Backpressure & Scheduling
	•	Monitor Discoveries Queue depth; if above thresholds, pause/slows Crawler (fewer tokens).
	•	If Scraper idle and Frontier small, unpause and allow more breadth.
	•	Enforce per-repo caps (e.g., max N pages/repo) to spread load.
	•	M4. Observability & Audit
	•	Aggregate crawler_metrics.csv + scraper_metrics.csv into run_metrics.csv.
	•	Provide live status (e.g., HTTP on localhost or periodic JSON dump):
	•	Q depth, pages/min, MB/hour, errors by class, top proxies, top repos, robots blocks, etc.
	•	Run manifest (run id, start/end, config hash, versions, exit reasons).

⸻

2.4 Shared Libraries (used by all)
	•	Policy Engine: implements scope, allow/deny paths, robots evaluation, and final gate before fetch.
	•	URL Tools: normalization/canonicalization, host filter, param sanitizer.
	•	Storage Primitives:
	•	Disk-backed queue (Discoveries Queue).
	•	Frontier KV + Bloom for dedup.
	•	Atomic CSV appender + daily rollover.
	•	Content store (hash-addressed + compression).
	•	Metrics: counters/timers/histograms with CSV sinks and in-memory snapshot.
	•	Proxy Client: fetch with proxy selection, observe headers (Retry-After), raise structured events.

⸻

Step 3 — Concept validation & risk analysis

A. robots + policy safety
	•	Risk: GitHub robots/policies are strict; hidden disallows may cause accidental hits.
	•	Mitigation: Deny-by-default model (only allow vetted patterns), pre-queue robots gate, nightly policy test suite with fixture URLs.

B. Rate limiting / bans
	•	Risk: 429/anti-bot throttles.
	•	Mitigation: Low starting concurrency (e.g., 1–2 global), per-proxy tokens, adaptive backoff, proxy cooldown, per-repo caps, and broad breadth over many repos.

C. Data volume & duplication
	•	Risk: repeated pages (mirrors, language toggles, same HTML under different params).
	•	Mitigation: Canonical URL + content hash dedupe; store once, reference from multiple metadata rows via same content_sha256.

D. Queue growth / backpressure
	•	Risk: Crawler outpaces Scraper → disk pressure and stale queues.
	•	Mitigation: Manager throttles Crawler; Scraper signals lag; pause/resume hooks.

E. Consistency of “one CSV”
	•	Risk: concurrent writers corrupt CSV.
	•	Mitigation: Single-writer Scraper process; atomic append with file locks or append-only WAL merged to CSV.

F. Crash recovery
	•	Risk: partial writes, frontier loss.
	•	Mitigation: Idempotent operations, append-only logs, periodic checkpoints, durable queue/frontier (fsync on commit).

G. Legal/ethical compliance
	•	Risk: violating policies.
	•	Mitigation: prominently configure User-Agent + contact, strict policy tests, logs showing robots compliance, slow/considerate crawl profile.

⸻

Step 4 — Iterative improvements (practical upgrades)
	1.	Two-phase discovery
	•	Phase A: Topics/Trending only → repo roots.
	•	Phase B: From repo root, bounded exploration to README, a few issues/PRs, and selective blob pages.
	•	Manager can increase depth gradually if no throttling.
	2.	Repository-aware caps
	•	Per-repo max pages, max issue/PR ids per window, max pagination depth (e.g., issues pages 1–2 only).
	3.	Soft “HTML budget”
	•	Manager sets MB/hour budget; Scraper enforces by delaying starts and smoothing throughput.
	4.	Adaptive policy learning
	•	Record paths that often 404 or redirect to disallowed; auto-update denylist to save cycles.
	5.	Content fingerprints for versioning
	•	For popular repos, store delta (optional) or keep only latest while logging metadata for all visits (to respect “HTML-only” constraint but manage disk).
	6.	Trajectory enrichment
	•	Add anchor text and lightweight DOM position hash (stable-ish) to improve link analysis.
	7.	Quality gates
	•	Before persisting: reject non-HTML content-types; cap HTML > X MB per page; log oversized pages separately.
	8.	Replay mode
	•	Scraper rebuilds CSV from stored HTML (useful for audits without re-fetching).

⸻

Step 5 — Questions (to finalize config)
	1.	Seeds: Do you want to start only from /topics and /trending (homepage only) plus a curated repo list, or include organization pages you provide?
	2.	Depth & caps: Default per-repo page cap (e.g., 30), issues/PR count limits (e.g., 10 each), and max pagination depth?
	3.	Rate profile: Initial global concurrency (1–2), max concurrency ceiling, and bytes/hour budget you’re comfortable with?
	4.	Proxies: Do you have a static pool (list) or shall we integrate a provider API? What’s the acceptable cost and geolocation preference?
	5.	Compression & storage: Store HTML as plain .html or compressed .html.zst? Target store_root capacity?
	6.	CSV schema: Are the metadata fields above sufficient? Any extra columns (e.g., repo_owner/repo_name, page_language)?
	7.	Run scheduling: Single long run or daily windows (to roll CSV & metrics and manage disk)?
	8.	Policy strictness: Do you want deny-by-default with a small allowlist (safest) or a broader allow with tight deny?
	9.	Audit & dashboard: Prefer console/TUI status, or a tiny local HTTP page with live metrics JSON?

⸻

Summary of module responsibilities (quick reference)
	•	Crawler
	•	Policy & robots gate → Frontier → Light fetch → Link extract → Discoveries Queue
	•	Outputs: edges.csv, crawler_metrics.csv, frontier state
	•	Scraper
	•	Consume discoveries → Full HTML fetch → Store HTML → Append to pages.csv
	•	Outputs: HTML files, pages.csv, scraper_metrics.csv
	•	Manager
	•	Config, Proxy pool, Rate control, Backpressure, Orchestration, Monitoring
	•	Outputs: run_metrics.csv, status JSON, run manifest

If you confirm the seeds, caps, and proxy setup, I’ll deliver a concrete runbook (configs, directory layout, CSV column spec, and failure playbooks) next.