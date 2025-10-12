Below is a complete developer guide to build the Scraper module that works with your Crawler.
It’s Python 3.11+, async, policy-safe, proxy-aware (via interface), and file-only (no SQL).
Includes architecture, functions, CLI, and code examples you can paste as a starting point.

Library refs: HTTPX (async client): https://www.python-httpx.org/ · Zstandard: https://python-zstandard.readthedocs.io/ · LMDB: https://lmdb.readthedocs.io/ · selectolax (optional sanitization): https://selectolax.readthedocs.io/

⸻

0) Goals & Contracts

Scraper does:
	•	Consumes Discoveries from the Crawler (JSONL items):
{timestamp, url, page_type, depth, referrer}
	•	Fetches each URL (HTML only), saves full HTML as loaded (optionally .zst).
	•	Writes one per-page CSV row with metadata + per-page metrics.
	•	Tracks global metrics (separate CSV recommended).
	•	Handles proxies via interface (no internal pool).
	•	Respects backpressure: pauses if spool > 10 GB (configurable).

Scraper does NOT:
	•	Re-decide policy (trusts Crawler’s allowlist+robots), but re-checks Content-Type.
	•	Write to Crawler’s frontier/metrics.

Artifacts:

workspace/
  state/
    page_index.lmdb        # url -> latest content hash / stored path / etag / last-modified
    spool_bookmark.json    # last processed file & byte offset (resume)
  spool/discoveries/       # (input; written by Crawler)
  store/html/aa/bb/<sha>.html.zst
  logs/pages.csv           # ONE CSV with per-page metadata (rows = pages)
  logs/scraper_metrics.csv # aggregated counters (optional but recommended)


⸻

1) Module Layout

scraper/
  __init__.py
  config.py            # load/validate config
  io/spool.py          # tailing JSONL consumer with bookmark
  io/csv_writer.py     # atomic append to pages.csv
  io/metrics.py        # counters -> scraper_metrics.csv
  io/storage.py        # content-addressed store (.html or .html.zst)
  index/page_index.py  # LMDB index: url -> {sha256, stored_path, etag, last_modified, ts}
  net/fetcher.py       # httpx async client + proxies + retries + conditional GET
  net/proxy_iface.py   # ProxyClient interface (Manager provides impl)
  util/limits.py       # concurrency + req/sec limiter
  util/bytes.py        # size-of-directory, etc.
  run.py               # CLI entrypoints (run/resume/replay/status)


⸻

2) Configuration (outside & inside)

How it looks from outside (CLI):

python -m scraper.run \
  --config config.yaml \
  --workspace /data/github_crawl \
  --mode run                 # run | resume | replay | status

config.yaml (example):

run_id: "2025-10-09T23-15Z"
workspace: "/data/github_crawl"

user_agent: "VI2025-Scraper/1.0 (+contact@email)"
accept_language: "en"
accept_encoding: "br, gzip"

limits:
  global_concurrency: 2
  req_per_sec: 1.0
  connect_timeout_ms: 4000
  read_timeout_ms: 15000
  total_timeout_ms: 25000
  max_retries: 3
  backoff_base_ms: 500
  backoff_cap_ms: 8000

spool:
  dir: "/data/github_crawl/spool/discoveries"
  max_backlog_gb: 10     # pause if spool directory exceeds this

store:
  root: "/data/github_crawl/store/html"
  compress: true         # .html.zst if true, plain .html if false
  permissions: 0o644

pages_csv:
  path: "/data/github_crawl/logs/pages.csv"

metrics:
  csv_path: "/data/github_crawl/logs/scraper_metrics.csv"
  flush_interval_sec: 10

index:
  db_path: "/data/github_crawl/state/page_index.lmdb"

bookmark:
  path: "/data/github_crawl/state/spool_bookmark.json"


⸻

3) Responsibilities & Key Functions

3.1 Spool Consumer (io/spool.py)

Purpose: Tail discovery JSONL files (written by Crawler), resume from last bookmark, yield items safely (even when file is still being written).

Key features:
	•	Bookmark stores {filename, offset}.
	•	Handles file rotation.
	•	Backpressure: can pause consuming when dir_size > max_backlog_gb.

Core API:

class SpoolReader:
    def __init__(self, dir_path: str, bookmark_path: str): ...
    async def iter_items(self):
        """
        Yields dict items parsed from JSONL in order.
        Maintains bookmark every N lines or on SIGINT.
        """

Example (simplified):

# scraper/io/spool.py
import os, json, asyncio
from pathlib import Path

class SpoolReader:
    def __init__(self, dir_path: str, bookmark_path: str):
        self.dir = Path(dir_path)
        self.bookmark_path = Path(bookmark_path)
        self.state = {"file": None, "offset": 0}

    def _load_bookmark(self):
        if self.bookmark_path.exists():
            self.state = json.loads(self.bookmark_path.read_text(encoding="utf-8"))

    def _save_bookmark(self):
        self.bookmark_path.parent.mkdir(parents=True, exist_ok=True)
        self.bookmark_path.write_text(json.dumps(self.state), encoding="utf-8")

    def _list_segment_files(self):
        return sorted([p for p in self.dir.glob("discoveries-*.jsonl")])

    async def iter_items(self):
        self._load_bookmark()
        files = self._list_segment_files()
        i = 0
        # Locate bookmark file index
        if self.state["file"]:
            for idx, f in enumerate(files):
                if f.name == self.state["file"]:
                    i = idx
                    break

        while True:
            files = self._list_segment_files()
            if i >= len(files):
                await asyncio.sleep(0.5)
                continue

            f = files[i]
            with f.open("r", encoding="utf-8") as fh:
                # Seek to offset
                fh.seek(self.state.get("offset", 0))
                while True:
                    pos = fh.tell()
                    line = fh.readline()
                    if not line:
                        # end of current file; move to next
                        self.state["file"] = f.name
                        self.state["offset"] = pos
                        self._save_bookmark()
                        break
                    try:
                        item = json.loads(line)
                        yield item
                    except Exception:
                        # Skip malformed lines, but advance offset
                        pass
            i += 1

Note: You can extend this to also tail the current active file (not just completed segments). The above prioritizes stability over real-time.

⸻

3.2 Storage (io/storage.py)

Purpose: Persist full HTML exactly as fetched. Uses content-addressed path by SHA-256. Supports .zst compression.

Key API:

class HtmlStore:
    def __init__(self, root: str, compress: bool, perm: int = 0o644): ...
    def save(self, html_bytes: bytes) -> dict:
        """
        Returns: { "sha256": str, "stored_path": str, "bytes": int }
        """

Example:

# scraper/io/storage.py
import os, hashlib
from pathlib import Path
try:
    import zstandard as zstd
except ImportError:
    zstd = None

class HtmlStore:
    def __init__(self, root: str, compress: bool, perm: int = 0o644):
        self.root = Path(root)
        self.compress = compress
        self.perm = perm
        self.root.mkdir(parents=True, exist_ok=True)

    def save(self, html_bytes: bytes) -> dict:
        sha = hashlib.sha256(html_bytes).hexdigest()
        sub = self.root / sha[:2] / sha[2:4]
        sub.mkdir(parents=True, exist_ok=True)
        if self.compress:
            assert zstd is not None, "zstandard required for compress=true"
            path = sub / f"{sha}.html.zst"
            cctx = zstd.ZstdCompressor(level=10)
            data = cctx.compress(html_bytes)
        else:
            path = sub / f"{sha}.html"
            data = html_bytes

        tmp = f"{path}.tmp"
        with open(tmp, "wb") as fh:
            fh.write(data)
        os.chmod(tmp, self.perm)
        os.replace(tmp, path)  # atomic
        return {"sha256": sha, "stored_path": str(path), "bytes": len(html_bytes)}


⸻

3.3 Page Index (index/page_index.py)

Purpose: Fast lookup of latest known page state by canonical URL to enable conditional requests and 304 handling.

Fields stored:
	•	url, sha256, stored_path, etag, last_modified, timestamp

API:

class PageIndex:
    def __init__(self, db_path: str): ...
    def get(self, url: str) -> dict | None
    def put(self, rec: dict) -> None

Example:

# scraper/index/page_index.py
import json, lmdb, os
from pathlib import Path

class PageIndex:
    def __init__(self, db_path: str):
        Path(os.path.dirname(db_path)).mkdir(parents=True, exist_ok=True)
        self.env = lmdb.open(db_path, map_size=2*1024**3, subdir=True, max_dbs=1, lock=True)
        self.db = self.env.open_db(b"page_index")

    def get(self, url: str) -> dict | None:
        with self.env.begin(db=self.db) as txn:
            val = txn.get(url.encode("utf-8"))
            return json.loads(val) if val else None

    def put(self, rec: dict) -> None:
        with self.env.begin(write=True, db=self.db) as txn:
            txn.put(rec["url"].encode("utf-8"), json.dumps(rec).encode("utf-8"))


⸻

3.4 Fetcher (net/fetcher.py)

Purpose: GET HTML with timeouts, retries, conditional headers, and proxy via interface.

Proxy interface (same contract as Crawler):

# scraper/net/proxy_iface.py
class ProxyClient:
    async def pick(self) -> dict | None: ...
    async def report_result(self, proxy_id: str | None, status: int | None, throttled: bool): ...

Fetcher API:

class Fetcher:
    async def fetch_html(self, url: str, prev: dict | None) -> dict:
        """
        prev = page_index.get(url) or None
        Returns:
          { ok, status, html_bytes?, headers, latency_ms, proxy_id, not_html?, not_modified? }
        """

Example:

# scraper/net/fetcher.py
import httpx, asyncio, time, random
from urllib.parse import urlsplit

class Fetcher:
    def __init__(self, cfg, proxy_client, limiter):
        self.cfg = cfg
        self.proxy_client = proxy_client
        self.limiter = limiter
        self.client = httpx.AsyncClient(http2=True, timeout=httpx.Timeout(
            connect=cfg.limits["connect_timeout_ms"]/1000,
            read=cfg.limits["read_timeout_ms"]/1000,
            write=cfg.limits["read_timeout_ms"]/1000,
            pool=None
        ))
        self.base_headers = {
            "User-Agent": cfg.user_agent,
            "Accept": "text/html,application/xhtml+xml",
            "Accept-Language": cfg.accept_language,
            "Accept-Encoding": cfg.accept_encoding,
        }

    async def fetch_html(self, url: str, prev: dict | None) -> dict:
        host = urlsplit(url).netloc.lower()
        retries = 0
        while True:
            proxy = await self.proxy_client.pick() if self.proxy_client else None
            headers = dict(self.base_headers)
            if prev:
                if etag := prev.get("etag"):
                    headers["If-None-Match"] = etag
                if lm := prev.get("last_modified"):
                    headers["If-Modified-Since"] = lm

            async with self.limiter.acquire(host):
                try:
                    resp = await self.client.get(url, headers=headers, proxies=proxy)
                except httpx.RequestError as e:
                    if retries >= self.cfg.limits["max_retries"]:
                        return {"ok": False, "error": str(e), "retries": retries}
                    await asyncio.sleep(self._backoff(retries)); retries += 1; continue

            throttled = (resp.status_code == 429) or ("Retry-After" in resp.headers)
            if self.proxy_client:
                await self.proxy_client.report_result(proxy.get("proxy_id") if proxy else None,
                                                      resp.status_code, throttled)

            if resp.status_code == 304:
                return {"ok": True, "status": 304, "not_modified": True, "headers": dict(resp.headers),
                        "latency_ms": resp.elapsed.total_seconds() * 1000, "proxy_id": proxy.get("proxy_id") if proxy else None}

            if resp.status_code >= 500 or resp.status_code in (408, 429):
                if retries >= self.cfg.limits["max_retries"]:
                    return {"ok": False, "status": resp.status_code, "retries": retries}
                await asyncio.sleep(self._retry_after_or_backoff(resp, retries)); retries += 1; continue

            ctype = resp.headers.get("Content-Type", "")
            if "text/html" not in ctype:
                return {"ok": False, "status": resp.status_code, "not_html": True}

            return {
                "ok": True,
                "status": resp.status_code,
                "html_bytes": resp.content,        # save as-is
                "headers": dict(resp.headers),
                "latency_ms": resp.elapsed.total_seconds() * 1000,
                "proxy_id": proxy.get("proxy_id") if proxy else None
            }

    def _retry_after_or_backoff(self, resp, retries):
        ra = resp.headers.get("Retry-After")
        if ra:
            try:
                return float(ra)
            except:
                pass
        return self._backoff(retries)

    def _backoff(self, retries):
        base = self.cfg.limits["backoff_base_ms"] / 1000
        cap  = self.cfg.limits["backoff_cap_ms"]  / 1000
        return min(cap, base * (2 ** retries)) + random.random() * 0.25


⸻

3.5 Concurrency & Rate Limits (util/limits.py)

# scraper/util/limits.py
import asyncio
from collections import defaultdict
from contextlib import asynccontextmanager

class RateLimiter:
    def __init__(self, global_concurrency: int):
        self.global_sem = asyncio.Semaphore(global_concurrency)

    @asynccontextmanager
    async def acquire(self, host: str):
        async with self.global_sem:
            yield

(If you need per-host caps or token buckets for req/sec, extend here. Start conservative.)

⸻

3.6 Pages CSV Writer (io/csv_writer.py)

Purpose: One CSV combining metadata + per-page metrics.

Columns (recommended):

timestamp, url, page_type, depth, referrer,
http_status, content_type, encoding,
content_sha256, content_bytes, stored_path,
etag, last_modified, fetch_latency_ms, retries,
proxy_id

Example:

# scraper/io/csv_writer.py
import csv, os, time
from pathlib import Path

class PagesCsv:
    def __init__(self, path: str):
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self._fh = open(self.path, "a", newline="", encoding="utf-8")
        self._w = csv.writer(self._fh)
        if self._fh.tell() == 0:
            self._w.writerow([
                "timestamp","url","page_type","depth","referrer",
                "http_status","content_type","encoding",
                "content_sha256","content_bytes","stored_path",
                "etag","last_modified","fetch_latency_ms","retries","proxy_id"
            ])

    def append(self, row: dict):
        self._w.writerow([
            row.get("timestamp"), row.get("url"), row.get("page_type"), row.get("depth"), row.get("referrer"),
            row.get("http_status"), row.get("content_type"), row.get("encoding"),
            row.get("content_sha256"), row.get("content_bytes"), row.get("stored_path"),
            row.get("etag"), row.get("last_modified"), row.get("fetch_latency_ms"), row.get("retries"), row.get("proxy_id")
        ])
        self._fh.flush()

    def close(self):
        self._fh.close()


⸻

3.7 Metrics Writer (io/metrics.py)

Purpose: Aggregate global counters and flush periodically to scraper_metrics.csv.

Suggested keys:
	•	pages_fetched, pages_saved, bytes_saved,
	•	status_2xx, status_3xx, status_4xx, status_5xx,
	•	not_html, not_modified_304, retries_total, proxy_throttle_events.

Example:

# scraper/io/metrics.py
import csv, time
from collections import Counter
from pathlib import Path

class Metrics:
    def __init__(self, csv_path: str, flush_interval_sec: int = 10):
        self.csv_path = Path(csv_path)
        self.csv_path.parent.mkdir(parents=True, exist_ok=True)
        self.flush_interval = flush_interval_sec
        self.c = Counter()
        self.last = time.time()

    def inc(self, key: str, n: int = 1): self.c[key] += n

    def maybe_flush(self):
        now = time.time()
        if now - self.last >= self.flush_interval:
            with open(self.csv_path, "a", newline="", encoding="utf-8") as fh:
                w = csv.writer(fh)
                ts = int(now)
                for k, v in self.c.items():
                    w.writerow([ts, k, v])
            self.c.clear()
            self.last = now


⸻

4) Orchestrator (Scraper) — How it works inside

Steps per discovery item:
	1.	Read {timestamp, url, page_type, depth, referrer} from SpoolReader.
	2.	Lookup prev = PageIndex.get(url) for ETag/Last-Modified.
	3.	Fetch with Fetcher.fetch_html(url, prev).
	4.	If 304 → reuse existing stored_path & sha256 from index; write CSV row with http_status=304.
	5.	If 2xx HTML → HtmlStore.save(html_bytes) → write pages.csv row with metadata.
	6.	Update PageIndex with latest {url, sha256, stored_path, etag, last_modified, timestamp}.
	7.	Metrics update; flush periodically.

Backpressure:
	•	If spool_dir_size > max_backlog_gb, pause consuming for a while (sleep & log) until backlog shrinks (Manager/Scraper coordination).

⸻

5) CLI Entrypoint (run.py)

Commands:
	•	scrape run – start from scratch (ignores bookmark if not present).
	•	scrape resume – continue from spool_bookmark.json.
	•	scrape replay – rebuild pages.csv from stored HTML (no network calls).
	•	scrape status – print counts, spool size, last processed file/offset.

Example (simplified):

# scraper/run.py
import asyncio, argparse, time, os
from .config import ScraperConfig
from .io.spool import SpoolReader
from .io.storage import HtmlStore
from .io.csv_writer import PagesCsv
from .io.metrics import Metrics
from .index.page_index import PageIndex
from .net.fetcher import Fetcher
from .net.proxy_iface import ProxyClient
from .util.limits import RateLimiter

def _dir_size_bytes(path: str) -> int:
    total = 0
    for root, _, files in os.walk(path):
        for f in files:
            total += os.path.getsize(os.path.join(root, f))
    return total

class DummyProxy(ProxyClient):
    async def pick(self): return None
    async def report_result(self, proxy_id, status, throttled): pass

async def run_scraper(cfg: ScraperConfig, mode: str):
    # Setup
    spool = SpoolReader(cfg.spool["dir"], cfg.bookmark["path"])
    store = HtmlStore(cfg.store["root"], cfg.store["compress"], cfg.store["permissions"])
    pages = PagesCsv(cfg.pages_csv["path"])
    metrics = Metrics(cfg.metrics["csv_path"], cfg.metrics["flush_interval_sec"])
    index = PageIndex(cfg.index["db_path"])
    limiter = RateLimiter(cfg.limits["global_concurrency"])
    fetcher = Fetcher(cfg, proxy_client=DummyProxy(), limiter=limiter)

    async for item in spool.iter_items():
        # Backpressure
        if (_dir_size_bytes(cfg.spool["dir"]) / (1024**3)) > cfg.spool["max_backlog_gb"]:
            print("Backpressure: spool too large; pausing...")
            await asyncio.sleep(5)
            continue

        url = item["url"]
        page_type = item.get("page_type")
        depth = item.get("depth")
        referrer = item.get("referrer")

        prev = index.get(url)
        res = await fetcher.fetch_html(url, prev)

        if not res.get("ok"):
            metrics.inc("fetch_fail"); metrics.maybe_flush(); continue
        metrics.inc("pages_fetched")

        ts = int(time.time())
        status = res["status"]
        headers = res.get("headers", {})
        ctype = headers.get("Content-Type", "")
        enc = headers.get("Content-Encoding") or ""

        if res.get("not_modified"):
            # reuse previous
            row = {
                "timestamp": ts, "url": url, "page_type": page_type, "depth": depth, "referrer": referrer,
                "http_status": status, "content_type": ctype, "encoding": enc,
                "content_sha256": prev.get("sha256") if prev else "", "content_bytes": 0,
                "stored_path": prev.get("stored_path") if prev else "",
                "etag": headers.get("ETag") or (prev.get("etag") if prev else ""),
                "last_modified": headers.get("Last-Modified") or (prev.get("last_modified") if prev else ""),
                "fetch_latency_ms": int(res.get("latency_ms", 0)), "retries": res.get("retries", 0),
                "proxy_id": res.get("proxy_id")
            }
            pages.append(row)
            metrics.inc("not_modified_304")
        else:
            if res.get("not_html"):
                metrics.inc("not_html"); metrics.maybe_flush(); continue
            html_bytes = res["html_bytes"]
            saved = store.save(html_bytes)
            # index update
            rec = {
                "url": url,
                "sha256": saved["sha256"],
                "stored_path": saved["stored_path"],
                "etag": headers.get("ETag"),
                "last_modified": headers.get("Last-Modified"),
                "timestamp": ts
            }
            index.put(rec)
            # CSV row
            row = {
                "timestamp": ts, "url": url, "page_type": page_type, "depth": depth, "referrer": referrer,
                "http_status": status, "content_type": ctype, "encoding": enc,
                "content_sha256": saved["sha256"], "content_bytes": saved["bytes"],
                "stored_path": saved["stored_path"],
                "etag": rec["etag"], "last_modified": rec["last_modified"],
                "fetch_latency_ms": int(res.get("latency_ms", 0)), "retries": res.get("retries", 0),
                "proxy_id": res.get("proxy_id")
            }
            pages.append(row)
            metrics.inc("pages_saved"); metrics.inc("bytes_saved", saved["bytes"])

        # status buckets
        if 200 <= status < 300: metrics.inc("status_2xx")
        elif 300 <= status < 400: metrics.inc("status_3xx")
        elif 400 <= status < 500: metrics.inc("status_4xx")
        else: metrics.inc("status_5xx")

        metrics.maybe_flush()

    pages.close()
    metrics.maybe_flush()

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--config", required=True)
    ap.add_argument("--workspace", required=False)
    ap.add_argument("--mode", choices=["run","resume","replay","status"], default="run")
    args = ap.parse_args()
    cfg = ScraperConfig.from_yaml(args.config)
    asyncio.run(run_scraper(cfg, args.mode))

if __name__ == "__main__":
    main()


⸻

6) How it works from Outside (Developer UX)
	•	Start scraping:
python -m scraper.run --config config.yaml --mode run
	•	Resume after interruption:
python -m scraper.run --config config.yaml --mode resume
	•	Replay (rebuild CSV from stored HTML; no network):
python -m scraper.run --config config.yaml --mode replay (implement as needed)
	•	Status:
python -m scraper.run --config config.yaml --mode status (print counters, spool size, last bookmark)

Contract with Crawler:
	•	Crawler writes discoveries-*.jsonl to spool/discoveries/ (append-only; rotates).
	•	Scraper tails these files (with bookmark) and never writes to the spool.
	•	Page types to expect: repo_root, blob, issues, pull.

⸻

7) Edge Cases & Safeguards
	•	Non-HTML responses → skip, record metric not_html.
	•	Large pages → optionally cap size (config) to avoid pathological downloads.
	•	304 Not Modified → write CSV row with old sha256/stored_path + current headers.
	•	Duplicate content (same SHA across different URLs) → stored once, referenced many times in CSV.
	•	Disk full / permissions → atomic writes with .tmp and os.replace; log errors and continue.
	•	Proxy throttling (429/Retry-After) → report via ProxyClient; rely on Manager to rotate/cooldown.
	•	Backpressure → pause when spool exceeds 10 GB (configurable).
	•	Graceful stop → on SIGINT/SIGTERM, flush CSV, save bookmark, close LMDB.

⸻

8) Implementation Checklist (quick)
	•	Parse config, create workspace paths.
	•	Initialize SpoolReader, HtmlStore, PagesCsv, Metrics, PageIndex, Fetcher, Limiter.
	•	Loop over SpoolReader.iter_items(), apply backpressure checks.
	•	Conditional GET using ETag/Last-Modified from PageIndex.
	•	On 2xx HTML → save HTML (as-is), write CSV, update index.
	•	On 304 → reuse index info, write CSV.
	•	Track metrics and flush periodically.
	•	Bookmark progress regularly.
	•	Provide resume, replay, status modes.

⸻

9) Notes & References
	•	HTTPX (async, timeouts, client options): https://www.python-httpx.org/async/
	•	Zstandard (fast compression): https://python-zstandard.readthedocs.io/
	•	LMDB (embedded KV): https://lmdb.readthedocs.io/
	•	selectolax (optional HTML post-sanitization): https://selectolax.readthedocs.io/

⸻

If you want, I can also:
	•	add a ready config.yaml with safe defaults,
	•	provide a minimal ProxyClient that talks to your Manager,
	•	and include a “status dashboard” (tiny HTTP server) that reads pages.csv + scraper_metrics.csv for live monitoring.