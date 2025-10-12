Below is a comprehensive developer guide to implement the Crawler module.
It’s written for Python, assumes async I/O, and follows your constraints:
	•	On-domain only (github.com).
	•	Strict robots.txt + allow/deny policy (deny-by-default).
	•	Trajectory map (crawl graph).
	•	Rich metrics (CSV).
	•	Proxy-aware (ready for dynamic switching via Manager).
	•	Separation of concerns (Crawler ≠ Scraper; connected via a disk-backed “Discoveries Queue”).

⸻

0) High-level scope

Crawler = Discovery Engine
	•	Input: seeds (e.g., /topics, /trending, curated repo list) + config.
	•	Output:
	•	Discoveries Queue → items for Scraper: {url, page_type, depth, referrer, discovered_at}
	•	Trajectory map edges.csv
	•	Crawler metrics crawler_metrics.csv
	•	Frontier state (resumable)
	•	Never saves full pages (that’s Scraper’s job). It fetches just enough HTML to discover links.

⸻

1) Folder layout

crawler/
  __init__.py
  config.py
  policy.py
  robots_cache.py
  url_tools.py
  frontier/
    __init__.py
    frontier.py         # LMDB-backed priority frontier
    dedup.py            # Bloom filter for seen URLs
  net/
    fetcher.py          # Async HTTP fetch with retries
    proxy_client.py     # Proxy interface (manager-provided)
    limiter.py          # Global & per-host rate limiting
  parse/
    extractor.py        # HTML link extraction + page-type classification
  io/
    discoveries.py      # Disk-backed queue (writer)
    trajectory.py       # edges.csv writer (append-only)
    metrics.py          # CSV metrics sink
  run.py                # Crawler runner (public API / CLI)


⸻

2) Public API (outside view)

2.1 CLI (suggested)

python -m crawler.run \
  --config config.yaml \
  --seeds seeds.txt \
  --workspace /data/github_crawl

2.2 Programmatic

from crawler.run import CrawlerService, CrawlerConfig

cfg = CrawlerConfig.from_yaml("config.yaml")
svc = CrawlerService(cfg)
await svc.start(seeds=["https://github.com/topics", "https://github.com/trending"])
await svc.run()     # runs until frontier empty OR stop condition
await svc.stop()

Contract to Scraper:
Crawler writes Discoveries into workspace/spool/discoveries/*.jsonl (rotated).
Scraper consumes those files independently.

⸻

3) Configuration (config.yaml)

run_id: "2025-10-09T23-15Z"
workspace: "/data/github_crawl"

user_agent: "VI2025-Crawler/1.0 (+contact@email)"
accept_language: "en"
accept_encoding: "br, gzip"

robots:
  user_agent: "VI2025-Crawler"
  cache_ttl_sec: 86400

scope:
  allowed_hosts: ["github.com"]
  denied_subdomains: ["api.github.com", "raw.githubusercontent.com", "gist.github.com"]
  # Deny-by-default, then allow explicit patterns:
  allow_patterns:
    - "^https://github\\.com/topics(?:/[^/?#]+)?$"
    - "^https://github\\.com/trending$"
    - "^https://github\\.com/[^/]+/[^/]+/?$"              # repo root
    - "^https://github\\.com/[^/]+/[^/]+/blob/[^?#]+$"     # blob view
    - "^https://github\\.com/[^/]+/[^/]+/(issues|pull)/?\\d*$"  # issue/pr
  deny_patterns:
    - "/search"
    - "/tree/"
    - "/commits/"
    - "/graphs/"
    - "/compare/"
    - "/archive/"
    - "\\?q="
    - "\\?tab="
    - "\\?since="
  content_types: ["text/html"]

limits:
  global_concurrency: 2
  per_host_concurrency: 2
  req_per_sec: 1.0
  connect_timeout_ms: 4000
  read_timeout_ms: 12000
  total_timeout_ms: 20000
  max_retries: 3
  backoff_base_ms: 500
  backoff_cap_ms: 8000

caps:
  per_repo_max_pages: 30
  per_repo_max_issues: 10
  per_repo_max_prs: 10
  max_depth: 4

frontier:
  db_path: "/data/github_crawl/state/frontier.lmdb"
  bloom_path: "/data/github_crawl/state/seen.bloom"
  bloom_capacity: 50000000
  bloom_error_rate: 0.001

spool:
  discoveries_dir: "/data/github_crawl/spool/discoveries"
  rotate_every_mb: 100

metrics:
  csv_path: "/data/github_crawl/logs/crawler_metrics.csv"
  flush_interval_sec: 10

trajectory:
  csv_path: "/data/github_crawl/logs/edges.csv"

Tip: Keep deny-by-default; only open what you’re sure is allowed.

⸻

4) Core components — responsibilities & key functions

4.1 config.py
	•	Responsibility: load/validate config; provide typed access throughout crawler.

from dataclasses import dataclass
from pathlib import Path
import yaml

@dataclass
class CrawlerConfig:
    run_id: str
    workspace: str
    user_agent: str
    accept_language: str
    accept_encoding: str
    # ... nested dicts turned into sub-dataclasses for robots, scope, limits, etc.

    @classmethod
    def from_yaml(cls, path: str) -> "CrawlerConfig":
        with open(path, "r", encoding="utf-8") as fh:
            raw = yaml.safe_load(fh)
        # validate & coerce
        return cls(**raw)


⸻

4.2 url_tools.py
	•	Responsibility: canonicalization and host/scheme checks.

Key functions:
	•	canonicalize(url: str) -> str
	•	is_same_host(url: str, host: str) -> bool
	•	strip_fragment_and_disallowed_params(url: str) -> str

from urllib.parse import urlsplit, urlunsplit, parse_qsl, urlencode

ALLOWED_QUERY_KEYS = set()  # drop all unless whitelisted

def canonicalize(url: str) -> str:
    s = urlsplit(url)
    scheme = s.scheme.lower()
    netloc = s.netloc.lower()
    path = s.path or "/"
    # strip fragment
    fragment = ""
    # drop (most) query params:
    q = [(k, v) for k, v in parse_qsl(s.query, keep_blank_values=True) if k in ALLOWED_QUERY_KEYS]
    query = urlencode(sorted(q))
    return urlunsplit((scheme, netloc, path, query, fragment))

def is_same_host(url: str, host: str) -> bool:
    return urlsplit(url).netloc.lower() == host

def strip_fragment_and_disallowed_params(url: str) -> str:
    return canonicalize(url)


⸻

4.3 robots_cache.py
	•	Responsibility: cache and evaluate robots rules.

Key functions:
	•	is_allowed(url: str) -> bool
	•	crawl_delay(host: str) -> float | None

Use a robust robots parser (e.g., reppy) if available; fallback to urllib.robotparser otherwise.

import time
from urllib.parse import urlsplit
from functools import lru_cache
# Placeholder: wrap your library of choice here.

class RobotsCache:
    def __init__(self, user_agent: str, ttl_sec: int):
        self.user_agent = user_agent
        self.ttl_sec = ttl_sec
        self._cache = {}  # host -> (fetched_at, parser)

    async def _fetch_robots(self, host: str):
        # GET https://{host}/robots.txt with conservative timeouts
        # Build parser object, store into cache with timestamp
        ...

    async def is_allowed(self, url: str) -> bool:
        host = urlsplit(url).netloc.lower()
        entry = self._cache.get(host)
        if not entry or (time.time() - entry[0]) > self.ttl_sec:
            await self._fetch_robots(host)
            entry = self._cache.get(host)
        parser = entry[1]
        return parser.can_fetch(self.user_agent, url)

    async def crawl_delay(self, host: str) -> float | None:
        # if parser supports it, return crawl-delay for user-agent
        ...


⸻

4.4 policy.py
	•	Responsibility: final gate before a URL is queued/fetched.

Key functions:
	•	classify(url: str) -> str | None → returns page_type or None.
	•	allowed(url: str) -> tuple[bool, str] → (True/False, reason).

import re
from urllib.parse import urlsplit
from .url_tools import canonicalize, is_same_host

class UrlPolicy:
    def __init__(self, cfg, robots: "RobotsCache"):
        self.cfg = cfg
        self.robots = robots
        self.allow_patterns = [re.compile(p) for p in cfg.scope["allow_patterns"]]
        self.deny_patterns  = [re.compile(p) for p in cfg.scope["deny_patterns"]]
        self.allowed_hosts  = set(cfg.scope["allowed_hosts"])
        self.denied_subdomains = set(cfg.scope["denied_subdomains"])

    async def allowed(self, url: str) -> tuple[bool, str]:
        url = canonicalize(url)
        host = urlsplit(url).netloc.lower()
        if host not in self.allowed_hosts:
            return False, "host_not_allowed"
        if host in self.denied_subdomains:
            return False, "subdomain_denied"
        for d in self.deny_patterns:
            if d.search(url):
                return False, "denied_pattern"
        if not any(p.match(url) for p in self.allow_patterns):
            return False, "not_in_allowlist"
        if not await self.robots.is_allowed(url):
            return False, "robots_disallow"
        return True, "ok"

    def classify(self, url: str) -> str | None:
        # Minimal examples:
        if re.match(r"^https://github\.com/topics(?:/[^/?#]+)?$", url):
            return "topic"
        if re.match(r"^https://github\.com/trending$", url):
            return "trending"
        if re.match(r"^https://github\.com/[^/]+/[^/]+/?$", url):
            return "repo_root"
        if re.match(r"^https://github\.com/[^/]+/[^/]+/blob/[^?#]+$", url):
            return "blob"
        m = re.match(r"^https://github\.com/[^/]+/[^/]+/(issues|pull)/?\d*$", url)
        if m:
            return m.group(1)  # "issues" or "pull"
        return None


⸻

4.5 frontier/frontier.py (LMDB-backed priority queue)
	•	Responsibility: persistent, resumable frontier with de-dup.

Key functions:
	•	push(url, priority, depth, referrer, discovered_at)
	•	pop_batch(n) → list of items (respect global/per-host limits outside)
	•	mark_done(url) / requeue(item, backoff_ms)
	•	seen(url) (delegates to Bloom filter)

Data model:
	•	Key: priority:ts:rand:canonical_url → Value: JSON meta
	•	Put lower numeric priority for breadth-first; include timestamp to stabilize ordering.

import json, time, os, random
import lmdb
from .dedup import SeenSet

class Frontier:
    def __init__(self, db_path: str, seen: SeenSet):
        os.makedirs(os.path.dirname(db_path), exist_ok=True)
        self.env = lmdb.open(db_path, map_size=8*1024**3, subdir=True, max_dbs=1, lock=True)
        self.db = self.env.open_db(b"frontier")
        self.seen = seen

    def _key(self, priority: int, url: str) -> bytes:
        ts = int(time.time() * 1000)
        rnd = random.getrandbits(16)
        return f"{priority:06d}:{ts:013d}:{rnd:04x}:{url}".encode("utf-8")

    def push(self, item: dict) -> bool:
        url = item["url"]
        if self.seen.check_and_add(url):
            return False
        key = self._key(item.get("priority", 100), item["url"])
        val = json.dumps(item).encode("utf-8")
        with self.env.begin(write=True, db=self.db) as txn:
            txn.put(key, val)
        return True

    def pop_batch(self, n: int) -> list[dict]:
        out = []
        with self.env.begin(write=True, db=self.db) as txn:
            with txn.cursor() as cur:
                while len(out) < n and cur.first():
                    key = cur.key()
                    val = cur.value()
                    cur.delete()  # remove from frontier
                    out.append(json.loads(val))
        return out


⸻

4.6 frontier/dedup.py (Bloom filter)
	•	Responsibility: fast URL seen-set.

import mmh3
from array import array

class SeenSet:
    def __init__(self, capacity=50_000_000, hashes=7):
        self.bits = array('B', b'\x00') * (capacity // 8 + 1)
        self.capacity = capacity
        self.hashes = hashes

    def _indexes(self, s: str):
        h1 = mmh3.hash64(s, signed=False)[0]
        h2 = mmh3.hash64(s, signed=False)[1]
        for i in range(self.hashes):
            yield (h1 + i*h2) % self.capacity

    def check_and_add(self, s: str) -> bool:
        seen = True
        idxs = list(self._indexes(s))
        for idx in idxs:
            byte, bit = divmod(idx, 8)
            if not (self.bits[byte] >> bit) & 1:
                seen = False
        # set bits
        for idx in idxs:
            byte, bit = divmod(idx, 8)
            self.bits[byte] |= (1 << bit)
        return seen

(You can swap this for pybloomfiltermmap3 for mmap-backed persistence.)

⸻

4.7 net/limiter.py
	•	Responsibility: global & per-host tokens.

import asyncio
from collections import defaultdict
from contextlib import asynccontextmanager

class RateLimiter:
    def __init__(self, global_concurrency: int, per_host_concurrency: int):
        self.global_sem = asyncio.Semaphore(global_concurrency)
        self.host_sems = defaultdict(lambda: asyncio.Semaphore(per_host_concurrency))

    @asynccontextmanager
    async def acquire(self, host: str):
        async with self.global_sem, self.host_sems[host]:
            yield


⸻

4.8 net/proxy_client.py
	•	Responsibility: unified proxy handle (Manager can hot-swap pools).

class ProxyClient:
    """Manager-provided. Here we define the interface used by Fetcher."""
    async def pick(self) -> dict | None:
        """
        Returns proxy descriptor, e.g.:
        {"proxy_id": "p1", "http": "http://user:pass@host:port",
                           "https": "http://user:pass@host:port"}
        or None for direct.
        """
        ...

    async def report_result(self, proxy_id: str | None, status: int | None, throttled: bool):
        """Called after every request to update proxy health."""
        ...


⸻

4.9 net/fetcher.py (HTTPX async + retries)
	•	Responsibility: perform GET with timeouts, retries, backoff, optional proxy.

import asyncio, time, httpx, random
from urllib.parse import urlsplit

class Fetcher:
    def __init__(self, cfg, proxy_client, limiter):
        self.cfg = cfg
        self.proxy_client = proxy_client
        self.limiter = limiter
        self.base_headers = {
            "User-Agent": cfg.user_agent,
            "Accept": "text/html,application/xhtml+xml",
            "Accept-Language": cfg.accept_language,
            "Accept-Encoding": cfg.accept_encoding,
        }
        self.client = httpx.AsyncClient(http2=True, timeout=httpx.Timeout(
            connect=cfg.limits["connect_timeout_ms"]/1000,
            read=cfg.limits["read_timeout_ms"]/1000,
            write=cfg.limits["read_timeout_ms"]/1000,
            pool=None
        ))

    async def get_html(self, url: str) -> dict:
        host = urlsplit(url).netloc.lower()
        retries = 0
        proxy_desc = None
        while True:
            async with self.limiter.acquire(host):
                proxy_desc = await self.proxy_client.pick() if self.proxy_client else None
                try:
                    resp = await self.client.get(
                        url,
                        headers=self.base_headers,
                        proxies=proxy_desc if proxy_desc else None,
                    )
                except httpx.RequestError as e:
                    if retries >= self.cfg.limits["max_retries"]:
                        return {"ok": False, "error": str(e), "retries": retries, "proxy": proxy_desc}
                    await asyncio.sleep(self._backoff(retries))
                    retries += 1
                    continue

                throttled = (resp.status_code == 429) or ("Retry-After" in resp.headers)
                if self.proxy_client:
                    await self.proxy_client.report_result(proxy_desc.get("proxy_id") if proxy_desc else None,
                                                          resp.status_code, throttled)

                if resp.status_code >= 500 or resp.status_code in (408, 429):
                    if retries >= self.cfg.limits["max_retries"]:
                        return {"ok": False, "status": resp.status_code, "retries": retries, "proxy": proxy_desc}
                    await asyncio.sleep(self._retry_after_or_backoff(resp, retries))
                    retries += 1
                    continue

                ctype = resp.headers.get("Content-Type", "")
                if "text/html" not in ctype:
                    return {"ok": False, "status": resp.status_code, "not_html": True, "proxy": proxy_desc}

                return {
                    "ok": True,
                    "status": resp.status_code,
                    "text": resp.text,
                    "headers": dict(resp.headers),
                    "latency_ms": resp.elapsed.total_seconds() * 1000,
                    "proxy": proxy_desc,
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
        delay = min(cap, base * (2 ** retries))
        return delay + random.random() * 0.25


⸻

4.10 parse/extractor.py
	•	Responsibility: extract links + classify page type.

from selectolax.parser import HTMLParser
from urllib.parse import urljoin
from ..url_tools import canonicalize

class Extractor:
    def __init__(self, base_url: str):
        self.base_url = base_url

    def extract_links(self, html: str) -> list[str]:
        root = HTMLParser(html)
        out = []
        for a in root.css("a[href]"):
            href = a.attributes.get("href")
            if not href: 
                continue
            abs_url = urljoin(self.base_url, href)
            out.append(canonicalize(abs_url))
        return out

(Classification is in policy.py to keep one source of truth.)

⸻

4.11 io/trajectory.py
	•	Responsibility: append edges (referrer → target).

import csv, os, time
from pathlib import Path

class TrajectoryWriter:
    def __init__(self, csv_path: str):
        Path(os.path.dirname(csv_path)).mkdir(parents=True, exist_ok=True)
        self.csv_path = csv_path
        self._fh = open(csv_path, "a", newline="", encoding="utf-8")
        self._w = csv.writer(self._fh)
        # optional header:
        # self._w.writerow(["ts", "src", "dst", "depth", "anchor"])

    def append(self, src: str, dst: str, depth: int, anchor: str | None = None):
        self._w.writerow([int(time.time()), src, dst, depth, anchor or ""])
        self._fh.flush()

    def close(self):
        self._fh.close()


⸻

4.12 io/metrics.py
	•	Responsibility: track counters; flush to CSV.

import csv, os, time
from pathlib import Path
from collections import Counter

class Metrics:
    def __init__(self, csv_path: str, flush_interval_sec: int = 10):
        Path(os.path.dirname(csv_path)).mkdir(parents=True, exist_ok=True)
        self.csv_path = csv_path
        self.flush_interval = flush_interval_sec
        self.c = Counter()
        self.last_flush = time.time()

    def inc(self, key: str, n: int = 1):
        self.c[key] += n

    def maybe_flush(self):
        now = time.time()
        if now - self.last_flush >= self.flush_interval:
            with open(self.csv_path, "a", newline="", encoding="utf-8") as fh:
                w = csv.writer(fh)
                for k, v in self.c.items():
                    w.writerow([int(now), k, v])
            self.c.clear()
            self.last_flush = now


⸻

4.13 io/discoveries.py (disk-backed queue; writer only)
	•	Responsibility: write discoveries to rotating JSONL files for Scraper.

import json, os, time
from pathlib import Path

class DiscoveriesWriter:
    def __init__(self, dir_path: str, rotate_every_mb: int = 100):
        self.dir = Path(dir_path)
        self.dir.mkdir(parents=True, exist_ok=True)
        self.rotate_every = rotate_every_mb * 1024 * 1024
        self.current = None
        self.bytes = 0
        self._open_new()

    def _open_new(self):
        ts = time.strftime("%Y%m%d-%H%M%S")
        name = f"discoveries-{ts}.jsonl"
        self.current = open(self.dir / name, "a", encoding="utf-8")
        self.bytes = 0

    def append(self, item: dict):
        line = json.dumps(item, ensure_ascii=False) + "\n"
        self.current.write(line)
        self.current.flush()
        self.bytes += len(line.encode("utf-8"))
        if self.bytes >= self.rotate_every:
            self.current.close()
            self._open_new()

    def close(self):
        if self.current:
            self.current.close()


⸻

5) Crawler orchestration (run.py)

Main loop:
	1.	Load config & seeds.
	2.	Initialize RobotsCache, UrlPolicy, Frontier, Fetcher, Extractor, Metrics, Trajectory, DiscoveriesWriter.
	3.	Seed Frontier with (url, depth=0, priority).
	4.	Loop:
	•	Pop batch from Frontier.
	•	For each item:
	•	Fetch (respect limiter + proxy).
	•	If OK HTML: Extract links, for each link: policy.allowed → if ok, classify, push to Frontier (depth+1), append trajectory edge; if type ∈ {repo_root, blob, issues, pull} → emit discovery (to Scraper).
	•	Update metrics (statuses, bytes, latencies, robots rejections, duplicates).
	•	Periodically flush metrics.
	•	Stop when: frontier empty OR depth > max_depth OR Manager’s stop file exists.

import asyncio, time
from urllib.parse import urlsplit
from .config import CrawlerConfig
from .robots_cache import RobotsCache
from .policy import UrlPolicy
from .frontier.frontier import Frontier
from .frontier.dedup import SeenSet
from .net.fetcher import Fetcher
from .net.limiter import RateLimiter
from .net.proxy_client import ProxyClient
from .parse.extractor import Extractor
from .io.trajectory import TrajectoryWriter
from .io.metrics import Metrics
from .io.discoveries import DiscoveriesWriter

class CrawlerService:
    def __init__(self, cfg: CrawlerConfig, proxy_client: ProxyClient | None = None):
        self.cfg = cfg
        self.proxy_client = proxy_client

    async def start(self, seeds: list[str]):
        self.robots = RobotsCache(self.cfg.robots["user_agent"], self.cfg.robots["cache_ttl_sec"])
        self.policy = UrlPolicy(self.cfg, self.robots)
        self.seen = SeenSet(self.cfg.frontier["bloom_capacity"])
        self.frontier = Frontier(self.cfg.frontier["db_path"], self.seen)
        self.metrics = Metrics(self.cfg.metrics["csv_path"], self.cfg.metrics["flush_interval_sec"])
        self.trajectory = TrajectoryWriter(self.cfg.trajectory["csv_path"])
        self.discoveries = DiscoveriesWriter(self.cfg.spool["discoveries_dir"], self.cfg.spool["rotate_every_mb"])
        self.limiter = RateLimiter(self.cfg.limits["global_concurrency"], self.cfg.limits["per_host_concurrency"])
        self.fetcher = Fetcher(self.cfg, self.proxy_client, self.limiter)

        # Seed
        for u in seeds:
            ok, reason = await self.policy.allowed(u)
            if ok:
                self.frontier.push({"url": u, "priority": 100, "depth": 0, "referrer": None, "discovered_at": int(time.time())})

    async def run(self):
        batch_size = 4
        while True:
            items = self.frontier.pop_batch(batch_size)
            if not items:
                await asyncio.sleep(0.2)
                self.metrics.maybe_flush()
                # exit if prolonged idle
                if self._frontier_is_empty():
                    break
                continue

            await asyncio.gather(*(self._handle_item(it) for it in items))
            self.metrics.maybe_flush()

    async def _handle_item(self, it: dict):
        url, depth, ref = it["url"], it["depth"], it["referrer"]
        if depth > self.cfg.caps["max_depth"]:
            self.metrics.inc("skip_depth")
            return
        res = await self.fetcher.get_html(url)
        if not res.get("ok"):
            self.metrics.inc(f"fetch_fail_{res.get('status','err')}")
            return

        self.metrics.inc("fetch_ok")
        self.metrics.inc("bytes_in", n=len(res["text"].encode("utf-8")))
        self.metrics.inc("latency_ms_sum", n=int(res["latency_ms"]))
        extractor = Extractor(url)
        links = extractor.extract_links(res["text"])

        for link in links:
            ok, reason = await self.policy.allowed(link)
            if not ok:
                self.metrics.inc(f"policy_reject_{reason}")
                continue

            page_type = self.policy.classify(link)
            # trajectory (src -> dst)
            self.trajectory.append(url, link, depth+1, anchor=None)

            pushed = self.frontier.push({
                "url": link,
                "priority": 100 + depth,  # breadth-first flavor
                "depth": depth + 1,
                "referrer": url,
                "discovered_at": int(time.time())
            })
            if pushed:
                self.metrics.inc("frontier_push")

            # emit to scraper only for target page types:
            if page_type in {"repo_root", "blob", "issues", "pull"}:
                self.discoveries.append({
                    "timestamp": int(time.time()),
                    "url": link,
                    "page_type": page_type,
                    "depth": depth + 1,
                    "referrer": url
                })

    def _frontier_is_empty(self) -> bool:
        # quick heuristic: try to pop one; if none, likely empty (or implement a size counter)
        return not self.frontier.pop_batch(1)

    async def stop(self):
        self.discoveries.close()
        self.trajectory.close()
        self.metrics.maybe_flush()


⸻

6) Metrics to track (suggested keys)
	•	Fetch: fetch_ok, fetch_fail_429, fetch_fail_5xx, fetch_fail_err, latency_ms_sum, bytes_in.
	•	Policy: policy_reject_host_not_allowed, policy_reject_denied_pattern, policy_reject_not_in_allowlist, policy_reject_robots_disallow.
	•	Frontier: frontier_push, skip_depth, duplicates (if you count dedup rejections).
	•	Proxies: (if available) proxy_throttle_events, proxy_switches.

Flush every 10s; you’ll post-aggregate for dashboards.

⸻

7) Trajectory map (edges.csv)
	•	Append one row per accepted link: ts, src, dst, depth, anchor.
	•	This gives you a crawl graph and a clear map of traversal.
	•	Avoid holding edges in memory; stream to CSV.

⸻

8) Backpressure & caps (Manager cooperation)
	•	Crawler writes to spool/discoveries/.
	•	If spool grows too large, pause adding new discoveries:
	•	Simple approach: if discoveries_dir size > N MB → temporarily emit only frontier pushes (no discoveries) until Scraper drains.
	•	Or expose a pause_discoveries.flag file; Crawler checks and pauses discoveries emission.
	•	Use caps to spread load:
	•	Track per-repo counts (repo owner/name parsed from URL) in-memory; stop emitting discoveries when reaching per_repo_max_*.
	•	Keep crawling for breadth, but don’t emit more items for that repo to Scraper.

⸻

9) Validation checklist
	•	Strict on-domain: only github.com (never raw., api., gist.).
	•	Deny-by-default policy with a minimal allowlist.
	•	Robots checked before queueing (and before fetching).
	•	Discovery rate low (start with 1 req/s, 1–2 concurrency).
	•	Resumability: LMDB frontier + Bloom seen-set on disk.
	•	CSV integrity: single-writer for edges.csv and crawler_metrics.csv.
	•	Discoveries queue is append-only JSONL, rotated.
	•	Proxy-ready: ProxyClient interface used on every request.

⸻

10) Typical runbook
	1.	Prepare config.yaml, seeds.txt.
	2.	python -m crawler.run --config config.yaml --seeds seeds.txt --workspace /data/github_crawl
	3.	Monitor:
	•	logs/crawler_metrics.csv (tail)
	•	logs/edges.csv (growing)
	•	spool/discoveries/ (new JSONL files appearing)
	4.	If rate-limited: reduce req_per_sec, reduce concurrency; ensure Manager rotates proxies.
	5.	Stop: Ctrl+C or stop.flag file you watch for clean shutdown.

⸻

11) Common pitfalls & safeguards
	•	Robots drift: Re-check allow/deny vs actual robots.txt regularly; write unit tests for policy samples.
	•	Duplicate links: canonicalize aggressively; drop fragments and most query params.
	•	Non-HTML bloat: always test Content-Type; skip non-text/html.
	•	Queue runaway: add backpressure; don’t emit discoveries if Scraper is backlogged.
	•	CSV corruption: always flush and close; use single writer per file.
	•	Proxy health: report 429/5xx to ProxyClient so Manager can cool down or swap.

⸻

Final questions for you (to lock the implementation):
	1.	Seeds set: Do you want only /topics + /trending (homepage only) plus a curated repo list, or also org pages you’ll provide?
	2.	Caps: Confirm defaults (per_repo_max_pages=30, issues=10, prs=10, max_depth=4).
	3.	Proxy pool: Will Manager provide a static pool or an API? Any geo/location constraints?
	4.	Spool budget: What’s the max size (MB/GB) before we pause discoveries?
	5.	Headers: Confirm User-Agent string (include contact).
	6.	Compression: Do you want to compress discoveries JSONL with .gz? (Scraper can read gz seamlessly.)

If you confirm these, I’ll generate a ready-to-run skeleton repo layout and unit test plan for policy, frontier, and fetcher.