# VI2025 Scraper - User Guide

**Complete guide to scraping GitHub HTML content using the VI2025 Scraper**

---

## Table of Contents

1. [Overview](#overview)
2. [Prerequisites](#prerequisites)
3. [Installation](#installation)
4. [Configuration](#configuration)
5. [Quick Start](#quick-start)
6. [Usage Scenarios](#usage-scenarios)
7. [Understanding the Output](#understanding-the-output)
8. [Advanced Features](#advanced-features)
9. [Monitoring and Control](#monitoring-and-control)
10. [Troubleshooting](#troubleshooting)
11. [Integration with Crawler](#integration-with-crawler)

---

## Overview

The VI2025 Scraper is a production-grade, async Python tool designed to:

- **Consume discoveries** from the Crawler (URLs to fetch)
- **Download HTML content** from GitHub pages
- **Store content** efficiently using content-addressed storage
- **Track metadata** in CSV format for analysis
- **Support resumption** after interruptions
- **Respect rate limits** and robots.txt

### Key Features

- ✅ **Conservative by default**: 1 req/sec, 2 concurrent connections
- ✅ **Conditional requests**: Uses ETags to avoid re-downloading unchanged content
- ✅ **Content deduplication**: Same content stored only once
- ✅ **Optional compression**: Zstandard compression can achieve 90%+ space savings
- ✅ **Full resumption**: Pick up exactly where you left off
- ✅ **Graceful shutdown**: CTRL+C cleanly saves state
- ✅ **Live monitoring**: Signal-based status dumps

---

## Prerequisites

### System Requirements

- **Python**: 3.11 or higher
- **OS**: Linux, macOS, or Windows (signals work best on Unix)
- **Disk**: Sufficient space for HTML storage (plan for ~500 KB per page)
- **Network**: Stable internet connection

### Python Dependencies

All dependencies are listed in `requirements.txt`:

```
httpx[http2] >= 0.27.0
pyyaml >= 6.0
lmdb >= 1.4.1
zstandard >= 0.21.0  # Optional, for compression
```

---

## Installation

### Step 1: Clone the Repository

```bash
cd /path/to/your/project
# Repository should already contain the scraper/ directory
```

### Step 2: Create Virtual Environment

```bash
python3 -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate
```

### Step 3: Install Dependencies

```bash
pip install -r requirements.txt
```

### Step 4: Verify Installation

```bash
# Using Python module
python -m scraper status --config scraper_config.yaml

# Using executable script
./vi-scrape status --config scraper_config.yaml
```

You should see the scraper status output with compliance information.

---

## Configuration

### Configuration File Structure

Create a configuration file (e.g., `my_scraper_config.yaml`):

```yaml
# Run identification
run_id: "2025-01-09T15-30Z"
workspace: "./workspace"

# User agent and headers
user_agent: "MyResearchBot/1.0 (+https://mysite.edu/bot-info)"
accept_language: "en"
accept_encoding: "br, gzip"

# Rate limiting and timeouts
limits:
  global_concurrency: 2       # Max concurrent requests
  req_per_sec: 1.0           # Requests per second (conservative!)
  connect_timeout_ms: 4000    # Connection timeout
  read_timeout_ms: 15000      # Read timeout
  total_timeout_ms: 25000     # Total request timeout
  max_retries: 3              # Retry attempts on failure
  backoff_base_ms: 500        # Exponential backoff base
  backoff_cap_ms: 8000        # Maximum backoff wait

# Discovery spool settings
spool:
  dir: "spool/discoveries"    # Where crawler writes discoveries
  max_backlog_gb: 10          # Pause if spool exceeds this

# HTML storage
store:
  root: "store/html"          # Where to store HTML files
  compress: false             # Enable .html.zst compression
  permissions: 0o644          # File permissions

# Output files
pages_csv:
  path: "logs/pages-{run_id}.csv"

metrics:
  csv_path: "logs/scraper_metrics-{run_id}.csv"
  flush_interval_sec: 10

# State files
index:
  db_path: "state/page_index.lmdb"

bookmark:
  path: "state/spool_bookmark.json"
```

### Important Configuration Notes

#### User Agent (**CRITICAL**)

Your `user_agent` **must** include contact information:

```yaml
# ✅ GOOD - includes contact
user_agent: "MyBot/1.0 (+https://example.edu/bot-info)"
user_agent: "ResearchBot/1.0 (+mailto:contact@university.edu)"

# ❌ BAD - no contact info
user_agent: "MyBot/1.0"
```

#### Rate Limits

Start conservatively:

```yaml
limits:
  global_concurrency: 2
  req_per_sec: 1.0
```

**Only increase** if you:
- Have permission from GitHub
- Monitor for 429 responses
- Increase gradually (e.g., 1.0 → 1.5 → 2.0 req/sec)

#### Compression

Enable compression to save disk space:

```yaml
store:
  compress: true  # Saves ~90% disk space
```

**Trade-off**: Uses more CPU during save/load operations.

---

## Quick Start

### Scenario: Scraping from Crawler Discoveries

#### Step 1: Ensure Crawler is Running

The Crawler should be writing discovery files to the spool directory:

```bash
# Check for discovery files
ls workspace/spool/discoveries/
# Should show: discoveries-001.jsonl, discoveries-002.jsonl, etc.
```

#### Step 2: Create Workspace Structure

```bash
mkdir -p workspace/{spool/discoveries,store/html,logs,state}
```

#### Step 3: Start the Scraper

```bash
# Using vi-scrape executable
./vi-scrape run --config scraper_config.yaml

# OR using Python module
python -m scraper run --config scraper_config.yaml
```

You'll see output like:

```
======================================================================
VI2025 SCRAPER - GitHub HTML Content Scraper
======================================================================
User-Agent: MyBot/1.0 (+https://example.edu/bot)
Run ID: 2025-01-09T15-30Z

COMPLIANCE LINKS:
  - Robots.txt: https://github.com/robots.txt
  - Acceptable Use: https://docs.github.com/...
  - Terms of Service: https://docs.github.com/...
======================================================================

Initializing scraper components...
Components initialized successfully

Scraper service starting...
Reading from spool: ./workspace/spool/discoveries
Storing HTML in: ./workspace/store/html
Writing CSV to: ./workspace/logs/pages-2025-01-09T15-30Z.csv

Waiting for discoveries from spool...
```

#### Step 4: Monitor Progress

The scraper will process each discovery:

```
Processing: https://github.com/python/cpython
  ✓ Saved: 245.50 KB
    Compressed: 31.23 KB (87.3%)

Processing: https://github.com/python/cpython/blob/main/README.md
  → 304 Not Modified (using cached)
```

#### Step 5: Graceful Shutdown

Press **CTRL+C** to stop:

```
SIGINT received - initiating graceful shutdown...

Cleaning up resources...
  CSV closed: 150 rows written
  Metrics flushed
  Index closed: 150 entries
  Fetcher closed

Final statistics:
  Runtime: 180.5s
  Pages processed: 150
  Average: 1.20s per page

Cleanup complete
```

---

## Usage Scenarios

### Scenario 1: Fresh Start

Starting from scratch:

```bash
# Clean workspace (if needed)
rm -rf workspace

# Create directories
mkdir -p workspace/{spool/discoveries,store/html,logs,state}

# Start scraper
./vi-scrape run --config my_config.yaml
```

### Scenario 2: Resume After Interruption

If the scraper stopped (crash, CTRL+C, etc.), resume exactly where it left off:

```bash
# Simply run again - it will auto-resume
./vi-scrape resume --config my_config.yaml
```

The scraper reads the bookmark (`state/spool_bookmark.json`) and continues from the exact byte offset.

### Scenario 3: Check Status

View current state without running:

```bash
./vi-scrape status --config my_config.yaml
```

Output:

```
SCRAPER STATUS
============================================================
Run ID: 2025-01-09T15-30Z
Workspace: ./workspace

Bookmark:
  File: discoveries-003.jsonl
  Offset: 45823 bytes

Spool:
  Directory: ./workspace/spool/discoveries
  Files: 5
  Total size: 2.451 GB
  Backpressure threshold: 10 GB

Stored HTML: 1247 files

Pages CSV: 1247 rows

Control Flags:
  Paused: NO
  Stop requested: NO
============================================================
```

### Scenario 4: Replay Mode (Rebuild CSV)

Rebuild the pages CSV from stored HTML without re-downloading:

```bash
./vi-scrape replay --config my_config.yaml
```

**Use case**: If you need to regenerate the CSV with different columns or fix corruption.

### Scenario 5: Running with Compression

Enable compression for large-scale scraping:

```yaml
store:
  compress: true
```

```bash
./vi-scrape run --config my_config_compressed.yaml
```

---

## Understanding the Output

### 1. Stored HTML Files

**Location**: `workspace/store/html/`

**Structure**: Content-addressed using SHA-256

```
store/html/
├── ab/
│   └── cd/
│       └── abcd1234...5678.html      # Uncompressed
│       └── abcd1234...5678.html.zst  # Compressed
├── 12/
│   └── 34/
│       └── 1234abcd...ef90.html
```

**Benefits**:
- Same content stored only once (deduplication)
- Deterministic paths (can verify integrity)
- Efficient directory structure (no huge flat directories)

### 2. Pages CSV

**Location**: `workspace/logs/pages-<run_id>.csv`

**Columns**:

| Column | Description |
|--------|-------------|
| `timestamp` | Unix timestamp of scrape |
| `url` | Full URL scraped |
| `page_type` | Type from crawler (repo_root, blob, issues, etc.) |
| `depth` | Crawl depth |
| `referrer` | Referring URL |
| `http_status` | HTTP status code (200, 304, etc.) |
| `content_type` | MIME type |
| `encoding` | Content encoding (gzip, br, etc.) |
| `content_sha256` | SHA-256 hash of content |
| `content_bytes` | Size in bytes |
| `stored_path` | Full path to stored file |
| `etag` | ETag header (for caching) |
| `last_modified` | Last-Modified header |
| `fetch_latency_ms` | Request latency |
| `retries` | Number of retries needed |
| `proxy_id` | Proxy used (if any) |
| `metadata` | JSON from discovery (repo info, etc.) |

**Example row**:

```csv
1704816000,https://github.com/python/cpython,repo_root,0,,200,text/html; charset=utf-8,,abc123...,245760,/workspace/store/html/ab/c1/abc123....html,"W/""etag-xyz""","Wed, 09 Jan 2025 15:30:00 GMT",1234,0,,"{""repo"":""python/cpython"",""stars"":50000}"
```

### 3. Metrics CSV

**Location**: `workspace/logs/scraper_metrics-<run_id>.csv`

**Format**: Timestamped counter snapshots

```csv
timestamp,metric,value
1704816100,pages_fetched,150
1704816100,pages_saved,142
1704816100,bytes_saved,35840000
1704816100,status_2xx,142
1704816100,status_3xx,8
1704816100,not_modified_304,20
```

**Key metrics**:
- `pages_fetched`: Total fetch attempts
- `pages_saved`: Successfully stored
- `bytes_saved`: Total HTML bytes
- `status_2xx/3xx/4xx/5xx`: HTTP status buckets
- `not_html`: Skipped (not HTML content)
- `not_modified_304`: Cached (304 responses)
- `retries_total`: Total retry attempts

### 4. Page Index (LMDB)

**Location**: `workspace/state/page_index.lmdb/`

**Purpose**: Fast lookup for conditional requests

**Contents**: URL → {sha256, stored_path, etag, last_modified, timestamp}

**Not meant for direct access** - use the scraper API or replay mode.

### 5. Bookmark

**Location**: `workspace/state/spool_bookmark.json`

**Format**:

```json
{
  "file": "discoveries-003.jsonl",
  "offset": 45823
}
```

**Purpose**: Enables exact resume after interruption.

---

## Advanced Features

### 1. Conditional Requests (304 Optimization)

The scraper automatically uses ETags and Last-Modified headers:

**First fetch**:
```
GET /python/cpython HTTP/2
```

**Response**:
```
HTTP/2 200 OK
ETag: "abc123"
Last-Modified: Wed, 09 Jan 2025 12:00:00 GMT
```

**Subsequent fetch**:
```
GET /python/cpython HTTP/2
If-None-Match: "abc123"
If-Modified-Since: Wed, 09 Jan 2025 12:00:00 GMT
```

**Response**:
```
HTTP/2 304 Not Modified
```

**Result**: No content downloaded, CSV row references cached copy.

### 2. Backpressure Control

If the discovery spool grows too large (default: 10 GB), the scraper pauses consumption:

```
BACKPRESSURE: Spool size (12.34 GB) exceeds threshold (10 GB)
Pausing consumption for 10 seconds...
```

**Why?**: Prevents overwhelming the scraper if the crawler is producing discoveries faster than they can be processed.

**Configure**:

```yaml
spool:
  max_backlog_gb: 20  # Increase threshold
```

### 3. Proxy Support (Future Integration)

The scraper supports proxy rotation via dependency injection:

```python
from scraper.run import ScraperService
from scraper.config import ScraperConfig

# Your proxy client implementation
class MyProxyClient:
    async def pick(self):
        return {
            "proxy_id": "proxy1",
            "http": "http://proxy.example.com:8080",
            "https": "http://proxy.example.com:8080"
        }

    async def report_result(self, proxy_id, status, throttled):
        # Log proxy health
        pass

config = ScraperConfig.from_yaml("config.yaml")
service = ScraperService(config, proxy_client=MyProxyClient())
await service.run()
```

### 4. Replay Mode

Rebuild the pages CSV from stored HTML:

```bash
./vi-scrape replay --config my_config.yaml
```

**Use cases**:
- CSV file corruption
- Need to add new columns
- Verify stored content integrity
- Generate different CSV format

**Note**: Replay mode only includes data stored in the index. Page type, depth, and referrer are not preserved.

---

## Monitoring and Control

### Real-time Monitoring

#### Progress Messages

The scraper prints detailed progress:

```
Processing: https://github.com/python/cpython
  ✓ Saved: 245.50 KB
    Compressed: 31.23 KB (87.3%)

Processing: https://github.com/torvalds/linux
  Found in index - ETag: "W/\"xyz\""
  → 304 Not Modified (using cached)

Progress: 10 pages processed
Metrics Summary:

Fetch Metrics:
  pages_fetched: 10
  pages_saved: 8
  bytes_saved: 2,048,000 (1.95 MB)

HTTP Status Codes:
  status_2xx: 8
  status_3xx: 2

Runtime: 12s
```

#### Status Signal (SIGUSR1)

Send SIGUSR1 to dump live status:

```bash
# Find PID
ps aux | grep vi-scrape

# Send signal
kill -USR1 <PID>
```

**Output** (to console):

```
============================================================
SCRAPER STATUS DUMP (SIGUSR1)
============================================================
  pages_processed: 157
  last_url: https://github.com/python/cpython/blob/main/README.md
  last_status: 200
  uptime: 203s
  spool: {items_read: 157, files_processed: 3, ...}
  metrics: {pages_fetched: 157, pages_saved: 145, ...}
  index: {entries: 145, db_size: 245760, ...}
  fetcher: {total_fetches: 157, successful: 145, ...}
============================================================
```

### Graceful Control

#### Graceful Shutdown (SIGINT/SIGTERM)

Press **CTRL+C** or send SIGTERM:

```bash
kill -TERM <PID>
```

**Actions performed**:
1. Stop accepting new items
2. Finish processing current item
3. Save bookmark
4. Close CSV file
5. Flush metrics
6. Sync and close LMDB index
7. Print final statistics

#### Configuration Reload (SIGHUP)

Reload limits and thresholds without restarting:

```bash
kill -HUP <PID>
```

**Reloadable settings**:
- `limits.*` (concurrency, rate, timeouts, retries)
- `store.compress`
- `spool.max_backlog_gb`
- `metrics.flush_interval_sec`

**Non-reloadable** (require restart):
- Paths (workspace, spool dir, store root, etc.)
- User agent

#### Manager Control Flags

For integration with a Manager component:

**Pause scraper**:
```bash
touch workspace/pause.scraper
```

The scraper will pause consumption and wait.

**Resume**:
```bash
rm workspace/pause.scraper
```

**Stop scraper**:
```bash
touch workspace/stop.scraper
```

The scraper will initiate graceful shutdown.

---

## Troubleshooting

### Issue: Scraper won't start

**Symptoms**: Immediate exit or error on startup

**Checks**:

1. **Verify spool directory exists**:
   ```bash
   ls workspace/spool/discoveries/
   ```

2. **Check config file**:
   ```bash
   cat scraper_config.yaml
   # Ensure paths are correct
   ```

3. **Test with status command**:
   ```bash
   ./vi-scrape status --config scraper_config.yaml
   ```

4. **Check Python version**:
   ```bash
   python --version  # Should be 3.11+
   ```

### Issue: "No module named 'yaml'" or similar

**Solution**: Install dependencies

```bash
source venv/bin/activate
pip install -r requirements.txt
```

### Issue: HTTP/2 error

**Error**: `ImportError: Using http2=True, but the 'h2' package is not installed`

**Solution**:
```bash
pip install "httpx[http2]"
```

### Issue: Scraper stuck, no progress

**Checks**:

1. **Verify discoveries are being written**:
   ```bash
   ls -lh workspace/spool/discoveries/
   # Check file timestamps
   ```

2. **Check spool bookmark**:
   ```bash
   cat workspace/state/spool_bookmark.json
   # {"file": "discoveries-003.jsonl", "offset": 12345}
   ```

3. **Check for pause flag**:
   ```bash
   ls workspace/pause.scraper  # Should not exist
   ```

4. **Send status signal**:
   ```bash
   kill -USR1 <PID>
   # Check if scraper is responsive
   ```

### Issue: 429 Rate Limiting

**Symptoms**: Many retries, slow progress

**Console output**:
```
Backoff: waiting 2.00s (retry 1)
Backoff: waiting 4.00s (retry 2)
```

**Solution**: Reduce rate limits

```yaml
limits:
  global_concurrency: 1
  req_per_sec: 0.5  # Even more conservative
```

**Check metrics**:
```bash
grep "status_4xx" workspace/logs/scraper_metrics*.csv
```

### Issue: Disk full

**Symptoms**: Write errors, crashes

**Solution**:

1. **Enable compression** (if not already):
   ```yaml
   store:
     compress: true  # Can save 90% space
   ```

2. **Check disk usage**:
   ```bash
   du -sh workspace/store/html/
   ```

3. **Clean old data** (if appropriate):
   ```bash
   # Backup first!
   tar czf backup-$(date +%Y%m%d).tar.gz workspace/

   # Remove old HTML (keeps CSV and index)
   rm -rf workspace/store/html/
   ```

### Issue: CSV corruption

**Symptoms**: Invalid CSV format, parsing errors

**Solution**: Use replay mode to rebuild

```bash
# Backup corrupted CSV
mv workspace/logs/pages-*.csv workspace/logs/pages-backup.csv

# Rebuild from stored HTML
./vi-scrape replay --config scraper_config.yaml
```

### Issue: Can't resume, bookmark missing

**Symptoms**: `ERROR: No bookmark found - cannot resume`

**Solution**: Use `run` instead of `resume`

```bash
# run command will start from beginning if no bookmark
./vi-scrape run --config scraper_config.yaml
```

Or manually create bookmark:

```bash
mkdir -p workspace/state
echo '{"file": "discoveries-001.jsonl", "offset": 0}' > workspace/state/spool_bookmark.json
```

---

## Integration with Crawler

### Workflow Overview

```
┌─────────────────┐
│    Crawler      │
│                 │
│  1. Discovers   │
│     GitHub URLs │
│                 │
│  2. Writes to   │
│     spool/      │
└────────┬────────┘
         │
         │ discoveries-*.jsonl
         │
         ▼
┌─────────────────┐
│    Scraper      │
│                 │
│  3. Reads spool │
│                 │
│  4. Fetches     │
│     HTML        │
│                 │
│  5. Stores in   │
│     store/      │
│                 │
│  6. Writes CSV  │
└─────────────────┘
```

### Discovery Format

The Crawler writes JSONL files to the spool:

**File**: `workspace/spool/discoveries/discoveries-001.jsonl`

**Format** (one JSON object per line):

```json
{"timestamp": 1704816000, "url": "https://github.com/python/cpython", "page_type": "repo_root", "depth": 0, "referrer": null, "metadata": {"repo": "python/cpython", "stars": 50000}}
{"timestamp": 1704816001, "url": "https://github.com/python/cpython/blob/main/README.md", "page_type": "blob", "depth": 1, "referrer": "https://github.com/python/cpython", "metadata": {"repo": "python/cpython", "file": "README.md"}}
```

**Required fields**:
- `timestamp`: Unix timestamp
- `url`: Full URL to scrape
- `page_type`: Type classification (repo_root, blob, issues, pull, etc.)
- `depth`: Crawl depth from seed
- `referrer`: Referring URL (or null)

**Optional**:
- `metadata`: Arbitrary JSON (passed through to CSV)

### Running Crawler and Scraper Together

**Terminal 1 - Crawler**:
```bash
source venv/bin/activate
python -m crawler run --config config.yaml --seeds seeds.txt
```

**Terminal 2 - Scraper**:
```bash
source venv/bin/activate
./vi-scrape run --config scraper_config.yaml
```

Both will run concurrently:
- Crawler discovers URLs → writes to spool
- Scraper reads spool → fetches HTML → stores

### Manager Integration (Future)

A Manager component can coordinate both:

```python
# Pseudocode
async def run_manager():
    crawler = CrawlerService(config)
    scraper = ScraperService(config, proxy_client=MyProxyPool())

    # Start both
    crawler_task = asyncio.create_task(crawler.run())
    scraper_task = asyncio.create_task(scraper.run())

    # Monitor and control
    while True:
        if spool_too_large():
            # Pause scraper via flag
            Path("workspace/pause.scraper").touch()

        if rate_limited():
            # Reduce crawler rate
            crawler.throttle()

        await asyncio.sleep(60)
```

---

## Best Practices

### 1. Start Conservative

```yaml
limits:
  global_concurrency: 1
  req_per_sec: 0.5
```

**Gradually increase** only after monitoring for 429 responses.

### 2. Monitor Compliance

Check robots.txt regularly:

```bash
curl https://github.com/robots.txt
```

Ensure your user agent is not disallowed.

### 3. Use Compression for Large Scrapes

```yaml
store:
  compress: true
```

**Savings**: 500 KB/page → 50 KB/page (typical)

For 100,000 pages:
- Uncompressed: ~50 GB
- Compressed: ~5 GB

### 4. Regular Status Checks

```bash
# Create a monitoring script
#!/bin/bash
while true; do
    date
    ./vi-scrape status --config scraper_config.yaml | grep -A 5 "Spool:"
    sleep 300  # Every 5 minutes
done
```

### 5. Backup Important Data

```bash
# Backup bookmark and index
tar czf state-backup-$(date +%Y%m%d-%H%M).tar.gz workspace/state/

# Backup CSVs
tar czf logs-backup-$(date +%Y%m%d-%H%M).tar.gz workspace/logs/
```

### 6. Clean Shutdown

Always use **CTRL+C** instead of `kill -9`:

```bash
# ✅ GOOD - graceful
kill -INT <PID>
# or press CTRL+C

# ❌ BAD - forces shutdown, may lose data
kill -9 <PID>
```

### 7. Test with Small Batches

Before large scrapes:

```bash
# Create test spool with 10 URLs
head -n 10 large_discoveries.jsonl > workspace/spool/discoveries/test.jsonl

# Run scraper
./vi-scrape run --config scraper_config.yaml

# Verify output
ls -lh workspace/store/html/
wc -l workspace/logs/pages-*.csv
```

---

## Appendix: CLI Reference

### Commands

#### `run`

Start scraper (fresh or auto-resume if bookmark exists)

```bash
./vi-scrape run --config <path>
```

**Options**:
- `--config`: Path to YAML config (required)

**Behavior**:
- Loads bookmark if exists
- Creates directories if needed
- Starts consuming spool

#### `resume`

Resume strictly from bookmark (fails if no bookmark)

```bash
./vi-scrape resume --config <path>
```

**Use case**: Ensure you're continuing from last position, not starting fresh.

#### `replay`

Rebuild pages CSV from stored HTML (no network)

```bash
./vi-scrape replay --config <path>
```

**Behavior**:
- Reads all entries from LMDB index
- Verifies stored files exist
- Writes new CSV with reconstructed data

**Note**: Some fields (page_type, depth, referrer) are not stored in index and will be blank.

#### `status`

Show current state (no changes made)

```bash
./vi-scrape status --config <path>
```

**Output**:
- Run ID and workspace
- Bookmark position
- Spool size and file count
- Stored HTML count
- CSV row count
- Control flags

---

## Appendix: File Format Reference

### Discovery JSONL

**One JSON object per line, no commas between lines**

```json
{"timestamp":1704816000,"url":"https://github.com/python/cpython","page_type":"repo_root","depth":0,"referrer":null,"metadata":{"source":"seed"}}
```

**Schema**:
```python
{
    "timestamp": int,        # Unix timestamp
    "url": str,              # Full URL
    "page_type": str,        # Classification
    "depth": int,            # Crawl depth
    "referrer": str | null,  # Referring URL
    "metadata": dict         # Arbitrary data (passed to CSV)
}
```

### Bookmark JSON

```json
{
    "file": "discoveries-003.jsonl",
    "offset": 45823
}
```

**Fields**:
- `file`: Filename (not full path)
- `offset`: Byte position in file

### Pages CSV

**Encoding**: UTF-8
**Delimiter**: Comma (`,`)
**Newlines**: Unix (`\n`)
**Quoting**: Minimal (only when needed)

**Header**:
```
timestamp,url,page_type,depth,referrer,http_status,content_type,encoding,content_sha256,content_bytes,stored_path,etag,last_modified,fetch_latency_ms,retries,proxy_id,metadata
```

**Row example**:
```csv
1704816000,https://github.com/python/cpython,repo_root,0,,200,text/html; charset=utf-8,,abc123...,245760,/workspace/store/html/ab/c1/abc123.html,"W/""xyz""","Wed, 09 Jan 2025 12:00:00 GMT",1234,0,,"{""repo"":""python/cpython""}"
```

### Metrics CSV

**Encoding**: UTF-8
**Delimiter**: Comma (`,`)
**No header** - just data rows

**Format**:
```
timestamp,metric_name,value
```

**Example**:
```csv
1704816100,pages_fetched,150
1704816100,pages_saved,142
1704816100,bytes_saved,35840000
```

---

## Support and Contact

### Documentation

- **Project docs**: See `docs/` directory
- **Scraper docs**: `docs/SCRAPER.md`
- **Crawler integration**: `docs/CRAWLER.md`

### Compliance

- **GitHub Robots.txt**: https://github.com/robots.txt
- **Acceptable Use Policies**: https://docs.github.com/en/site-policy/acceptable-use-policies
- **Terms of Service**: https://docs.github.com/site-policy/github-terms

### Issues

For bugs or questions, check the repository issues or create a new one with:
- Configuration file (redact sensitive info)
- Error messages
- Output of `vi-scrape status`
- Python version (`python --version`)

---

**End of User Guide**

For technical details on implementation, see `docs/SCRAPER.md`.

For integration with the Crawler, see `docs/CRAWLER.md` and `docs/PROJECT.md`.