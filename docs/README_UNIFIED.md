# Unified Crawler - Quick Start Guide

## Overview

This is the **refactored single-fetch architecture** that eliminates duplicate URL fetches.

**Key Improvement**: Each URL is fetched **only once** (50% reduction in network calls compared to old dual-crawler/scraper architecture).

## Architecture

```
Seeds → Frontier Queue (JSONL) → Fetch + Store → Extract Links → Enqueue New URLs
                                      ↓
                              Unified Metadata (JSONL)
                                      ↓
                              HTML Store (SHA-256 addressed)
```

## File Structure

```
workspace/
├── state/
│   ├── frontier.jsonl          # URL queue (to fetch)
│   ├── fetched_urls.txt        # Already fetched (dedup)
│   └── robots_cache.jsonl      # Cached robots.txt
├── metadata/
│   └── crawl_metadata.jsonl    # Unified crawl+scrape metadata
└── store/
    └── html/                   # Content-addressed HTML storage
        └── aa/bb/<sha>.html
```

## Quick Start

### 1. Reset (Clean Slate)

```bash
./vi-scrape-unified reset
```

### 2. Configure Workspace

```bash
./vi-scrape-unified configure
```

### 3. Run Unified Crawler

```bash
./vi-scrape-unified run
```

Or with custom config:

```bash
python crawler_unified.py --config config_unified.yaml --seeds seeds.txt
```

## Configuration

Edit `config_unified.yaml`:

```yaml
run_id: "unified-2025-01-12"
workspace: "./workspace"

user_agent: "YourBot/1.0 (+mailto:your@email.com)"

limits:
  req_per_sec: 1.0           # Requests per second
  max_depth: 4               # Maximum crawl depth

caps:
  per_repo_max_pages: 30     # Max pages per repository
  per_repo_max_issues: 10    # Max issues per repository
  per_repo_max_prs: 10       # Max PRs per repository

storage:
  frontier_file: "state/frontier.jsonl"
  fetched_urls_file: "state/fetched_urls.txt"
  robots_cache_file: "state/robots_cache.jsonl"
  metadata_file: "metadata/crawl_metadata.jsonl"
  html_store_root: "store/html"
  html_compress: false
```

## Seeds File

Create `seeds.txt` with starting URLs (one per line):

```
https://github.com/python/cpython
https://github.com/torvalds/linux
https://github.com/tensorflow/tensorflow
```

## Monitoring Progress

### Watch metadata file grow:
```bash
watch -n 1 'wc -l workspace/metadata/crawl_metadata.jsonl'
```

### Check frontier size:
```bash
jq -s length workspace/state/frontier.jsonl
```

### Check fetched URLs count:
```bash
wc -l workspace/state/fetched_urls.txt
```

### Check stored HTML count:
```bash
find workspace/store/html -name "*.html" | wc -l
```

### View latest metadata:
```bash
tail -1 workspace/metadata/crawl_metadata.jsonl | jq .
```

## Metadata Schema

Each record in `crawl_metadata.jsonl`:

```json
{
  "url": "https://github.com/python/cpython",
  "timestamp": 1705075200,
  "depth": 1,
  "page_type": "repo",
  "referrer": "https://github.com/topics/python",
  "http_status": 200,
  "content_type": "text/html; charset=utf-8",
  "encoding": "",
  "content_sha256": "abc123...",
  "content_bytes": 54321,
  "stored_path": "workspace/store/html/ab/c1/abc123....html",
  "etag": "W/\"abc123\"",
  "last_modified": "Thu, 12 Jan 2025 10:00:00 GMT",
  "fetch_latency_ms": 234.5,
  "retries": 0,
  "proxy_id": null,
  "metadata": {}
}
```

## Features

- ✅ **Single fetch per URL** - No duplicate network calls
- ✅ **File-based storage** - No databases (LMDB removed)
- ✅ **Unified metadata** - All data in one JSONL file
- ✅ **Breadth-first crawl** - Explores by depth
- ✅ **Resume support** - Restart from frontier.jsonl
- ✅ **Robots.txt compliance** - Cached and respected
- ✅ **Content-addressed storage** - SHA-256 deduplication
- ✅ **Rate limiting** - Configurable req/sec
- ✅ **Per-repo caps** - Limits on pages/issues/PRs

## Differences from Old Architecture

| Aspect | Old (Crawler + Scraper) | New (Unified) |
|--------|-------------------------|---------------|
| Fetches per URL | 2 (crawler + scraper) | 1 (unified) |
| Services | 2 separate | 1 unified |
| Databases | LMDB (frontier, index) | None (all files) |
| Metadata | 2 files (discoveries, pages CSV) | 1 file (JSONL) |
| Spool | Yes (discoveries JSONL) | No (eliminated) |
| Performance | Baseline | 50% faster |

## Troubleshooting

### Frontier file missing
Run `./vi-scrape-unified configure` to create directories.

### No HTML stored
Check `crawl_metadata.jsonl` for errors (http_status != 200).

### Out of memory
Reduce `max_depth` or enable compaction in code.

### Rate limited (429 errors)
Decrease `req_per_sec` in config.

### No links extracted
Check seeds are valid GitHub URLs matching policy patterns.

## Advanced Usage

### Verify no duplicate fetches:
```bash
sort workspace/state/fetched_urls.txt | uniq -d
# Should be empty
```

### Analyze depth distribution:
```bash
jq '.depth' workspace/metadata/crawl_metadata.jsonl | sort -n | uniq -c
```

### Find failed fetches:
```bash
jq 'select(.http_status != 200)' workspace/metadata/crawl_metadata.jsonl
```

### Extract all fetched URLs:
```bash
jq -r '.url' workspace/metadata/crawl_metadata.jsonl
```

## Performance Tips

1. **Increase rate limit** (if GitHub allows):
   ```yaml
   limits:
     req_per_sec: 2.0  # Increase from 1.0
   ```

2. **Enable compression** (saves disk space):
   ```yaml
   storage:
     html_compress: true
   ```

3. **Adjust depth** (control crawl scope):
   ```yaml
   caps:
     max_depth: 3  # Reduce from 4
   ```

## Support

See `REFACTORING_GUIDE.md` for detailed implementation documentation.

For questions or issues, check the refactoring guide's troubleshooting section.

---

**Happy Crawling! 🚀**
