# VI2025 — GitHub Crawler & Scraper

This repository contains a small GitHub-focused crawler and a scraper that
consume a discovery spool and store HTML pages in a content-addressed store.
The tooling is split into two parts:

- crawler/: discovery producer that writes `discoveries-*.jsonl` files to the
  spool (workspace/spool/discoveries). A minimal continuous producer is
  included for testing; replace it with a full crawler for production.
- scraper/: tails discovery JSONL files, fetches HTML, stores content under
  `workspace/store/html/`, and appends per-page metadata rows to CSVs under
  `workspace/logs/`.

This README covers the data layout, file formats, and step-by-step instructions
to reset state and run a continuous production-like run on macOS (zsh).

## Data layout (important paths)

All paths are relative to the repository root unless shown with an absolute path.

- workspace/
  - spool/
    - discoveries/                     # discovery JSONL files (discoveries-*.jsonl)
  - store/
    - html/                            # content-addressed HTML store
      - <sha[:2]>/<sha[2:4]>/<sha>.html
      - <sha> may have optional `.zst` compressed versions if compression enabled
  - state/
    - spool_bookmark.json              # {"file": "discoveries-xxx.jsonl", "offset": N}
    - page_index.lmdb/                 # LMDB page index (url -> {sha256, stored_path, etag, ...})
    - (other state files such as frontier/bloom filter may exist)
  - logs/
    - pages-<run_id>.csv               # pages CSV (rows of stored page metadata)
    - scraper_metrics-<run_id>.csv     # periodic metrics emitted by scraper

### Discovery spool format

Each discovery file is newline-delimited JSON (JSONL). Each line is a discovery
object with at least the following keys:

Example line:

```json
{"url": "https://github.com/topics", "page_type": "topic", "depth": 0, "referrer": null, "metadata": {}}
```

The scraper's `SpoolReader` reads `discoveries-*.jsonl` files in name order and
maintains the bookmark `workspace/state/spool_bookmark.json` with `{file,offset}`
so it can resume on restart.

### Pages CSV schema

`workspace/logs/pages-<run_id>.csv` has header and rows with these fields:

- timestamp
- url
- page_type
- depth
- referrer
- http_status
- content_type
- encoding
- content_sha256
- content_bytes
- stored_path
- etag
- last_modified
- fetch_latency_ms
- retries
- proxy_id
- metadata

CSV rows are appended by the scraper as pages are processed.

### Stored HTML layout

Stored HTML is content-addressed by sha256 and arranged with a two-level
directory split: `workspace/store/html/aa/bb/<sha>.html` (optionally compressed
as `.html.zst`). The `page_index.lmdb` holds a mapping used for conditional
requests and dedup.

## Quick reset: wipe all data and start fresh

Use this when you want to remove all store, logs, spool and index and begin a
clean run.

1. Activate virtualenv (replace path if different):

```bash
source venv/bin/activate
```

2. Stop running crawler / scraper processes before deleting files.

3. Remove files (run from repo root):

```bash
# remove discoveries and bookmark
rm -rf workspace/spool/discoveries/*
rm -f workspace/state/spool_bookmark.json

# remove stored html and index
rm -rf workspace/store/html/*
rm -rf workspace/state/page_index.lmdb/*

# remove logs and metrics
rm -f workspace/logs/pages-*.csv
rm -f workspace/logs/scraper_metrics-*.csv

# recreate directories
mkdir -p workspace/spool/discoveries
mkdir -p workspace/store/html
mkdir -p workspace/logs
mkdir -p workspace/state
```

4. Optional: create an empty bookmark so scraper starts fresh when files appear
   (not necessary if spool will be repopulated):

```bash
echo '{"file": null, "offset": 0}' > workspace/state/spool_bookmark.json
```

## Run a continuous production-like run (recommended minimal setup)

Goal: run both crawler (producer) and scraper so discoveries are produced
continually and consumed. Use a supervisor for long-lived runs — below are
several options.

Prerequisites
- Python virtualenv activated (`source venv/bin/activate`)
- `config.yaml` / `scraper_config.yaml` set up. Example field of note:
  - `scraper_config.yaml.spool.dir` should point to `workspace/spool/discoveries`

### Option A — lightweight (tmux) — recommended for manual production runs

1. Open tmux session:

```bash
tmux new -s vi-prod
```

2. Start the crawler (left pane)

```bash
source venv/bin/activate
# `--period` controls how frequently the test crawler writes discoveries
python -m crawler run --config config.yaml --period 2.0 --seeds seeds.txt
```

3. Split pane and start the scraper (right pane)

```bash
tmux split-window -h
source venv/bin/activate
./vi-scrape run --config scraper_config.yaml
```

4. Detach tmux (Ctrl-b d). Both processes continue running. Re-attach with
   `tmux attach -t vi-prod`.

### Option B — supervised with launchd (macOS)

Create `~/Library/LaunchAgents/com.yourorg.vinf.crawler.plist` and
`com.yourorg.vinf.scraper.plist` with ProgramArguments set to the appropriate
commands and `KeepAlive` true. Then load with:

```bash
# load plists
launchctl load ~/Library/LaunchAgents/com.yourorg.vinf.crawler.plist
launchctl load ~/Library/LaunchAgents/com.yourorg.vinf.scraper.plist
```

See macOS `launchctl` docs for details.

### Option C — Docker / containers (recommended for robust deployments)

Package each component into a container that mounts the `workspace/` volume.
Use docker-compose to run both with restart policies, log collection, and
resource limits. Not included here but recommended for real production.

### Safe shutdown & restart

1. Stop the crawler first (so no new discoveries are created):

```bash
# send TERM to crawler PID
kill -TERM <crawler-pid>
```

2. Wait for crawler to stop. Then stop scraper; scraper will flush metrics and
   save the spool bookmark during graceful shutdown:

```bash
kill -TERM <scraper-pid>
```

3. If you must force-stop, use `kill -9` but then check `workspace/state` and
   `workspace/spool` for consistency and possibly run integrity checks.

## Signals and graceful shutdown (scraper specifics)

- scraper handles SIGTERM/SIGINT and attempts to save the spool bookmark and
  flush metrics before exit.
- crawler (the provided test producer) will stop writing and exit on TERM.

## Troubleshooting checklist

- No discoveries in spool? Ensure crawler is running and writing files named
  `discoveries-*.jsonl` in `workspace/spool/discoveries/`.
- Scraper hangs waiting: check `workspace/state/spool_bookmark.json` and that
  the spool folder contains files with names matching the bookmark.
- Permission denied when storing HTML: verify `scraper_config.yaml` ->
  `store.permissions` is a numeric permission (like `0o644` or `420`); the
  config loader will parse string octals.
- Unrecognized errors during HTTP fetch: check that the environment (proxies,
  TLS libraries) matches expected versions; the fetcher uses httpx.

## Data retention and storage policy (suggested)

- Keep page CSVs and stored HTML for as long as needed. Store CSVs in a
  date-named archive or move to cold storage if retention is needed.
- Rotate `workspace/spool/discoveries/*` files once processed and removed by
  a downstream archiver, or implement rotation on the producer side to avoid
  unbounded spool growth.

## Where to look in the code (developer pointers)

- `scraper/io/spool.py` — spool tailing and bookmark handling
- `scraper/io/storage.py` — content-addressed storage for HTML
- `scraper/run.py` — main scraper orchestrator
- `crawler/run.py` — the minimal producer implementation (replace with full
  crawler for production)

## Next steps (project-specific)

If you want this repo to run truly “production-grade”, I can implement the
following next:

1. Spool rotation and backlog checks in the producer (avoid unlimited file growth)
2. A frontier/dedup (LMDB or bloom) so the crawler doesn't re-emit duplicates
3. Robots/policy checks in the crawler prior to emitting discoveries
4. launchd/systemd unit files or docker-compose templates for production

Tell me which of the next steps you'd like and I will implement it.
