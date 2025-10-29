#!/usr/bin/env python3
"""
CLI utility for summarising crawler outputs stored in the local workspace.

The tool scans `workspace/metadata/crawl_metadata.jsonl` alongside
`workspace/state/service_stats.json` (paths configurable via CLI flags) and
emits:

- Console summary with high-level metrics (document counts, sizes, success vs.
  failure, HTTP status distribution).
- Per page-type breakdown (counts, byte totals, relative share, optional
  relevance tagging).
- Optional Markdown and CSV artefacts for direct inclusion in wiki/report
  deliverables.
- Optional token statistics from extracted text dumps when `tiktoken` is
  installed.

Usage (basic):

```bash
python tools/crawl_stats.py --workspace workspace
```

Usage (with Markdown + CSV outputs):

```bash
python tools/crawl_stats.py \
  --workspace workspace \
  --markdown-output docs/generated/crawl_stats.md \
  --csv-output docs/generated/crawl_stats.csv
```
"""

from __future__ import annotations

import argparse
import csv
import json
import math
import sys
from collections import Counter
from dataclasses import dataclass, field
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Sequence, Tuple


def format_bytes(num_bytes: int) -> str:
    """Render byte counts using human-friendly units."""
    step = 1024.0
    units = ["B", "KB", "MB", "GB", "TB"]
    value = float(num_bytes)
    for unit in units:
        if value < step or unit == units[-1]:
            return f"{value:.2f} {unit}"
        value /= step
    return f"{value:.2f} {units[-1]}"


def format_seconds(seconds: float) -> str:
    """Format seconds into a compact h/m/s string."""
    total_seconds = int(seconds)
    hours, remainder = divmod(total_seconds, 3600)
    minutes, secs = divmod(remainder, 60)
    parts: List[str] = []
    if hours:
        parts.append(f"{hours}h")
    if minutes or (hours and secs):
        parts.append(f"{minutes}m")
    parts.append(f"{secs}s")
    return " ".join(parts)


def parse_args(argv: Optional[Sequence[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Summarise crawled metadata and service stats."
    )
    parser.add_argument(
        "--workspace",
        default="workspace",
        help="Workspace root containing state/ and metadata/ directories (default: %(default)s).",
    )
    parser.add_argument(
        "--metadata-file",
        help="Explicit path to crawl_metadata.jsonl (default: <workspace>/metadata/crawl_metadata.jsonl).",
    )
    parser.add_argument(
        "--service-stats-file",
        help="Explicit path to service_stats.json (default: <workspace>/state/service_stats.json).",
    )
    parser.add_argument(
        "--markdown-output",
        help="Optional path to write Markdown summary (directories created automatically).",
    )
    parser.add_argument(
        "--csv-output",
        help="Optional path to write CSV breakdown of page types.",
    )
    parser.add_argument(
        "--relevant-page-types",
        nargs="*",
        default=[],
        help="Page types considered relevant (e.g. repo_root issues pull). Only counts 2xx documents.",
    )
    parser.add_argument(
        "--text-root",
        help="Optional root directory with extracted text files for token statistics.",
    )
    parser.add_argument(
        "--token-model",
        default="cl100k_base",
        help="tiktoken model identifier used for token counting (default: %(default)s).",
    )
    parser.add_argument(
        "--token-limit",
        type=int,
        help="Optional maximum number of text files to sample when computing token stats.",
    )
    parser.add_argument(
        "--verbose",
        action="store_true",
        help="Enable verbose logging (prints progress while scanning metadata/text files).",
    )
    return parser.parse_args(argv)


@dataclass
class PageTypeStats:
    count: int = 0
    total_bytes: int = 0

    def add(self, size: int) -> None:
        self.count += 1
        self.total_bytes += size

    @property
    def average_bytes(self) -> float:
        if self.count == 0:
            return 0.0
        return self.total_bytes / self.count


@dataclass
class CrawlStats:
    relevant_page_types: set = field(default_factory=set)
    total_docs: int = 0
    total_bytes: int = 0
    success_docs: int = 0
    failed_docs: int = 0
    status_counts: Counter = field(default_factory=Counter)
    page_types: Dict[str, PageTypeStats] = field(default_factory=dict)
    relevant_docs: int = 0
    relevant_bytes: int = 0
    first_timestamp: Optional[int] = None
    last_timestamp: Optional[int] = None
    # Collected timestamps of metadata records (epoch seconds)
    timestamps: List[int] = field(default_factory=list)

    def ingest(self, record: Dict) -> None:
        status = int(record.get("http_status") or 0)
        bytes_value = int(record.get("content_bytes") or 0)
        page_type = record.get("page_type") or "unknown"

        self.total_docs += 1
        self.total_bytes += bytes_value
        self.status_counts[status] += 1

        if 200 <= status < 300:
            self.success_docs += 1
        else:
            self.failed_docs += 1

        bucket = self.page_types.setdefault(page_type, PageTypeStats())
        bucket.add(bytes_value)

        if (
            self.relevant_page_types
            and page_type in self.relevant_page_types
            and 200 <= status < 300
        ):
            self.relevant_docs += 1
            self.relevant_bytes += bytes_value

        ts = record.get("timestamp")
        if isinstance(ts, (int, float)):
            ts_int = int(ts)
            # collect timestamp for run segmentation
            self.timestamps.append(ts_int)
            if self.first_timestamp is None or ts_int < self.first_timestamp:
                self.first_timestamp = ts_int
            if self.last_timestamp is None or ts_int > self.last_timestamp:
                self.last_timestamp = ts_int


def load_service_stats(path: Path) -> Optional[Dict]:
    if not path.exists():
        return None
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception as exc:  # pragma: no cover - defensive
        print(f"[warn] Failed to parse service stats ({path}): {exc}", file=sys.stderr)
        return None


def stream_metadata(path: Path, verbose: bool = False) -> Iterable[Dict]:
    if not path.exists():
        raise FileNotFoundError(f"Metadata file not found: {path}")

    with path.open("r", encoding="utf-8") as handle:
        for idx, line in enumerate(handle, start=1):
            line = line.strip()
            if not line:
                continue
            try:
                yield json.loads(line)
            except json.JSONDecodeError as exc:
                print(
                    f"[warn] Skipping malformed JSONL line {idx}: {exc}",
                    file=sys.stderr,
                )
            if verbose and idx % 10000 == 0:
                print(f"[info] Processed {idx} metadata records...", file=sys.stderr)


def compute_run_segments(timestamps: List[int], gap_seconds: int = 300) -> List[Tuple[int, int, int]]:
    """Compute run segments from sorted timestamps.

    A run segment is a contiguous sequence of timestamps where gaps between
    consecutive timestamps are <= gap_seconds. Returns list of tuples
    (start_ts, end_ts, duration_seconds).
    """
    if not timestamps:
        return []

    ts_sorted = sorted(timestamps)
    segments: List[Tuple[int, int, int]] = []
    start = ts_sorted[0]
    prev = start

    for t in ts_sorted[1:]:
        if t - prev > gap_seconds:
            # gap larger than threshold -> close previous run
            segments.append((start, prev, prev - start))
            start = t
        prev = t

    # close final run
    segments.append((start, prev, prev - start))
    return segments


def compute_token_stats(
    text_root: Path, model: str, limit: Optional[int], verbose: bool = False
) -> Tuple[Optional[Dict[str, float]], Optional[str]]:
    try:
        import tiktoken  # type: ignore
    except ImportError:
        return None, "tiktoken not installed – skipping token statistics"

    if not text_root.exists():
        return None, f"text root does not exist: {text_root}"

    encoding = tiktoken.get_encoding(model)
    total_tokens = 0
    total_chars = 0
    processed_files = 0

    txt_paths = sorted(text_root.rglob("*.txt"))
    for path in txt_paths:
        if limit is not None and processed_files >= limit:
            break
        try:
            text = path.read_text("utf-8", errors="ignore")
        except Exception as exc:  # pragma: no cover - defensive
            if verbose:
                print(f"[warn] Failed to read {path}: {exc}", file=sys.stderr)
            continue

        tokens = encoding.encode(text)
        total_tokens += len(tokens)
        total_chars += len(text)
        processed_files += 1

        if verbose and processed_files % 100 == 0:
            print(
                f"[info] Tokenised {processed_files} text files...", file=sys.stderr
            )

    if processed_files == 0:
        return None, "no text files processed for token stats"

    avg_tokens = total_tokens / processed_files
    avg_chars = total_chars / processed_files if processed_files else 0.0
    return (
        {
            "files": processed_files,
            "tokens": total_tokens,
            "avg_tokens": avg_tokens,
            "avg_chars": avg_chars,
        },
        None,
    )


def write_markdown(
    path: Path,
    stats: CrawlStats,
    service_stats: Optional[Dict],
    token_stats: Optional[Dict[str, float]],
    run_segments: Optional[List[Tuple[int, int, int]]] = None,
) -> None:
    lines: List[str] = []
    lines.append("# Crawl Statistics Report\n")
    lines.append("## Summary\n")
    summary_rows = [
        ("Total documents", f"{stats.total_docs}"),
        ("Successful (2xx)", f"{stats.success_docs}"),
        ("Failed (!=2xx)", f"{stats.failed_docs}"),
        ("Total size", f"{format_bytes(stats.total_bytes)} ({stats.total_bytes:,} bytes)"),
    ]
    if stats.relevant_page_types:
        summary_rows.append(
            (
                "Relevant documents",
                f"{stats.relevant_docs} (page types: {', '.join(sorted(stats.relevant_page_types))})",
            )
        )
    if stats.first_timestamp and stats.last_timestamp:
        summary_rows.append(
            (
                "Crawl window",
                f"{stats.first_timestamp} → {stats.last_timestamp}",
            )
        )

    lines.append("| Metric | Value |\n| --- | --- |\n")
    for key, value in summary_rows:
        lines.append(f"| {key} | {value} |\n")
    lines.append("\n")

    if service_stats:
        lines.append("## Service Stats Snapshot\n")
        runtime = service_stats.get("runtime_seconds")
        acceptance = service_stats.get("acceptance_rate_percent")
        lines.append("| Metric | Value |\n| --- | --- |\n")
        lines.append(
            f"| URLs fetched | {service_stats.get('urls_fetched', 'n/a')} |\n"
        )
        lines.append(
            f"| URLs enqueued | {service_stats.get('urls_enqueued', 'n/a')} |\n"
        )
        lines.append(
            f"| Policy denied | {service_stats.get('policy_denied', 'n/a')} |\n"
        )
        if runtime is not None:
            lines.append(f"| Runtime | {format_seconds(float(runtime))} |\n")
        if acceptance is not None:
            lines.append(f"| Acceptance rate | {float(acceptance):.2f}% |\n")
        lines.append("\n")

    lines.append("## Page Type Breakdown\n")
    lines.append(
        "| Page Type | Documents | Share | Total Size | Avg Size | Relevant |\n"
        "| --- | --- | --- | --- | --- | --- |\n"
    )
    for page_type, entry in sorted(
        stats.page_types.items(), key=lambda item: item[1].count, reverse=True
    ):
        share = (entry.count / stats.total_docs * 100.0) if stats.total_docs else 0.0
        relevant = (
            "yes"
            if stats.relevant_page_types and page_type in stats.relevant_page_types
            else ""
        )
        lines.append(
            f"| {page_type} | {entry.count} | {share:.2f}% | "
            f"{format_bytes(entry.total_bytes)} | "
            f"{format_bytes(int(entry.average_bytes))} | {relevant} |\n"
        )
    lines.append("\n")

    lines.append("## HTTP Status Distribution\n")
    lines.append("| Status | Count | Share |\n| --- | --- | --- |\n")
    for status, count in sorted(stats.status_counts.items()):
        share = (count / stats.total_docs * 100.0) if stats.total_docs else 0.0
        lines.append(f"| {status} | {count} | {share:.2f}% |\n")
    lines.append("\n")

    if token_stats:
        lines.append("## Token Statistics\n")
        lines.append("| Metric | Value |\n| --- | --- |\n")
        lines.append(f"| Processed text files | {int(token_stats['files'])} |\n")
        lines.append(f"| Total tokens | {int(token_stats['tokens'])} |\n")
        lines.append(f"| Avg tokens per file | {token_stats['avg_tokens']:.2f} |\n")
        lines.append(f"| Avg characters per file | {token_stats['avg_chars']:.2f} |\n")
        lines.append("\n")

    # Run segmentation summary
    if run_segments is not None:
        total_run_seconds = sum(seg[2] for seg in run_segments)
        lines.append("## Run Segments\n")
        lines.append(f"- Number of runs: {len(run_segments)}\n")
        lines.append(f"- Total runtime (sum of runs): {format_seconds(total_run_seconds)} ({total_run_seconds} s)\n")
        lines.append("\n")
        lines.append("| Run # | Start (epoch) | End (epoch) | Duration |\n| --- | ---: | ---: | ---: |\n")
        for idx, (start, end, dur) in enumerate(run_segments, start=1):
            lines.append(f"| {idx} | {start} | {end} | {format_seconds(dur)} ({dur}s) |\n")
        lines.append("\n")

    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("".join(lines), encoding="utf-8")


def write_csv(path: Path, stats: CrawlStats) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.writer(handle)
        writer.writerow(
            [
                "page_type",
                "documents",
                "share_percent",
                "total_bytes",
                "total_bytes_human",
                "avg_bytes",
                "avg_bytes_human",
                "relevant",
            ]
        )
        for page_type, entry in sorted(
            stats.page_types.items(), key=lambda item: item[1].count, reverse=True
        ):
            share = (entry.count / stats.total_docs * 100.0) if stats.total_docs else 0.0
            relevant = (
                "yes"
                if stats.relevant_page_types and page_type in stats.relevant_page_types
                else "no"
            )
            writer.writerow(
                [
                    page_type,
                    entry.count,
                    f"{share:.4f}",
                    entry.total_bytes,
                    format_bytes(entry.total_bytes),
                    f"{entry.average_bytes:.2f}",
                    format_bytes(int(entry.average_bytes)),
                    relevant,
                ]
            )


def print_console_summary(
    stats: CrawlStats,
    service_stats: Optional[Dict],
    token_stats: Optional[Dict[str, float]],
    run_segments: Optional[List[Tuple[int, int, int]]] = None,
) -> None:
    print("=== Crawl Statistics ===")
    print(f"Total documents:  {stats.total_docs}")
    print(f"Successful (2xx): {stats.success_docs}")
    print(f"Failed (!=2xx):  {stats.failed_docs}")
    print(f"Total size:       {format_bytes(stats.total_bytes)} ({stats.total_bytes:,} bytes)")
    if stats.relevant_page_types:
        print(
            f"Relevant docs:   {stats.relevant_docs} "
            f"(page types: {', '.join(sorted(stats.relevant_page_types))})"
        )
    if stats.first_timestamp and stats.last_timestamp:
        print(
            f"Crawl window:    {stats.first_timestamp} → {stats.last_timestamp}"
        )

    if service_stats:
        print("\n-- Service Stats Snapshot --")
        runtime = service_stats.get("runtime_seconds")
        acceptance = service_stats.get("acceptance_rate_percent")
        if runtime is not None:
            print(f"Runtime:         {format_seconds(float(runtime))}")
        if acceptance is not None:
            print(f"Acceptance rate: {float(acceptance):.2f}%")
        print(
            f"URLs fetched:    {service_stats.get('urls_fetched', 'n/a')} "
            f"(enqueued: {service_stats.get('urls_enqueued', 'n/a')}, "
            f"policy denied: {service_stats.get('policy_denied', 'n/a')})"
        )

    print("\n-- HTTP Status Distribution --")
    for status, count in sorted(stats.status_counts.items()):
        share = (count / stats.total_docs * 100.0) if stats.total_docs else 0.0
        print(f"{status:>5} : {count:>7} ({share:5.2f}%)")

    print("\n-- Top Page Types --")
    for page_type, entry in sorted(
        stats.page_types.items(), key=lambda item: item[1].count, reverse=True
    )[:10]:
        share = (entry.count / stats.total_docs * 100.0) if stats.total_docs else 0.0
        print(
            f"{page_type:<15} | {entry.count:>6} docs | "
            f"{share:5.2f}% | {format_bytes(entry.total_bytes)} total"
        )

    if token_stats:
        print("\n-- Token Statistics --")
        print(f"Processed text files : {int(token_stats['files'])}")
        print(f"Total tokens          : {int(token_stats['tokens'])}")
        print(f"Avg tokens / file     : {token_stats['avg_tokens']:.2f}")
        print(f"Avg chars / file      : {token_stats['avg_chars']:.2f}")

    # Print run segmentation summary if available
    if run_segments is not None:
        total_run_seconds = sum(seg[2] for seg in run_segments)
        print("\n-- Run Segments --")
        print(f"Number of runs  : {len(run_segments)}")
        print(f"Total run time  : {format_seconds(total_run_seconds)} ({total_run_seconds} s)")
        for idx, (start, end, dur) in enumerate(run_segments, start=1):
            print(f"Run {idx}: {start} → {end} (duration: {format_seconds(dur)})")


def main(argv: Optional[Sequence[str]] = None) -> int:
    args = parse_args(argv)

    workspace = Path(args.workspace).resolve()
    metadata_path = (
        Path(args.metadata_file).resolve()
        if args.metadata_file
        else workspace / "metadata" / "crawl_metadata.jsonl"
    )
    service_stats_path = (
        Path(args.service_stats_file).resolve()
        if args.service_stats_file
        else workspace / "state" / "service_stats.json"
    )

    relevant_page_types = set(args.relevant_page_types)

    stats = CrawlStats(relevant_page_types=relevant_page_types)
    try:
        for record in stream_metadata(metadata_path, verbose=args.verbose):
            stats.ingest(record)
    except FileNotFoundError as exc:
        print(f"[error] {exc}", file=sys.stderr)
        return 1

    service_stats = load_service_stats(service_stats_path)

    # Compute run segments from metadata timestamps. If metadata was produced
    # in multiple short runs, gaps longer than 5 minutes indicate separate runs.
    run_segments: Optional[List[Tuple[int, int, int]]] = None

    # default gap threshold: 5 minutes (300 seconds)
    gap_threshold = 300

    token_stats = None
    if args.text_root:
        token_stats, token_warning = compute_token_stats(
            Path(args.text_root).resolve(),
            model=args.token_model,
            limit=args.token_limit,
            verbose=args.verbose,
        )
        if token_warning:
            print(f"[info] {token_warning}", file=sys.stderr)

    # Only compute runs if we have at least one timestamp collected
    if getattr(stats, "timestamps", None):
        run_segments = compute_run_segments(stats.timestamps, gap_seconds=gap_threshold)

    print_console_summary(stats, service_stats, token_stats, run_segments)

    if args.markdown_output:
        write_markdown(Path(args.markdown_output), stats, service_stats, token_stats, run_segments=run_segments)
        print(f"\n[info] Markdown written to {args.markdown_output}")

    if args.csv_output:
        write_csv(Path(args.csv_output), stats)
        print(f"[info] CSV written to {args.csv_output}")

    return 0


if __name__ == "__main__":  # pragma: no cover
    sys.exit(main())
