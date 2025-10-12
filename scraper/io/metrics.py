"""
Metrics tracking and CSV output
"""

import csv
import time
from pathlib import Path
from collections import Counter
from typing import Dict, Any


class Metrics:
    """
    Track scraper metrics and flush to CSV periodically.
    """

    def __init__(self, csv_path: str, flush_interval_sec: int = 10):
        """
        Initialize metrics tracker.

        Args:
            csv_path: Path to metrics CSV file
            flush_interval_sec: Flush interval in seconds
        """
        self.csv_path = Path(csv_path)
        self.csv_path.parent.mkdir(parents=True, exist_ok=True)
        self.flush_interval = flush_interval_sec

        # Counters
        self.counters = Counter()

        # Timing
        self.last_flush = time.time()
        self.start_time = time.time()

        # Write header if new file
        if not self.csv_path.exists():
            with open(self.csv_path, 'w', newline='', encoding='utf-8') as f:
                writer = csv.writer(f)
                writer.writerow(["timestamp", "metric", "value"])

    def inc(self, key: str, n: int = 1):
        """
        Increment a counter.

        Args:
            key: Metric name
            n: Amount to increment
        """
        self.counters[key] += n

    def set(self, key: str, value: int):
        """
        Set a counter value.

        Args:
            key: Metric name
            value: Value to set
        """
        self.counters[key] = value

    def maybe_flush(self) -> bool:
        """
        Flush metrics if interval has passed.

        Returns:
            True if flushed
        """
        now = time.time()
        if now - self.last_flush >= self.flush_interval:
            self.flush()
            return True
        return False

    def flush(self):
        """Force flush metrics to CSV"""
        if not self.counters:
            return

        timestamp = int(time.time())

        with open(self.csv_path, 'a', newline='', encoding='utf-8') as f:
            writer = csv.writer(f)
            for key, value in sorted(self.counters.items()):
                writer.writerow([timestamp, key, value])

        self.last_flush = time.time()

    def get_stats(self) -> Dict[str, Any]:
        """Get current metrics"""
        stats = dict(self.counters)
        stats['_runtime'] = int(time.time() - self.start_time)
        return stats

    def get_summary(self) -> str:
        """Get formatted summary of metrics"""
        lines = ["Metrics Summary:"]

        # Group metrics by category
        fetch_metrics = {}
        status_metrics = {}
        other_metrics = {}

        for key, value in sorted(self.counters.items()):
            if key.startswith("status_"):
                status_metrics[key] = value
            elif key in ["pages_fetched", "pages_saved", "bytes_saved", "not_html",
                        "not_modified_304", "retries_total"]:
                fetch_metrics[key] = value
            else:
                other_metrics[key] = value

        # Display fetch metrics
        if fetch_metrics:
            lines.append("\nFetch Metrics:")
            for key, value in fetch_metrics.items():
                if key == "bytes_saved":
                    lines.append(f"  {key}: {value:,} ({value / (1024**2):.2f} MB)")
                else:
                    lines.append(f"  {key}: {value:,}")

        # Display status codes
        if status_metrics:
            lines.append("\nHTTP Status Codes:")
            for key, value in status_metrics.items():
                lines.append(f"  {key}: {value:,}")

        # Display other metrics
        if other_metrics:
            lines.append("\nOther Metrics:")
            for key, value in other_metrics.items():
                lines.append(f"  {key}: {value:,}")

        # Add runtime
        runtime = int(time.time() - self.start_time)
        lines.append(f"\nRuntime: {runtime}s")

        return "\n".join(lines)