"""Lightweight Spark progress helpers.

Uses SparkStatusTracker to emit heartbeat logs while batch jobs run. This is
useful for long-running actions (e.g., large writes) where the Spark driver
logs stay quiet. Intended for Spark 3.x DataFrame workloads.
"""

from __future__ import annotations

import logging
import threading
import uuid
from typing import Iterable, Optional

from pyspark.sql import SparkSession


def _safe_get(info: object, candidates: Iterable[str]) -> Optional[int]:
    """Best-effort getter that tolerates Spark 3.x API differences."""
    for name in candidates:
        attr = getattr(info, name, None)
        if attr is None:
            continue
        try:
            return attr() if callable(attr) else attr
        except Exception:
            continue
    return None


class SparkProgressReporter:
    """Background thread that logs active Spark stage/task progress."""

    def __init__(self, spark: SparkSession, logger: logging.Logger, interval: float = 30.0) -> None:
        self.spark = spark
        self.logger = logger
        self.interval = interval
        self._status_tracker = spark.sparkContext.statusTracker()
        self._stop_event = threading.Event()
        self._thread: Optional[threading.Thread] = None
        self._label = "spark"

    def start(self, label: str = "spark") -> None:
        self._label = label
        if self._thread and self._thread.is_alive():
            return
        self._stop_event.clear()
        self._thread = threading.Thread(
            target=self._run, name="spark-progress-reporter", daemon=True
        )
        self._thread.start()

    def stop(self) -> None:
        self._stop_event.set()
        if self._thread:
            self._thread.join(timeout=5)

    def _run(self) -> None:
        while not self._stop_event.wait(self.interval):
            try:
                active_stage_ids = self._status_tracker.getActiveStageIds()
            except Exception:
                # Spark context was likely stopped; exit silently.
                break

            if not active_stage_ids:
                self.logger.info(f"[{self._label}] Waiting for Spark jobs to report progress...")
                continue

            summaries = []
            for stage_id in sorted(active_stage_ids):
                info = self._status_tracker.getStageInfo(stage_id)
                if info is None:
                    continue

                name = _safe_get(info, ["name"]) or "stage"
                total = _safe_get(info, ["numTasks"])
                completed = _safe_get(info, ["numCompletedTasks", "numCompleteTasks"]) or 0
                active = _safe_get(info, ["numActiveTasks"]) or 0
                failed = _safe_get(info, ["numFailedTasks"]) or 0
                summaries.append(
                    f"{stage_id} ({name}): {completed}/{total or '?'} done, active={active}, failed={failed}"
                )

            if summaries:
                joined = "; ".join(summaries)
                self.logger.info(f"[{self._label}] Active Spark stages -> {joined}")


class JobGroup:
    """Context manager to label Spark jobs with a friendly description."""

    def __init__(self, spark: SparkSession, description: str, group_id: Optional[str] = None) -> None:
        self.spark = spark
        self.description = description
        self.group_id = group_id or f"job-{uuid.uuid4()}"

    def __enter__(self) -> "JobGroup":
        self.spark.sparkContext.setJobGroup(
            self.group_id, self.description, interruptOnCancel=True
        )
        # Spark 3.4 exposes setJobDescription for UI clarity; ignore if unavailable.
        try:
            self.spark.sparkContext.setJobDescription(self.description)
        except Exception:
            pass
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        try:
            self.spark.sparkContext.clearJobGroup()
            self.spark.sparkContext.setJobDescription(None)
        except Exception:
            pass
