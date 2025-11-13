"""I/O helpers for the Spark migration layer."""

from __future__ import annotations

import hashlib
import json
import shutil
from pathlib import Path
from typing import Iterable, Optional, Sequence, Tuple

from pyspark.sql import DataFrame, SparkSession


def _ensure_parent(path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)


def _move_single_part(tmp_dir: Path, target_path: Path) -> None:
    """Rename the single part file produced by Spark into ``target_path``."""
    part_files = list(tmp_dir.glob("part-*"))
    if not part_files:
        raise FileNotFoundError(f"No part files produced under {tmp_dir}")
    if len(part_files) > 1:
        raise RuntimeError(f"Expected 1 part file under {tmp_dir}, found {len(part_files)}")
    _ensure_parent(target_path)
    part_files[0].rename(target_path)
    shutil.rmtree(tmp_dir, ignore_errors=True)


def write_tsv(df: DataFrame, path: Path, columns: Sequence[str], *, header: bool = False) -> None:
    """Write dataframe rows into a single UTF-8 TSV file."""
    tmp_dir = Path(str(path) + ".tmpdir")
    if tmp_dir.exists():
        shutil.rmtree(tmp_dir)
    (
        df.select(*columns)
        .coalesce(1)
        .write.mode("overwrite")
        .option("sep", "\t")
        .option("quote", "\u0000")
        .option("header", "true" if header else "false")
        .option("encoding", "UTF-8")
        .csv(str(tmp_dir))
    )
    _move_single_part(tmp_dir, path)


def write_ndjson(df: DataFrame, path: Path, columns: Optional[Sequence[str]] = None) -> None:
    """Materialize DF rows into newline-delimited JSON."""
    target = Path(path)
    tmp_dir = Path(str(path) + ".tmpdir")
    if tmp_dir.exists():
        shutil.rmtree(tmp_dir)
    payload = df.select(*columns) if columns else df
    (
        payload.coalesce(1)
        .toJSON()
        .coalesce(1)
        .write.mode("overwrite")
        .text(str(tmp_dir))
    )
    _move_single_part(tmp_dir, target)


def compute_sha1(path: Path) -> str:
    """Return the SHA-1 digest of ``path``."""
    sha = hashlib.sha1()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            sha.update(chunk)
    return sha.hexdigest()


def detect_metadata_format(path: Path, explicit: Optional[str] = None) -> str:
    if explicit:
        return explicit
    suffix = path.suffix.lower()
    if suffix in {".tsv", ".csv"}:
        return "tsv"
    if suffix in {".jsonl", ".json"}:
        return "json"
    return "json"


def read_metadata(spark: SparkSession, path: Path, *, fmt: Optional[str] = None) -> DataFrame:
    fmt = detect_metadata_format(path, fmt)
    if fmt == "tsv":
        return (
            spark.read.format("csv")
            .option("sep", "\t")
            .option("header", "true")
            .option("encoding", "UTF-8")
            .load(str(path))
        )
    # Default: JSON/JSONL
    return spark.read.json(str(path))


def read_tsv(spark: SparkSession, path: Path, *, header: bool = True) -> DataFrame:
    """Read a UTF-8 TSV file."""
    return (
        spark.read.format("csv")
        .option("sep", "\t")
        .option("header", "true" if header else "false")
        .option("encoding", "UTF-8")
        .load(str(path))
    )


def read_ndjson(spark: SparkSession, path: Path) -> DataFrame:
    """Read newline-delimited JSON (NDJSON)."""
    return spark.read.json(str(path))
