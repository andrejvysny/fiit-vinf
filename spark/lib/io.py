"""I/O helpers for the Spark migration layer."""

from __future__ import annotations

import hashlib
import json
import logging
import shutil
from pathlib import Path
from typing import Iterable, Optional, Sequence, Tuple

from pyspark.sql import DataFrame, SparkSession


def _ensure_parent(path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)


def merge_part_files(
    parts_dir: Path,
    output_file: Path,
    *,
    has_header: bool = True,
    delete_parts: bool = True
) -> int:
    """
    Merge Spark part files into a single output file.

    This is the recommended approach for large datasets:
    1. Spark writes in parallel to multiple part files (fast, scalable)
    2. After job completes, merge parts into single file (I/O only, no Spark overhead)

    This avoids the performance bottleneck of coalesce(1) which forces all data
    through a single partition before writing.

    Args:
        parts_dir: Directory containing part-* files from Spark output
        output_file: Path to the final merged output file
        has_header: If True, preserve header from first file and skip from others
        delete_parts: If True, remove parts_dir after successful merge

    Returns:
        Number of lines written (excluding header if present)

    Reference:
        https://spark.apache.org/docs/latest/sql-data-sources-csv.html
    """
    log = logging.getLogger(__name__)

    if not parts_dir.exists():
        raise FileNotFoundError(f"Parts directory not found: {parts_dir}")

    # Find all part files, sorted for deterministic order
    part_files = sorted(parts_dir.glob("part-*"))

    if not part_files:
        # Check for _SUCCESS file but no parts (empty DataFrame)
        success_file = parts_dir / "_SUCCESS"
        if success_file.exists():
            log.warning(f"No part files in {parts_dir}, creating empty output")
            _ensure_parent(output_file)
            output_file.touch()
            if delete_parts:
                shutil.rmtree(parts_dir, ignore_errors=True)
            return 0
        raise FileNotFoundError(f"No part files found in {parts_dir}")

    log.info(f"Merging {len(part_files)} part files into {output_file}")

    _ensure_parent(output_file)
    lines_written = 0
    header_written = False

    with open(output_file, 'w', encoding='utf-8') as out:
        for i, part_file in enumerate(part_files):
            with open(part_file, 'r', encoding='utf-8') as inp:
                for line_no, line in enumerate(inp):
                    # Skip header from non-first files
                    if has_header and line_no == 0:
                        if not header_written:
                            out.write(line)
                            header_written = True
                        continue
                    out.write(line)
                    lines_written += 1

    log.info(f"Merged {lines_written} lines to {output_file}")

    if delete_parts:
        shutil.rmtree(parts_dir, ignore_errors=True)
        log.info(f"Cleaned up parts directory: {parts_dir}")

    return lines_written


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


def _prepare_output_dir(path: Path) -> None:
    """Remove any previous output at ``path`` so Spark can write a directory there."""
    if path.exists():
        if path.is_file():
            path.unlink()
        else:
            shutil.rmtree(path)


def write_tsv(
    df: DataFrame,
    path: Path,
    columns: Sequence[str],
    *,
    header: bool = False,
    single_file: bool = True,
    max_records_per_file: Optional[int] = None,
    merge_after_write: bool = False
) -> Path:
    """
    Write dataframe rows into TSV files.

    Output modes (in order of recommendation for large datasets):

    1. merge_after_write=True (RECOMMENDED for large datasets):
       - Spark writes parts in parallel (fast, scalable, no shuffle bottleneck)
       - After Spark finishes, parts are merged into single file via simple I/O
       - Best of both worlds: parallel write + single output file
       - Reference: https://spark.apache.org/docs/latest/sql-data-sources-csv.html

    2. single_file=False, merge_after_write=False:
       - Spark writes to directory with multiple part files
       - Fastest write, but output is a directory not a file
       - Use when downstream can handle partitioned input

    3. single_file=True (legacy, NOT recommended for large datasets):
       - Uses coalesce(1) which forces all data through one partition
       - Creates single-threaded bottleneck, can cause OOM on large data
       - Only use for small samples where convenience outweighs performance

    Args:
        df: DataFrame to write
        path: Output path (file for single_file/merge modes, directory otherwise)
        columns: Column names to write
        header: Whether to write header row
        single_file: If True, use coalesce(1) - slow for large data
        max_records_per_file: Limit records per part file
        merge_after_write: If True, write parts then merge (recommended for large data)

    Returns:
        Path to the output (file or directory depending on mode)
    """
    log = logging.getLogger(__name__)

    writer = (
        df.select(*columns)
        .write.mode("overwrite")
        .option("sep", "\t")
        .option("header", "true" if header else "false")
        .option("encoding", "UTF-8")
        .option("escape", "\"")
    )

    if max_records_per_file:
        writer = writer.option("maxRecordsPerFile", max_records_per_file)

    # Mode 1: Write parts in parallel, then merge (RECOMMENDED for large datasets)
    if merge_after_write:
        parts_dir = Path(str(path) + ".parts")
        _prepare_output_dir(parts_dir)
        parts_dir.mkdir(parents=True, exist_ok=True)

        log.info(f"Writing parts to {parts_dir} (will merge after)")
        writer.csv(str(parts_dir))

        # Merge parts into single file
        output_file = Path(path)
        if output_file.exists():
            if output_file.is_dir():
                shutil.rmtree(output_file)
            else:
                output_file.unlink()

        merge_part_files(parts_dir, output_file, has_header=header, delete_parts=True)
        return output_file

    # Mode 2: Legacy single_file with coalesce(1) - slow for large data
    if single_file:
        tmp_dir = Path(str(path) + ".tmpdir")
        if tmp_dir.exists():
            shutil.rmtree(tmp_dir)
        writer.coalesce(1).csv(str(tmp_dir))
        _move_single_part(tmp_dir, path)
        return Path(path)

    # Mode 3: Write parts to directory (no merge)
    target_dir = Path(str(path))
    _prepare_output_dir(target_dir)
    target_dir.mkdir(parents=True, exist_ok=True)
    writer.csv(str(target_dir))
    return target_dir


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
