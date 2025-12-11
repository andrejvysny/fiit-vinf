"""Interactive CLI for running ad-hoc searches against the index."""

from __future__ import annotations

import argparse
import sys
from pathlib import Path
from typing import Optional

from config_loader import ConfigError, load_yaml_config

from .config import IndexerConfig
from .idf import IDFRegistry
from .search import SearchEngine
from datetime import datetime, timezone
import re
from pathlib import Path
import json
import time


IDF_REGISTRY = IDFRegistry()
MANIFEST_TOKEN = "manifest"


def _parse_idf_arg(value: str) -> str:
    token = value.strip().lower()
    if token == MANIFEST_TOKEN:
        return MANIFEST_TOKEN
    return IDF_REGISTRY.ensure(token)


def parse_args(argv: Optional[list[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Query the inverted index.")
    parser.add_argument(
        "--config",
        default="config.yml",
        help="Path to unified configuration file (default: %(default)s).",
    )
    parser.add_argument(
        "--index",
        dest="index_dir",
        default=None,
        help="Path to the index directory (defaults to indexer.query.index_dir).",
    )
    parser.add_argument(
        "--query",
        required=True,
        help="Search query (plain text).",
    )
    parser.add_argument(
        "--top",
        type=int,
        default=None,
        help="Number of results to return (defaults to indexer.query.top_k).",
    )
    parser.add_argument(
        "--idf-method",
        default=None,
        type=_parse_idf_arg,
        help=(
            "IDF weighting strategy (defaults to indexer.query.idf_method). "
            "'manifest' uses stored weights from build time. "
            f"Supported methods: {', '.join(IDF_REGISTRY.supported_methods)}."
        ),
    )
    parser.add_argument(
        "--show-path",
        action="store_true",
        help="Display full file paths in the output.",
    )
    return parser.parse_args(argv)


def main(argv: Optional[list[str]] = None) -> int:

    # start overall program timer
    start_total = time.perf_counter()
    args = parse_args(argv)
    t_after_args = time.perf_counter()

    # measure config load
    config_start = time.perf_counter()
    try:
        app_config = load_yaml_config(args.config)
    except ConfigError as exc:
        total_elapsed = time.perf_counter() - start_total
        print(f"[error] {exc}", file=sys.stderr)
        print(f"Timing (s): args_parse={t_after_args - start_total:.6f}, total={total_elapsed:.6f}", file=sys.stderr)
        return 1
    config_elapsed = time.perf_counter() - config_start

    idxcfg_start = time.perf_counter()
    indexer_config = IndexerConfig.from_app_config(app_config)
    idxcfg_elapsed = time.perf_counter() - idxcfg_start
    query_cfg = indexer_config.query

    index_dir_str = args.index_dir or query_cfg.index_dir
    index_dir = Path(index_dir_str).resolve()
    # measure index loading / SearchEngine construction
    index_load_start = time.perf_counter()
    try:
        search_index = SearchEngine(index_dir)
    except FileNotFoundError as exc:
        total_elapsed = time.perf_counter() - start_total
        # Print a friendly error and timing information, then exit.
        print(f"[error] {exc}", file=sys.stderr)
        print(
            "Index not found. Build the index first with: python -m indexer.build --config config.yml",
            file=sys.stderr,
        )
        print(f"Timing (s): args_parse={t_after_args - start_total:.6f}, total={total_elapsed:.6f}", file=sys.stderr)
        return 2
    index_load_elapsed = time.perf_counter() - index_load_start

    idf_setting = args.idf_method or query_cfg.idf_method
    if idf_setting is None:
        idf_setting = MANIFEST_TOKEN
    if idf_setting != MANIFEST_TOKEN:
        idf_setting = _parse_idf_arg(str(idf_setting))
    top_k = args.top if args.top is not None else query_cfg.top_k
    show_path = args.show_path or query_cfg.show_path

    use_stored_idf = idf_setting == MANIFEST_TOKEN
    effective_method = None if use_stored_idf else idf_setting

    # Measure query execution time for reporting and quick diagnostics
    start = time.perf_counter()
    results = search_index.search(
        args.query,
        top_k=top_k,
        idf_method=effective_method,
        use_stored_idf=use_stored_idf,
    )
    elapsed = time.perf_counter() - start

    if not results:
        print("No results found.")
        print(f"Elapsed: {elapsed:.6f} seconds")
        return 0

    # Print a compact header describing the output meaning and context
    effective_idf_name = (
        idf_setting if idf_setting != MANIFEST_TOKEN else search_index.default_idf_method
    )

    print("=" * 80)
    print(f"Query: {args.query!r}")
    print(f"IDF method: {effective_idf_name} | results: {len(results)} | index: {index_dir}")
    print(f"Elapsed: {elapsed:.6f} seconds")
    print(f"Generated: {datetime.now(timezone.utc).isoformat()}")
    print()
    print("Fields:")
    print("  - score: ranking score (higher is better)")
    print("  - doc: internal document id")
    print("  - length: document length (index tokens)")
    print("  - tokenize/tiktoken: token counts when available")
    print("  - matched_terms: term(freq) pairs contributing to score")
    print("=" * 80)
    print()

    # Prepare markdown report
    report_lines = []
    report_lines.append("# Query report")
    report_lines.append("")
    report_lines.append(f"- Query: `{args.query}`")
    report_lines.append(f"- IDF method: `{effective_idf_name}`")
    report_lines.append(f"- Results: {len(results)}")
    report_lines.append(f"- Index directory: `{index_dir}`")
    report_lines.append(f"- Query time (s): {elapsed:.6f}")
    report_lines.append(f"- Generated: `{datetime.now(timezone.utc).isoformat()}`")
    report_lines.append("")

    # Determine reports directory from config. By default, put reports under
    # the project root in `reports/query`. If the configured path is
    # absolute, use it. If it's relative, interpret it relative to the
    # project root (not the index directory) so all reports land in the
    # repository-level `reports/` tree.
    reports_dir = None
    try:
        if getattr(query_cfg, "write_report", True):
            raw_reports = getattr(query_cfg, "report_dir", None)
            if raw_reports:
                raw_reports_path = Path(raw_reports)
                if raw_reports_path.is_absolute():
                    reports_dir = raw_reports_path
                else:
                    # place relative paths under project root
                    reports_dir = Path.cwd().resolve() / raw_reports_path
            else:
                # default location at project root: reports/query
                reports_dir = Path.cwd().resolve() / "reports" / "query"
            reports_dir.mkdir(parents=True, exist_ok=True)
    except Exception:
        reports_dir = None

    for rank, result in enumerate(results, start=1):
        term_summary = ", ".join(f"{term}({tf})" for term, tf in result.matched_terms.items())
        metadata_parts = [f"len={result.length}"]
        if result.tokenize_count is not None:
            metadata_parts.append(f"tokenize={result.tokenize_count}")
        if result.token_count is not None:
            metadata_parts.append(f"tiktoken={result.token_count}")
        metadata = ", ".join(metadata_parts)

        # Compute a relative path for display
        doc_path = Path(result.path)
        rel_path = None
        try:
            # prefer path relative to index dir parent (likely workspace/store)
            rel_path = doc_path.relative_to(index_dir.parent)
        except Exception:
            try:
                rel_path = doc_path.relative_to(Path.cwd())
            except Exception:
                rel_path = doc_path

        # Print to stdout with improved structure
        print("-" * 60)
        print(f"{rank}. {result.title}")
        print(f"   score: {result.score:.6f}")
        print(f"   doc_id: {result.doc_id}")
        print(f"   length: {result.length}")
        if result.tokenize_count is not None:
            print(f"   tokenize: {result.tokenize_count}")
        if result.token_count is not None:
            print(f"   tiktoken: {result.token_count}")
        if term_summary:
            print("   matched_terms:")
            for t, tf in result.matched_terms.items():
                print(f"     - {t}: {tf}")
        # Always show a relative path for compact display. Show the absolute
        # path only when the user requested it via --show-path.
        if show_path:
            print(f"   path: {doc_path}")
        print(f"   rel:  {rel_path}")
        print()

        # Add to markdown report (clear structure)
        report_lines.append(f"## {rank}. {result.title}")
        report_lines.append("")
        report_lines.append("```json")
        meta_block = {
            "doc_id": result.doc_id,
            "score": float(f"{result.score:.6f}"),
            "length": result.length,
        }
        if result.tokenize_count is not None:
            meta_block["tokenize"] = result.tokenize_count
        if result.token_count is not None:
            meta_block["tiktoken"] = result.token_count
        report_lines.append(json.dumps(meta_block, ensure_ascii=False, indent=2))
        report_lines.append("```")
        report_lines.append("")
        report_lines.append(f"- path: `{doc_path}`")
        report_lines.append(f"- rel_path: `{rel_path}`")
        if term_summary:
            report_lines.append("")
            report_lines.append("### Matched terms")
            report_lines.append("")
            for t, tf in result.matched_terms.items():
                report_lines.append(f"- `{t}`: {tf}")
        report_lines.append("")

    # Save markdown report if reporting is enabled and we have a directory
    report_elapsed = 0.0
    if reports_dir is not None:
        safe_q = re.sub(r"[^0-9A-Za-z_.-]", "_", args.query)[:200]
        # use timezone-aware UTC timestamp for filenames too
        timestamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
        report_name = f"query_{safe_q}_{timestamp}.md"
        report_path = reports_dir / report_name
        try:
            report_start = time.perf_counter()
            report_path.write_text("\n".join(report_lines), encoding="utf-8")
            report_elapsed = time.perf_counter() - report_start
            print(f"Saved detailed report to: {report_path}")
        except Exception as exc:
            print(f"Warning: failed to write report to {report_path}: {exc}", file=sys.stderr)


    # END TIMER and print actual runtime
    total_elapsed = time.perf_counter() - start_total

    # Build timing breakdown
    args_parse_elapsed = t_after_args - start_total
    timings = {
        "args_parse": args_parse_elapsed,
        "config_load": config_elapsed,
        "indexer_config": idxcfg_elapsed,
        "index_load": index_load_elapsed,
        "search": elapsed,
        "report_write": report_elapsed,
        "total": total_elapsed,
    }

    # Print a readable timing summary to stdout
    print()
    print("Timing breakdown (seconds):")
    for k in ("args_parse", "config_load", "indexer_config", "index_load", "search", "report_write", "total"):
        print(f"  {k}: {timings[k]:.6f}")

    # Append timing breakdown to the markdown report if it was written
    if reports_dir is not None:
        try:
            with report_path.open("a", encoding="utf-8") as f:
                f.write("\n")
                f.write("## Timing breakdown\n\n")
                f.write("```json\n")
                f.write(json.dumps(timings, ensure_ascii=False, indent=2))
                f.write("\n```\n")
            print(f"Appended timing breakdown to: {report_path}")
        except Exception as exc:
            print(f"Warning: failed to append timing to {report_path}: {exc}", file=sys.stderr)

    print(f"Total elapsed (program): {total_elapsed:.6f} seconds")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
