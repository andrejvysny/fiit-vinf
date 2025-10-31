from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, Optional

from config_loader import get_section

from .idf import DEFAULT_IDF_METHOD


def _resolve_config_path(
    workspace: Optional[str], value: Optional[str], default_relative: str
) -> str:
    base = Path(workspace or "./workspace")
    if value:
        candidate = Path(value)
        if candidate.is_absolute():
            return str(candidate.resolve())
        parts = candidate.parts
        if parts and parts[0] == base.name:
            candidate = Path(*parts[1:]) if len(parts) > 1 else Path(".")
        return str((base / candidate).resolve())
    return str((base / default_relative).resolve())


@dataclass
class IndexerBuildConfig:
    input_dir: str
    output_dir: str
    limit: Optional[int] = None
    dry_run: bool = False
    use_tokens: bool = False
    token_model: str = "cl100k_base"


@dataclass
class IndexerQueryConfig:
    index_dir: str
    top_k: int = 10
    idf_method: str = "manifest"
    show_path: bool = False
    # Directory (relative to the project root, or absolute) where query
    # reports are written. Defaults to `reports/query` at the project root.
    # If empty or None, reporting can be disabled via write_report.
    report_dir: str = "reports/query"
    # Whether to write a markdown report for each query run.
    write_report: bool = True


@dataclass
class IndexerConfig:
    build: IndexerBuildConfig
    query: IndexerQueryConfig

    @classmethod
    def from_app_config(cls, config: Dict[str, Any]) -> "IndexerConfig":
        workspace = config.get("workspace")
        indexer_section = get_section(config, "indexer")

        if "build" in indexer_section:
            build_section = get_section(indexer_section, "build")
        else:
            build_section = indexer_section

        query_section = get_section(indexer_section, "query")

        input_dir = _resolve_config_path(
            workspace, build_section.get("input_dir") or indexer_section.get("input_dir"), "store/text"
        )
        output_dir = _resolve_config_path(
            workspace, build_section.get("output_dir") or indexer_section.get("output_dir"), "store/index/default"
        )
        limit = build_section.get("limit") if "limit" in build_section else indexer_section.get("limit")
        dry_run = bool(build_section.get("dry_run", indexer_section.get("dry_run", False)))
        use_tokens = bool(build_section.get("use_tokens", indexer_section.get("use_tokens", False)))
        token_model = build_section.get("token_model") or indexer_section.get("token_model") or "cl100k_base"

        build_cfg = IndexerBuildConfig(
            input_dir=input_dir,
            output_dir=output_dir,
            limit=limit,
            dry_run=dry_run,
            use_tokens=use_tokens,
            token_model=token_model,
        )

        raw_query_index = query_section.get("index_dir")
        if raw_query_index is not None:
            query_index = _resolve_config_path(workspace, raw_query_index, "store/index/default")
        else:
            query_index = output_dir
        query_top = query_section.get("top_k", 10)
        query_idf = query_section.get("idf_method", "manifest")
        query_show_path = bool(query_section.get("show_path", False))
        query_report_dir = query_section.get("report_dir", "query_reports")
        query_write_report = bool(query_section.get("write_report", True))

        query_cfg = IndexerQueryConfig(
            index_dir=query_index,
            top_k=query_top,
            idf_method=query_idf,
            show_path=query_show_path,
            report_dir=query_report_dir,
            write_report=query_write_report,
        )

        return cls(build=build_cfg, query=query_cfg)
