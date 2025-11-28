"""
Unified Statistics Module for VINF Pipeline.

This module provides centralized stats collection and reporting
for all pipeline stages: Wiki extraction, HTML extraction, and Join.
"""

import json
import os
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional


# Default stats directory - use project root
def get_default_stats_dir() -> Path:
    """Get the default stats directory, accounting for Docker environment."""
    # Check if running in Docker container with mounted app
    if Path("/opt/app").exists():
        return Path("/opt/app/stats")
    # Fall back to relative path from this file's location
    this_file = Path(__file__).resolve()
    project_root = this_file.parent.parent.parent  # spark/lib/stats.py -> project root
    return project_root / "stats"


DEFAULT_STATS_DIR = get_default_stats_dir()


def ensure_stats_dir(stats_dir: Optional[Path] = None) -> Path:
    """Ensure the stats directory exists and return its path."""
    stats_path = stats_dir or DEFAULT_STATS_DIR
    stats_path.mkdir(parents=True, exist_ok=True)
    return stats_path


def get_run_id() -> str:
    """Generate a unique run ID based on timestamp."""
    return datetime.now().strftime("%Y%m%d_%H%M%S")


class PipelineStats:
    """
    Unified stats collector for pipeline stages.

    Usage:
        stats = PipelineStats("wiki_extraction")
        stats.set("pages_processed", 500)
        stats.set_nested("entities", "categories", 3114)
        stats.save()
    """

    def __init__(
        self,
        stage_name: str,
        stats_dir: Optional[Path] = None,
        run_id: Optional[str] = None
    ):
        self.stage_name = stage_name
        self.stats_dir = ensure_stats_dir(stats_dir)
        self.run_id = run_id or get_run_id()
        self.start_time = datetime.now()
        self.data: Dict[str, Any] = {
            "stage": stage_name,
            "run_id": self.run_id,
            "start_time": self.start_time.isoformat(),
            "end_time": None,
            "duration_seconds": None,
            "status": "running",
            "config": {},
            "inputs": {},
            "outputs": {},
            "entities": {},
            "performance": {},
            "errors": []
        }

    def set(self, key: str, value: Any) -> "PipelineStats":
        """Set a top-level stat value."""
        self.data[key] = value
        return self

    def set_nested(self, category: str, key: str, value: Any) -> "PipelineStats":
        """Set a nested stat value under a category."""
        if category not in self.data:
            self.data[category] = {}
        self.data[category][key] = value
        return self

    def set_config(self, **kwargs) -> "PipelineStats":
        """Set configuration values."""
        self.data["config"].update(kwargs)
        return self

    def set_inputs(self, **kwargs) -> "PipelineStats":
        """Set input metadata."""
        self.data["inputs"].update(kwargs)
        return self

    def set_outputs(self, **kwargs) -> "PipelineStats":
        """Set output metadata."""
        self.data["outputs"].update(kwargs)
        return self

    def set_entities(self, entity_type: str, count: int, **extra) -> "PipelineStats":
        """Set entity count for a specific type."""
        self.data["entities"][entity_type] = {
            "count": count,
            **extra
        }
        return self

    def add_error(self, error: str, context: Optional[str] = None) -> "PipelineStats":
        """Add an error to the stats."""
        self.data["errors"].append({
            "timestamp": datetime.now().isoformat(),
            "error": error,
            "context": context
        })
        return self

    def finalize(self, status: str = "completed") -> "PipelineStats":
        """Finalize the stats with end time and duration."""
        end_time = datetime.now()
        self.data["end_time"] = end_time.isoformat()
        self.data["duration_seconds"] = round(
            (end_time - self.start_time).total_seconds(), 2
        )
        self.data["status"] = status

        # Calculate success rate if there are errors
        error_count = len(self.data["errors"])
        total_items = self.data.get("inputs", {}).get("total_items", 0)
        if total_items > 0:
            self.data["performance"]["success_rate"] = round(
                (total_items - error_count) / total_items * 100, 2
            )
            self.data["performance"]["error_count"] = error_count

        return self

    def get_stats_path(self) -> Path:
        """Get the path for the stats file."""
        return self.stats_dir / f"{self.stage_name}.json"

    def get_history_path(self) -> Path:
        """Get the path for the history file."""
        history_dir = self.stats_dir / "history"
        history_dir.mkdir(parents=True, exist_ok=True)
        return history_dir / f"{self.stage_name}_{self.run_id}.json"

    def save(self) -> Path:
        """Save the stats to the stats directory."""
        # Save current stats (overwrite)
        stats_path = self.get_stats_path()
        with open(stats_path, 'w', encoding='utf-8') as f:
            json.dump(self.data, f, indent=2, ensure_ascii=False)

        # Save to history
        history_path = self.get_history_path()
        with open(history_path, 'w', encoding='utf-8') as f:
            json.dump(self.data, f, indent=2, ensure_ascii=False)

        return stats_path

    def to_dict(self) -> Dict[str, Any]:
        """Return the stats as a dictionary."""
        return self.data.copy()


def load_stats(stage_name: str, stats_dir: Optional[Path] = None) -> Optional[Dict[str, Any]]:
    """Load stats for a specific stage."""
    stats_path = ensure_stats_dir(stats_dir) / f"{stage_name}.json"
    if stats_path.exists():
        with open(stats_path, 'r', encoding='utf-8') as f:
            return json.load(f)
    return None


def load_all_stats(stats_dir: Optional[Path] = None) -> Dict[str, Dict[str, Any]]:
    """Load all available stage stats."""
    stats_path = ensure_stats_dir(stats_dir)
    all_stats = {}

    for stage in ["wiki_extraction", "html_extraction", "join", "lucene_index", "lucene_search", "index_comparison"]:
        stats = load_stats(stage, stats_path)
        if stats:
            all_stats[stage] = stats

    return all_stats


def generate_pipeline_summary(stats_dir: Optional[Path] = None) -> Dict[str, Any]:
    """Generate a summary of all pipeline stages."""
    all_stats = load_all_stats(stats_dir)

    summary = {
        "generated_at": datetime.now().isoformat(),
        "stages": {},
        "totals": {
            "total_duration_seconds": 0,
            "total_entities_extracted": 0,
            "total_wiki_pages": 0,
            "total_html_files": 0,
            "total_joins": 0,
            "overall_status": "unknown"
        }
    }

    statuses = []

    for stage_name, stats in all_stats.items():
        stage_summary = {
            "status": stats.get("status", "unknown"),
            "duration_seconds": stats.get("duration_seconds", 0),
            "run_id": stats.get("run_id"),
            "start_time": stats.get("start_time"),
            "end_time": stats.get("end_time")
        }

        if stage_name == "wiki_extraction":
            entities = stats.get("entities", {})
            stage_summary["pages_processed"] = entities.get("pages", {}).get("count", 0)
            stage_summary["categories"] = entities.get("categories", {}).get("count", 0)
            stage_summary["links"] = entities.get("links", {}).get("count", 0)
            stage_summary["infobox_fields"] = entities.get("infobox", {}).get("count", 0)
            stage_summary["abstracts"] = entities.get("abstracts", {}).get("count", 0)
            stage_summary["aliases"] = entities.get("aliases", {}).get("count", 0)
            stage_summary["text_files"] = entities.get("text_files", {}).get("count", 0)
            summary["totals"]["total_wiki_pages"] = stage_summary["pages_processed"]

        elif stage_name == "html_extraction":
            outputs = stats.get("outputs", {})
            entities = stats.get("entities", {})
            stage_summary["files_processed"] = stats.get("inputs", {}).get("total_files", 0)
            stage_summary["text_files_written"] = outputs.get("text_files_written", 0)
            stage_summary["total_entities"] = outputs.get("entities_written", 0)
            stage_summary["entity_types"] = list(entities.keys())
            summary["totals"]["total_html_files"] = stage_summary["files_processed"]
            summary["totals"]["total_entities_extracted"] = stage_summary["total_entities"]

        elif stage_name == "join":
            join_stats = stats.get("join_statistics", {})
            stage_summary["total_entities"] = join_stats.get("total_entities", 0)
            stage_summary["matched_entities"] = join_stats.get("matched_entities", 0)
            stage_summary["match_rate"] = join_stats.get("match_rate", 0)
            stage_summary["unique_wiki_pages"] = join_stats.get("unique_wiki_pages_joined", 0)
            stage_summary["unique_docs_matched"] = join_stats.get("unique_docs_with_wiki", 0)
            summary["totals"]["total_joins"] = stage_summary["matched_entities"]

        elif stage_name == "lucene_index":
            outputs = stats.get("outputs", {})
            index_stats = stats.get("index_stats", {})
            stage_summary["docs_indexed"] = outputs.get("docs_indexed", 0)
            stage_summary["docs_with_entities"] = outputs.get("docs_with_entities", 0)
            stage_summary["docs_with_wiki"] = outputs.get("docs_with_wiki", 0)
            stage_summary["total_entities"] = index_stats.get("total_entities", 0)
            stage_summary["total_wiki_matches"] = index_stats.get("total_wiki_matches", 0)
            summary["totals"]["total_docs_indexed"] = stage_summary["docs_indexed"]

        elif stage_name == "lucene_search":
            search_stats = stats.get("search_stats", {})
            stage_summary["total_queries"] = search_stats.get("total_queries", 0)
            stage_summary["total_results"] = search_stats.get("total_results", 0)
            stage_summary["avg_query_time_ms"] = search_stats.get("avg_query_time_ms", 0)
            stage_summary["query_types"] = search_stats.get("query_types", [])

        elif stage_name == "index_comparison":
            comp_metrics = stats.get("comparison_metrics", {})
            query_types = stats.get("query_types", {})
            stage_summary["queries_compared"] = stats.get("outputs", {}).get("queries_compared", 0)
            stage_summary["avg_tfidf_latency_ms"] = comp_metrics.get("avg_tfidf_latency_ms", 0)
            stage_summary["avg_lucene_latency_ms"] = comp_metrics.get("avg_lucene_latency_ms", 0)
            stage_summary["latency_improvement_pct"] = comp_metrics.get("latency_improvement_pct", 0)
            stage_summary["avg_overlap_at_5"] = comp_metrics.get("avg_overlap_at_5", 0)
            stage_summary["avg_overlap_at_10"] = comp_metrics.get("avg_overlap_at_10", 0)
            stage_summary["avg_overlap_at_20"] = comp_metrics.get("avg_overlap_at_20", 0)
            stage_summary["query_types"] = list(query_types.keys())

        summary["stages"][stage_name] = stage_summary
        summary["totals"]["total_duration_seconds"] += stats.get("duration_seconds", 0)
        statuses.append(stats.get("status", "unknown"))

    # Determine overall status
    if all(s == "completed" for s in statuses):
        summary["totals"]["overall_status"] = "completed"
    elif any(s == "error" for s in statuses):
        summary["totals"]["overall_status"] = "error"
    elif any(s == "running" for s in statuses):
        summary["totals"]["overall_status"] = "running"

    return summary


def generate_markdown_report(stats_dir: Optional[Path] = None) -> str:
    """Generate a human-readable markdown report of pipeline stats."""
    summary = generate_pipeline_summary(stats_dir)

    lines = [
        "# VINF Pipeline Statistics Report",
        "",
        f"**Generated:** {summary['generated_at']}",
        f"**Overall Status:** {summary['totals']['overall_status']}",
        f"**Total Duration:** {summary['totals']['total_duration_seconds']:.2f} seconds",
        "",
        "---",
        "",
    ]

    # Wiki Extraction
    if "wiki_extraction" in summary["stages"]:
        wiki = summary["stages"]["wiki_extraction"]
        lines.extend([
            "## 1. Wikipedia Extraction",
            "",
            f"- **Status:** {wiki.get('status', 'N/A')}",
            f"- **Duration:** {wiki.get('duration_seconds', 0):.2f} seconds",
            f"- **Pages Processed:** {wiki.get('pages_processed', 0):,}",
            "",
            "### Entities Extracted:",
            "",
            f"| Entity Type | Count |",
            f"|-------------|-------|",
            f"| Categories | {wiki.get('categories', 0):,} |",
            f"| Links | {wiki.get('links', 0):,} |",
            f"| Infobox Fields | {wiki.get('infobox_fields', 0):,} |",
            f"| Abstracts | {wiki.get('abstracts', 0):,} |",
            f"| Aliases | {wiki.get('aliases', 0):,} |",
            f"| Text Files | {wiki.get('text_files', 0):,} |",
            "",
        ])

    # HTML Extraction
    if "html_extraction" in summary["stages"]:
        html = summary["stages"]["html_extraction"]
        lines.extend([
            "## 2. HTML Extraction",
            "",
            f"- **Status:** {html.get('status', 'N/A')}",
            f"- **Duration:** {html.get('duration_seconds', 0):.2f} seconds",
            f"- **Files Processed:** {html.get('files_processed', 0):,}",
            f"- **Text Files Written:** {html.get('text_files_written', 0):,}",
            f"- **Total Entities:** {html.get('total_entities', 0):,}",
            "",
        ])

    # Join
    if "join" in summary["stages"]:
        join = summary["stages"]["join"]
        lines.extend([
            "## 3. Entity-Wiki Join",
            "",
            f"- **Status:** {join.get('status', 'N/A')}",
            f"- **Duration:** {join.get('duration_seconds', 0):.2f} seconds",
            f"- **Total Entities:** {join.get('total_entities', 0):,}",
            f"- **Matched Entities:** {join.get('matched_entities', 0):,}",
            f"- **Match Rate:** {join.get('match_rate', 0):.2f}%",
            f"- **Unique Wiki Pages Joined:** {join.get('unique_wiki_pages', 0):,}",
            f"- **Unique Documents with Matches:** {join.get('unique_docs_matched', 0):,}",
            "",
        ])

    # Summary
    lines.extend([
        "---",
        "",
        "## Summary",
        "",
        f"| Metric | Value |",
        f"|--------|-------|",
        f"| Total Duration | {summary['totals']['total_duration_seconds']:.2f}s |",
        f"| Wiki Pages Processed | {summary['totals']['total_wiki_pages']:,} |",
        f"| HTML Files Processed | {summary['totals']['total_html_files']:,} |",
        f"| Total Entities Extracted | {summary['totals']['total_entities_extracted']:,} |",
        f"| Total Joins | {summary['totals']['total_joins']:,} |",
        "",
    ])

    return "\n".join(lines)


def save_pipeline_summary(stats_dir: Optional[Path] = None) -> tuple[Path, Path]:
    """Generate and save the pipeline summary in both JSON and Markdown formats."""
    stats_path = ensure_stats_dir(stats_dir)

    # Generate and save JSON summary
    summary = generate_pipeline_summary(stats_path)
    json_path = stats_path / "pipeline_summary.json"
    with open(json_path, 'w', encoding='utf-8') as f:
        json.dump(summary, f, indent=2, ensure_ascii=False)

    # Generate and save Markdown report
    report = generate_markdown_report(stats_path)
    md_path = stats_path / "pipeline_summary.md"
    with open(md_path, 'w', encoding='utf-8') as f:
        f.write(report)

    return json_path, md_path
