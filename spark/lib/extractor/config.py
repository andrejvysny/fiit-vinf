"""Extractor configuration from YAML config."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, Optional

from config_loader import get_section


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
class ExtractorConfig:
    input_root: str
    text_out: Optional[str]
    entities_out: Optional[str]
    limit: Optional[int] = None
    sample: Optional[int] = None
    force: bool = False
    dry_run: bool = False
    verbose: bool = False
    enable_text: bool = True
    enable_entities: bool = True

    @classmethod
    def from_app_config(cls, config: Dict[str, Any]) -> "ExtractorConfig":
        workspace = config.get("workspace")
        extractor_section = get_section(config, "extractor")
        outputs_section = get_section(extractor_section, "outputs") if "outputs" in extractor_section else {}

        input_root = _resolve_config_path(workspace, extractor_section.get("input_root"), "store/html")
        text_out = _resolve_config_path(workspace, outputs_section.get("text") or extractor_section.get("text_out"), "store/text")
        entities_out = _resolve_config_path(workspace, outputs_section.get("entities") or extractor_section.get("entities_out"), "store/entities/entities.tsv")

        return cls(
            input_root=input_root,
            text_out=text_out,
            entities_out=entities_out,
            limit=extractor_section.get("limit"),
            sample=extractor_section.get("sample"),
            force=bool(extractor_section.get("force", False)),
            dry_run=bool(extractor_section.get("dry_run", False)),
            verbose=bool(extractor_section.get("verbose", False)),
            enable_text=bool(extractor_section.get("enable_text", True)),
            enable_entities=bool(extractor_section.get("enable_entities", True)),
        )
