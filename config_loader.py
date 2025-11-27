from __future__ import annotations

from pathlib import Path
from typing import Any, Dict

import yaml


class ConfigError(Exception):
    """Raised when the configuration file is missing or malformed."""


def load_yaml_config(path: str) -> Dict[str, Any]:
    """Load a YAML configuration file into a dictionary."""
    config_path = Path(path).expanduser()
    if not config_path.exists():
        raise ConfigError(f"Configuration file not found: {config_path}")

    with config_path.open("r", encoding="utf-8") as handle:
        data = yaml.safe_load(handle) or {}

    if not isinstance(data, dict):
        raise ConfigError("Configuration root must be a mapping.")

    return data


def get_section(config: Dict[str, Any], section: str) -> Dict[str, Any]:
    """Return a configuration section, ensuring it is a mapping."""
    value = config.get(section, {})
    if value is None:
        return {}
    if not isinstance(value, dict):
        raise ConfigError(f"Section '{section}' must be a mapping.")
    return value
