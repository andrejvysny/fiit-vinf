"""
Utility functions for byte/size calculations
"""

import os
from pathlib import Path
from typing import Union


def dir_size_bytes(path: Union[str, Path]) -> int:
    """
    Calculate total size of directory in bytes.

    Args:
        path: Directory path

    Returns:
        Total size in bytes
    """
    path = Path(path)
    if not path.exists():
        return 0

    total = 0
    try:
        for entry in path.rglob('*'):
            if entry.is_file():
                total += entry.stat().st_size
    except Exception as e:
        print(f"WARNING: Error calculating directory size: {e}")

    return total


def dir_size_gb(path: Union[str, Path]) -> float:
    """
    Calculate total size of directory in GB.

    Args:
        path: Directory path

    Returns:
        Total size in GB
    """
    return dir_size_bytes(path) / (1024 ** 3)


def format_bytes(num_bytes: int) -> str:
    """
    Format bytes as human-readable string.

    Args:
        num_bytes: Number of bytes

    Returns:
        Formatted string (e.g., "1.5 GB", "256 KB")
    """
    for unit in ['B', 'KB', 'MB', 'GB', 'TB']:
        if abs(num_bytes) < 1024.0:
            return f"{num_bytes:.2f} {unit}"
        num_bytes /= 1024.0
    return f"{num_bytes:.2f} PB"