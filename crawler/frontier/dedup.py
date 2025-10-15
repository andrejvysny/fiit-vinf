"""LEGACY: dedup module removed.

The unified crawler uses `crawler/frontier/dedup_file.py` (FileDedupStore).
Importing this legacy dedup module will raise ImportError.
"""

raise ImportError("Legacy dedup module removed. Use crawler/frontier/dedup_file.py")
