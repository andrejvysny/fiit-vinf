"""URL frontier (priority queue) using LMDB for persistence.

Maintains a priority queue of URLs to crawl, with scores determining
fetch order. Lower scores are fetched first (BFS-like if depth is used).
"""
"""LEGACY: frontier module removed.

The repo contains a new file-backed frontier implementation in
`crawler/frontier/frontier_file.py`. This legacy module has been removed to
avoid duplication. Importing it will raise ImportError.
"""

raise ImportError("Legacy frontier module removed. Use crawler/frontier/frontier_file.py")

