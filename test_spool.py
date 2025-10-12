#!/usr/bin/env python3
"""
Test script for SpoolReader - simulates file rotation and resume
"""

import os
import json
import asyncio
import tempfile
import shutil
from pathlib import Path
from scraper.io.spool import SpoolReader


async def test_spool_reader():
    """Test spool reader with rotation and resume"""

    # Create temp directories
    with tempfile.TemporaryDirectory() as tmpdir:
        spool_dir = Path(tmpdir) / "spool"
        state_dir = Path(tmpdir) / "state"
        spool_dir.mkdir()
        state_dir.mkdir()

        bookmark_path = state_dir / "bookmark.json"

        print("TEST 1: Basic reading")
        print("-" * 40)

        # Create first discovery file
        disc1 = spool_dir / "discoveries-001.jsonl"
        with open(disc1, 'w') as f:
            f.write('{"timestamp": 1, "url": "https://github.com/python/cpython", "page_type": "repo_root", "depth": 0, "referrer": null}\n')
            f.write('{"timestamp": 2, "url": "https://github.com/python/cpython/blob/main/README.md", "page_type": "blob", "depth": 1, "referrer": "https://github.com/python/cpython"}\n')

        # Create reader
        reader = SpoolReader(str(spool_dir), str(bookmark_path))

        # Read first 2 items
        items = []
        async for item in reader.iter_items():
            items.append(item)
            print(f"Read: {item['url']}")
            if len(items) >= 2:
                break

        # Force save bookmark
        reader._save_bookmark(force=True)
        print(f"Stats: {reader.get_stats()}")
        print()

        print("TEST 2: Resume after restart")
        print("-" * 40)

        # Simulate restart - create new reader
        reader2 = SpoolReader(str(spool_dir), str(bookmark_path))

        # Add more items to same file
        with open(disc1, 'a') as f:
            f.write('{"timestamp": 3, "url": "https://github.com/torvalds/linux", "page_type": "repo_root", "depth": 0, "referrer": null}\n')

        # Read next item (should skip first 2)
        async for item in reader2.iter_items():
            print(f"Read after resume: {item['url']}")
            assert item['url'] == "https://github.com/torvalds/linux", "Should read only new item"
            break

        reader2._save_bookmark(force=True)
        print(f"Stats: {reader2.get_stats()}")
        print()

        print("TEST 3: File rotation")
        print("-" * 40)

        # Create second discovery file
        disc2 = spool_dir / "discoveries-002.jsonl"
        with open(disc2, 'w') as f:
            f.write('{"timestamp": 4, "url": "https://github.com/facebook/react", "page_type": "repo_root", "depth": 0, "referrer": null}\n')

        # Create new reader
        reader3 = SpoolReader(str(spool_dir), str(bookmark_path))

        # Read from rotated file
        async for item in reader3.iter_items():
            print(f"Read from rotated file: {item['url']}")
            assert item['url'] == "https://github.com/facebook/react", "Should read from new file"
            break

        reader3._save_bookmark(force=True)
        print(f"Final stats: {reader3.get_stats()}")
        print()

        print("TEST 4: Check final bookmark")
        print("-" * 40)

        # Load and print final bookmark
        with open(bookmark_path, 'r') as f:
            bookmark = json.load(f)
        print(f"Final bookmark: {bookmark}")

        assert bookmark['file'] == "discoveries-002.jsonl", "Should be on second file"
        print()

        print("✅ All tests passed!")


if __name__ == "__main__":
    asyncio.run(test_spool_reader())