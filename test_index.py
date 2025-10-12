#!/usr/bin/env python3
"""
Test script for PageIndex LMDB
"""

import tempfile
import time
from pathlib import Path
from scraper.index.page_index import PageIndex


def test_page_index():
    """Test LMDB page index operations"""

    with tempfile.TemporaryDirectory() as tmpdir:
        db_path = Path(tmpdir) / "page_index.lmdb"

        print("TEST 1: Basic operations")
        print("-" * 40)

        # Create index
        index = PageIndex(str(db_path))

        # Test put/get
        record1 = {
            "url": "https://github.com/python/cpython",
            "sha256": "abc123def456",
            "stored_path": "store/ab/c1/abc123def456.html",
            "etag": '"etag-123"',
            "last_modified": "Wed, 21 Oct 2015 07:28:00 GMT",
            "timestamp": int(time.time())
        }

        # Put record
        assert index.put(record1), "Put should succeed"
        print(f"✓ Put record: {record1['url']}")

        # Get record
        retrieved = index.get("https://github.com/python/cpython")
        assert retrieved is not None, "Should retrieve record"
        assert retrieved['sha256'] == "abc123def456", "SHA should match"
        print(f"✓ Retrieved record: {retrieved['url']}")

        # Check existence
        assert index.exists("https://github.com/python/cpython"), "Should exist"
        assert not index.exists("https://example.com"), "Should not exist"
        print("✓ Exists check working")

        print()
        print("TEST 2: Update existing record")
        print("-" * 40)

        # Update with new etag
        record1['etag'] = '"etag-456"'
        record1['timestamp'] = int(time.time())
        assert index.put(record1), "Update should succeed"

        # Verify update
        retrieved = index.get("https://github.com/python/cpython")
        assert retrieved['etag'] == '"etag-456"', "ETag should be updated"
        print(f"✓ Updated ETag: {retrieved['etag']}")

        print()
        print("TEST 3: Multiple records")
        print("-" * 40)

        # Add more records
        urls = [
            "https://github.com/torvalds/linux",
            "https://github.com/facebook/react",
            "https://github.com/microsoft/vscode"
        ]

        for i, url in enumerate(urls):
            record = {
                "url": url,
                "sha256": f"sha256_{i}",
                "stored_path": f"store/path_{i}.html",
                "timestamp": int(time.time())
            }
            index.put(record)
            print(f"✓ Added: {url}")

        # Check size
        assert index.size() == 4, f"Should have 4 records, got {index.size()}"
        print(f"✓ Index size: {index.size()}")

        print()
        print("TEST 4: Iteration with prefix")
        print("-" * 40)

        # Iterate all
        all_urls = []
        for url, record in index.iterate():
            all_urls.append(url)
        assert len(all_urls) == 4, "Should iterate 4 records"
        print(f"✓ Iterated all: {len(all_urls)} records")

        # Iterate with prefix
        github_urls = []
        for url, record in index.iterate(prefix="https://github.com/"):
            github_urls.append(url)
        assert len(github_urls) == 4, "All should match github prefix"
        print(f"✓ Prefix iteration: {len(github_urls)} github URLs")

        print()
        print("TEST 5: Statistics")
        print("-" * 40)

        stats = index.get_stats()
        print(f"Entries: {stats['entries']}")
        print(f"Gets: {stats['gets']}")
        print(f"Puts: {stats['puts']}")
        print(f"Hits: {stats['hits']}")
        print(f"Hit rate: {stats['hit_rate']}")
        print(f"DB size: {stats['db_size']} bytes")

        print()
        print("TEST 6: Persistence")
        print("-" * 40)

        # Close index
        index.close()
        print("✓ Index closed")

        # Re-open and verify data persisted
        index2 = PageIndex(str(db_path))
        assert index2.size() == 4, "Should have 4 records after reopen"
        retrieved = index2.get("https://github.com/python/cpython")
        assert retrieved is not None, "Should retrieve after reopen"
        assert retrieved['etag'] == '"etag-456"', "Data should persist"
        print(f"✓ Reopened index, size: {index2.size()}")
        print(f"✓ Retrieved persisted record: {retrieved['url']}")

        index2.close()

        print()
        print("✅ All tests passed!")


if __name__ == "__main__":
    test_page_index()