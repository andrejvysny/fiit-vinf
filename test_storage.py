#!/usr/bin/env python3
"""
Test script for HtmlStore
"""

import tempfile
import hashlib
from pathlib import Path
from scraper.io.storage import HtmlStore


def test_html_store():
    """Test HTML storage with and without compression"""

    with tempfile.TemporaryDirectory() as tmpdir:
        print("TEST 1: Uncompressed storage")
        print("-" * 40)

        # Create store without compression
        store = HtmlStore(root=str(Path(tmpdir) / "html"), compress=False)

        # Test HTML content
        html1 = b"""<!DOCTYPE html>
<html><head><title>Test Page 1</title></head>
<body><h1>Hello World</h1></body></html>"""

        html2 = b"""<!DOCTYPE html>
<html><head><title>Test Page 2</title></head>
<body><h1>Different Content</h1></body></html>"""

        # Save first HTML
        result1 = store.save(html1)
        print(f"Saved HTML 1:")
        print(f"  SHA-256: {result1['sha256'][:16]}...")
        print(f"  Path: {result1['stored_path']}")
        print(f"  Size: {result1['bytes']} bytes")
        assert Path(result1['stored_path']).exists(), "File should exist"

        # Verify SHA-256
        expected_sha = hashlib.sha256(html1).hexdigest()
        assert result1['sha256'] == expected_sha, "SHA should match"
        print(f"  ✓ SHA verified")

        # Save same content again (deduplication test)
        result1_dup = store.save(html1)
        assert result1_dup.get('already_exists'), "Should detect duplicate"
        print(f"  ✓ Deduplication working")

        # Save different content
        result2 = store.save(html2)
        print(f"\nSaved HTML 2:")
        print(f"  SHA-256: {result2['sha256'][:16]}...")
        print(f"  Path: {result2['stored_path']}")

        # Read back content
        read1 = store.read(result1['sha256'])
        assert read1 == html1, "Content should match"
        print(f"\n✓ Read verification passed")

        # Check existence
        assert store.exists(result1['sha256']), "Should exist"
        assert not store.exists("nonexistent"), "Should not exist"
        print(f"✓ Existence check working")

        # Get stats
        stats = store.get_stats()
        print(f"\nUncompressed store stats:")
        print(f"  Files stored: {stats['files_stored']}")
        print(f"  Bytes stored: {stats['bytes_stored']}")
        print(f"  Disk files: {stats['disk_files']}")
        print(f"  Disk usage: {stats['disk_usage']} bytes")

        print()
        print("TEST 2: Compressed storage")
        print("-" * 40)

        # Create store with compression
        store_compressed = HtmlStore(
            root=str(Path(tmpdir) / "html_compressed"),
            compress=True
        )

        # Save large HTML for better compression
        large_html = b"<!DOCTYPE html>\n<html>\n" + (b"<p>This is repeated content. " * 100) + b"\n</html>"

        result3 = store_compressed.save(large_html)
        print(f"Saved compressed HTML:")
        print(f"  SHA-256: {result3['sha256'][:16]}...")
        print(f"  Original size: {result3['bytes']} bytes")
        print(f"  Compressed size: {result3.get('compressed_bytes', 'N/A')} bytes")
        print(f"  Compression ratio: {result3.get('compression_ratio', 'N/A')}")

        # Verify file has .zst extension
        assert result3['stored_path'].endswith('.html.zst'), "Should have .zst extension"
        print(f"  ✓ Correct extension")

        # Read back compressed content
        read3 = store_compressed.read(result3['sha256'])
        assert read3 == large_html, "Decompressed content should match"
        print(f"  ✓ Decompression working")

        # Get compressed stats
        stats2 = store_compressed.get_stats()
        print(f"\nCompressed store stats:")
        print(f"  Files stored: {stats2['files_stored']}")
        print(f"  Original bytes: {stats2['bytes_stored']}")
        print(f"  Compressed bytes: {stats2['bytes_on_disk']}")
        print(f"  Overall compression: {stats2.get('overall_compression', 'N/A')}")
        print(f"  Space saved: {stats2.get('space_saved', 0)} bytes")

        print()
        print("TEST 3: Path structure")
        print("-" * 40)

        # Verify nested directory structure
        sha = result1['sha256']
        expected_subdir = Path(store.root) / sha[:2] / sha[2:4]
        assert expected_subdir.exists(), "Nested directory should exist"
        print(f"✓ Directory structure: {sha[:2]}/{sha[2:4]}/{sha}.html")

        # Check permissions (Unix only)
        import os
        if os.name != 'nt':
            file_stat = Path(result1['stored_path']).stat()
            mode = oct(file_stat.st_mode)[-3:]
            assert mode == '644', f"Permissions should be 644, got {mode}"
            print(f"✓ File permissions: {mode}")

        print()
        print("✅ All storage tests passed!")


if __name__ == "__main__":
    test_html_store()