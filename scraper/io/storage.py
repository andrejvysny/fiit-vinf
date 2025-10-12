"""
Content-addressed HTML storage with optional compression
"""

import os
import hashlib
from pathlib import Path
from typing import Dict, Any

try:
    import zstandard as zstd
    ZSTD_AVAILABLE = True
except ImportError:
    ZSTD_AVAILABLE = False


class HtmlStore:
    """
    Store HTML content using SHA-256 content addressing.
    Supports optional zstandard compression.
    """

    def __init__(self, root: str, compress: bool = False, permissions: int = 0o644):
        """
        Initialize HTML store.

        Args:
            root: Root directory for storage
            compress: Enable zstandard compression
            permissions: File permissions for stored files
        """
        self.root = Path(root)
        self.compress = compress
        self.permissions = permissions

        if compress and not ZSTD_AVAILABLE:
            raise ImportError("zstandard package required for compression. Install with: pip install zstandard")

        # Create root directory
        self.root.mkdir(parents=True, exist_ok=True)

        # Statistics
        self.files_stored = 0
        self.bytes_stored = 0
        self.bytes_compressed = 0

    def save(self, html_bytes: bytes) -> Dict[str, Any]:
        """
        Save HTML content to disk.

        Args:
            html_bytes: Raw HTML bytes to store

        Returns:
            Dict with:
            - sha256: Content hash
            - stored_path: Path where file was stored
            - bytes: Original size in bytes
            - compressed_bytes: Compressed size (if compression enabled)
        """
        # Calculate SHA-256 hash
        sha256 = hashlib.sha256(html_bytes).hexdigest()

        # Create nested directory structure (aa/bb/sha256.html[.zst])
        subdir = self.root / sha256[:2] / sha256[2:4]
        subdir.mkdir(parents=True, exist_ok=True)

        # Determine file path
        if self.compress:
            file_path = subdir / f"{sha256}.html.zst"
        else:
            file_path = subdir / f"{sha256}.html"

        # Check if already exists (deduplication)
        if file_path.exists():
            return {
                "sha256": sha256,
                "stored_path": str(file_path),
                "bytes": len(html_bytes),
                "already_exists": True
            }

        # Prepare data for writing
        if self.compress:
            # Compress with zstandard (level 10 for good compression)
            compressor = zstd.ZstdCompressor(level=10, threads=-1)
            data_to_write = compressor.compress(html_bytes)
            compressed_size = len(data_to_write)
        else:
            data_to_write = html_bytes
            compressed_size = len(html_bytes)

        # Write atomically (via temp file)
        tmp_path = file_path.with_suffix(file_path.suffix + '.tmp')

        try:
            # Write to temp file
            with open(tmp_path, 'wb') as f:
                f.write(data_to_write)

            # Set permissions
            os.chmod(tmp_path, self.permissions)

            # Atomic rename
            os.replace(tmp_path, file_path)

            # Update statistics
            self.files_stored += 1
            self.bytes_stored += len(html_bytes)
            self.bytes_compressed += compressed_size

            result = {
                "sha256": sha256,
                "stored_path": str(file_path),
                "bytes": len(html_bytes)
            }

            if self.compress:
                result["compressed_bytes"] = compressed_size
                result["compression_ratio"] = f"{(1 - compressed_size/len(html_bytes)) * 100:.1f}%"

            return result

        except Exception as e:
            # Clean up temp file on error
            if tmp_path.exists():
                tmp_path.unlink()
            raise Exception(f"Failed to store HTML: {e}")

    def read(self, sha256: str) -> bytes:
        """
        Read HTML content by SHA-256 hash.

        Args:
            sha256: Content hash

        Returns:
            Original HTML bytes (decompressed if needed)
        """
        # Build path
        subdir = self.root / sha256[:2] / sha256[2:4]

        # Try compressed first if compression is enabled
        if self.compress:
            file_path = subdir / f"{sha256}.html.zst"
            if file_path.exists():
                with open(file_path, 'rb') as f:
                    compressed_data = f.read()
                decompressor = zstd.ZstdDecompressor()
                return decompressor.decompress(compressed_data)

        # Try uncompressed
        file_path = subdir / f"{sha256}.html"
        if file_path.exists():
            with open(file_path, 'rb') as f:
                return f.read()

        raise FileNotFoundError(f"Content not found: {sha256}")

    def exists(self, sha256: str) -> bool:
        """Check if content exists by hash"""
        subdir = self.root / sha256[:2] / sha256[2:4]

        if self.compress:
            if (subdir / f"{sha256}.html.zst").exists():
                return True

        return (subdir / f"{sha256}.html").exists()

    def get_path(self, sha256: str) -> str:
        """Get storage path for given hash"""
        subdir = self.root / sha256[:2] / sha256[2:4]

        if self.compress:
            path = subdir / f"{sha256}.html.zst"
            if path.exists():
                return str(path)

        path = subdir / f"{sha256}.html"
        if path.exists():
            return str(path)

        return None

    def get_stats(self) -> Dict[str, Any]:
        """Get storage statistics"""
        stats = {
            "files_stored": self.files_stored,
            "bytes_stored": self.bytes_stored,
            "bytes_on_disk": self.bytes_compressed if self.compress else self.bytes_stored,
            "compression_enabled": self.compress
        }

        if self.compress and self.bytes_stored > 0:
            stats["overall_compression"] = f"{(1 - self.bytes_compressed/self.bytes_stored) * 100:.1f}%"
            stats["space_saved"] = self.bytes_stored - self.bytes_compressed

        # Count actual files on disk
        total_files = 0
        total_size = 0
        for path in self.root.rglob("*.html*"):
            if not path.name.endswith('.tmp'):
                total_files += 1
                total_size += path.stat().st_size

        stats["disk_files"] = total_files
        stats["disk_usage"] = total_size

        return stats