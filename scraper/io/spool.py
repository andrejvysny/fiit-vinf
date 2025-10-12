"""
Spool reader - tails discovery JSONL files with bookmark support
"""

import os
import json
import asyncio
from pathlib import Path
from typing import AsyncIterator, Dict, Any, Optional


class SpoolReader:
    """Read discoveries from rotating JSONL files with resume support"""

    def __init__(self, dir_path: str, bookmark_path: str):
        self.dir = Path(dir_path)
        self.bookmark_path = Path(bookmark_path)
        self.state = {"file": None, "offset": 0}
        self.poll_interval = 0.5  # Check for new data every 500ms
        self.save_interval = 100  # Save bookmark every N items

        # Statistics
        self.items_read = 0
        self.files_processed = 0
        self.last_item = None

    def _load_bookmark(self):
        """Load bookmark from disk if exists"""
        if self.bookmark_path.exists():
            try:
                with open(self.bookmark_path, 'r') as f:
                    self.state = json.load(f)
                print(f"Loaded bookmark: file={self.state.get('file')}, offset={self.state.get('offset')}")
            except Exception as e:
                print(f"WARNING: Failed to load bookmark: {e}")
                self.state = {"file": None, "offset": 0}
        else:
            print("No bookmark found - starting from beginning")

    def _save_bookmark(self, force: bool = False):
        """Save bookmark to disk (atomic write)"""
        if not force and self.items_read % self.save_interval != 0:
            return

        try:
            # Ensure directory exists
            self.bookmark_path.parent.mkdir(parents=True, exist_ok=True)

            # Atomic write via temp file
            tmp_path = str(self.bookmark_path) + ".tmp"
            with open(tmp_path, 'w') as f:
                json.dump(self.state, f)
            os.replace(tmp_path, self.bookmark_path)

            if force:
                print(f"Bookmark saved: file={self.state.get('file')}, offset={self.state.get('offset')}")
        except Exception as e:
            print(f"WARNING: Failed to save bookmark: {e}")

    def _list_segment_files(self):
        """List discovery files in order"""
        if not self.dir.exists():
            return []

        files = []
        for f in self.dir.glob("discoveries-*.jsonl"):
            files.append(f)

        # Sort by name (assumes timestamp-based naming)
        return sorted(files)

    def _find_start_position(self, files: list) -> tuple[int, int]:
        """Find starting file index and offset based on bookmark"""
        if not self.state.get("file"):
            return 0, 0

        # Find bookmark file in list
        for idx, f in enumerate(files):
            if f.name == self.state["file"]:
                return idx, self.state.get("offset", 0)

        # Bookmark file not found - might have been rotated/deleted
        print(f"WARNING: Bookmark file {self.state['file']} not found - starting from first file")
        return 0, 0

    async def iter_items(self) -> AsyncIterator[Dict[str, Any]]:
        """
        Iterate over discovery items from JSONL files.
        Yields parsed JSON objects, maintains bookmark.
        """
        self._load_bookmark()

        # Track current file handle
        current_file = None
        current_fh = None

        try:
            while True:
                files = self._list_segment_files()

                if not files:
                    # No files yet - wait
                    await asyncio.sleep(self.poll_interval)
                    continue

                # Find starting position
                file_idx, offset = self._find_start_position(files)

                # Process files from starting position
                while file_idx < len(files):
                    file_path = files[file_idx]

                    # Open file if not already open
                    if current_file != file_path:
                        if current_fh:
                            current_fh.close()
                        current_file = file_path
                        current_fh = open(file_path, 'r', encoding='utf-8')
                        current_fh.seek(offset)
                        self.files_processed += 1
                        print(f"Reading file: {file_path.name} (offset: {offset})")
                        offset = 0  # Reset offset for next file

                    # Read lines from current position
                    lines_read = 0
                    while True:
                        pos = current_fh.tell()
                        line = current_fh.readline()

                        if not line:
                            # End of file - check if this is the last file
                            if file_idx == len(files) - 1:
                                # This is the currently active file - wait for more data
                                await asyncio.sleep(self.poll_interval)

                                # Check if new files appeared
                                new_files = self._list_segment_files()
                                if len(new_files) > len(files):
                                    # New file created - move to next
                                    break
                            else:
                                # Not the last file - move to next
                                break

                        # Parse line
                        line = line.strip()
                        if not line:
                            continue

                        try:
                            item = json.loads(line)
                            self.items_read += 1
                            lines_read += 1
                            self.last_item = item

                            # Update bookmark
                            self.state["file"] = file_path.name
                            self.state["offset"] = current_fh.tell()
                            self._save_bookmark()

                            yield item

                        except json.JSONDecodeError as e:
                            print(f"WARNING: Skipping malformed line at {file_path.name}:{pos} - {e}")
                            continue

                    # Move to next file if we finished current one
                    if lines_read == 0 and file_idx < len(files) - 1:
                        file_idx += 1
                        print(f"Moving to next file (index {file_idx})")
                    elif file_idx == len(files) - 1:
                        # Stay on last file, waiting for more data
                        await asyncio.sleep(self.poll_interval)
                        # Refresh file list
                        files = self._list_segment_files()

        finally:
            # Clean up on exit
            if current_fh:
                current_fh.close()
            self._save_bookmark(force=True)
            print(f"Spool reader stopped - processed {self.items_read} items from {self.files_processed} files")

    def get_stats(self) -> Dict[str, Any]:
        """Get reader statistics"""
        return {
            "items_read": self.items_read,
            "files_processed": self.files_processed,
            "current_file": self.state.get("file"),
            "current_offset": self.state.get("offset"),
            "last_item_url": self.last_item.get("url") if self.last_item else None
        }