"""
Manager control flags - check for pause/stop flags dropped by Manager
"""

import os
import asyncio
from pathlib import Path
from typing import Optional


class ControlFlags:
    """Check for Manager control flags in workspace"""

    def __init__(self, workspace: str):
        self.workspace = Path(workspace)
        self.pause_file = self.workspace / "pause.scraper"
        self.stop_file = self.workspace / "stop.scraper"
        self.check_interval = 1.0  # Check every 1 second

    def is_paused(self) -> bool:
        """Check if pause flag exists"""
        return self.pause_file.exists()

    def should_stop(self) -> bool:
        """Check if stop flag exists"""
        return self.stop_file.exists()

    async def wait_while_paused(self):
        """Wait while pause flag is present"""
        was_paused = False
        while self.is_paused():
            if not was_paused:
                print("PAUSE: Manager pause flag detected - pausing consumption...")
                was_paused = True
            await asyncio.sleep(self.check_interval)

        if was_paused:
            print("RESUME: Pause flag removed - resuming operation")

    async def check_stop(self) -> bool:
        """Check stop flag and log if found"""
        if self.should_stop():
            print("STOP: Manager stop flag detected - initiating graceful shutdown...")
            return True
        return False

    def clear_flags(self):
        """Clear control flags (usually done by Manager, not Scraper)"""
        if self.pause_file.exists():
            self.pause_file.unlink()
        if self.stop_file.exists():
            self.stop_file.unlink()