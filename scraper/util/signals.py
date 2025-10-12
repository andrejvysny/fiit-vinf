"""
Signal handlers for graceful shutdown and status reporting
"""

import signal
import asyncio
import json
from typing import Optional, Callable, Any


class SignalHandler:
    """Manages UNIX signal handling for the scraper"""

    def __init__(self):
        self.shutdown_event = asyncio.Event()
        self.reload_callback: Optional[Callable] = None
        self.status_callback: Optional[Callable] = None
        self._original_handlers = {}

    def setup(self):
        """Register signal handlers"""
        # Store original handlers for cleanup
        self._original_handlers[signal.SIGINT] = signal.signal(signal.SIGINT, self._handle_shutdown)
        self._original_handlers[signal.SIGTERM] = signal.signal(signal.SIGTERM, self._handle_shutdown)
        self._original_handlers[signal.SIGHUP] = signal.signal(signal.SIGHUP, self._handle_reload)
        self._original_handlers[signal.SIGUSR1] = signal.signal(signal.SIGUSR1, self._handle_status)

    def cleanup(self):
        """Restore original signal handlers"""
        for sig, handler in self._original_handlers.items():
            signal.signal(sig, handler)

    def _handle_shutdown(self, signum, frame):
        """Handle SIGINT/SIGTERM for graceful shutdown"""
        sig_name = "SIGINT" if signum == signal.SIGINT else "SIGTERM"
        print(f"\n{sig_name} received - initiating graceful shutdown...")
        self.shutdown_event.set()

    def _handle_reload(self, signum, frame):
        """Handle SIGHUP for configuration reload"""
        print("\nSIGHUP received - reloading configuration...")
        if self.reload_callback:
            try:
                self.reload_callback()
                print("Configuration reloaded successfully")
            except Exception as e:
                print(f"Failed to reload configuration: {e}")

    def _handle_status(self, signum, frame):
        """Handle SIGUSR1 for status dump"""
        print("\n" + "=" * 60)
        print("SCRAPER STATUS DUMP (SIGUSR1)")
        print("=" * 60)
        if self.status_callback:
            try:
                status = self.status_callback()
                if isinstance(status, dict):
                    for key, value in status.items():
                        print(f"  {key}: {value}")
                else:
                    print(status)
            except Exception as e:
                print(f"Failed to get status: {e}")
        else:
            print("No status callback registered")
        print("=" * 60 + "\n")

    def on_reload(self, callback: Callable):
        """Register callback for configuration reload"""
        self.reload_callback = callback

    def on_status(self, callback: Callable):
        """Register callback for status reporting"""
        self.status_callback = callback

    async def wait_for_shutdown(self):
        """Wait for shutdown signal"""
        await self.shutdown_event.wait()