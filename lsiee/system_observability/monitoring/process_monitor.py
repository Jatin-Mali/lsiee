"""Process monitoring and snapshot capture."""

from __future__ import annotations

import logging
from datetime import datetime
from typing import Any, Dict, List

import psutil

logger = logging.getLogger(__name__)


class ProcessMonitor:
    """Monitor system processes."""

    def capture_snapshot(self, cpu_interval: float = 0.0) -> List[Dict[str, Any]]:
        """Capture a snapshot of running processes."""
        snapshot: List[Dict[str, Any]] = []
        timestamp = datetime.now().timestamp()

        for proc in psutil.process_iter(["pid", "name", "username"]):
            try:
                info = proc.info
                memory_info = proc.memory_info()
                record: Dict[str, Any] = {
                    "timestamp": timestamp,
                    "pid": info["pid"],
                    "name": info.get("name") or "<unknown>",
                    "exe_path": self._safe_exe(proc),
                    "cmdline": self._safe_cmdline(proc),
                    "cpu_percent": proc.cpu_percent(interval=cpu_interval),
                    "memory_mb": memory_info.rss / (1024 * 1024),
                    "memory_percent": proc.memory_percent(),
                    "status": proc.status(),
                    "num_threads": proc.num_threads(),
                    "create_time": proc.create_time(),
                    "parent_pid": proc.ppid(),
                }

                try:
                    io = proc.io_counters()
                    record["io_read_bytes"] = io.read_bytes
                    record["io_write_bytes"] = io.write_bytes
                except (psutil.AccessDenied, AttributeError, NotImplementedError):
                    record["io_read_bytes"] = None
                    record["io_write_bytes"] = None

                snapshot.append(record)
            except (
                psutil.NoSuchProcess,
                psutil.AccessDenied,
                psutil.ZombieProcess,
                ProcessLookupError,
            ) as exc:
                logger.debug("Skipping inaccessible process %s: %s", proc, exc)

        logger.info("Captured snapshot of %s processes", len(snapshot))
        return snapshot

    def get_process_by_name(self, name: str) -> List[Dict[str, Any]]:
        """Return processes whose name contains the provided text."""
        name_lower = name.lower()
        return [proc for proc in self.capture_snapshot() if name_lower in proc["name"].lower()]

    def get_top_cpu(self, n: int = 10) -> List[Dict[str, Any]]:
        """Return the top N processes by CPU usage."""
        snapshot = self.capture_snapshot()
        snapshot.sort(key=lambda item: item["cpu_percent"], reverse=True)
        return snapshot[:n]

    def get_top_memory(self, n: int = 10) -> List[Dict[str, Any]]:
        """Return the top N processes by memory usage."""
        snapshot = self.capture_snapshot()
        snapshot.sort(key=lambda item: item["memory_mb"], reverse=True)
        return snapshot[:n]

    @staticmethod
    def _safe_cmdline(proc: psutil.Process) -> str | None:
        """Return the process command line if accessible."""
        try:
            cmdline = proc.cmdline()
        except (
            psutil.NoSuchProcess,
            psutil.AccessDenied,
            psutil.ZombieProcess,
            ProcessLookupError,
        ):
            return None
        return " ".join(cmdline) if cmdline else None

    @staticmethod
    def _safe_exe(proc: psutil.Process) -> str | None:
        """Return the executable path if accessible."""
        try:
            exe_path = proc.exe()
        except (
            psutil.NoSuchProcess,
            psutil.AccessDenied,
            psutil.ZombieProcess,
            ProcessLookupError,
            NotImplementedError,
        ):
            return None
        return exe_path or None
