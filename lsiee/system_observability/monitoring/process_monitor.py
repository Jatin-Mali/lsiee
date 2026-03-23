"""Process monitoring and snapshot capture."""

from __future__ import annotations

import getpass
import logging
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List

import psutil

from lsiee.config import config

logger = logging.getLogger(__name__)


class ProcessMonitor:
    """Monitor system processes."""

    def capture_snapshot(self, cpu_interval: float = 0.0) -> List[Dict[str, Any]]:
        """Capture a snapshot of running processes."""
        snapshot: List[Dict[str, Any]] = []
        timestamp = datetime.now().timestamp()
        current_username = self._current_username()

        for proc in psutil.process_iter(["pid", "name", "username"]):
            try:
                info = proc.info
                username = info.get("username")
                if not self._should_monitor_username(username, current_username):
                    continue
                process_name = info.get("name") or "<unknown>"
                if self._is_excluded_process(process_name):
                    continue

                memory_info = proc.memory_info()
                record: Dict[str, Any] = {
                    "timestamp": timestamp,
                    "pid": info["pid"],
                    "name": self._sanitize_process_name(process_name),
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
        if not config.get("monitoring.store_cmdline", False):
            return None
        try:
            cmdline = proc.cmdline()
        except (
            psutil.NoSuchProcess,
            psutil.AccessDenied,
            psutil.ZombieProcess,
            ProcessLookupError,
        ):
            return None
        if not cmdline:
            return None

        sanitized = []
        for argument in cmdline[:8]:
            lowered = argument.lower()
            if any(token in lowered for token in ("token", "secret", "password", "key=")):
                sanitized.append("[REDACTED]")
            else:
                sanitized.append(argument[:128])
        return " ".join(sanitized)[:512]

    @staticmethod
    def _safe_exe(proc: psutil.Process) -> str | None:
        """Return the executable path if accessible."""
        if not config.get("monitoring.store_exe_path", False):
            return None
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
        if not exe_path:
            return None
        return Path(exe_path).name

    @staticmethod
    def _current_username() -> str | None:
        """Return the username of the current process if available."""
        try:
            return psutil.Process().username()
        except Exception:
            try:
                return getpass.getuser()
            except Exception:
                return None

    @staticmethod
    def _normalize_username(username: str | None) -> str | None:
        if not username:
            return None
        normalized = username.strip().lower()
        if "\\" in normalized:
            normalized = normalized.split("\\")[-1]
        if "/" in normalized:
            normalized = normalized.split("/")[-1]
        return normalized

    def _should_monitor_username(self, username: str | None, current_username: str | None) -> bool:
        """Restrict collection to the current user unless explicitly disabled."""
        if not config.get("monitoring.current_user_only", True):
            return True
        if username is None or current_username is None:
            return True
        return self._normalize_username(username) == self._normalize_username(current_username)

    @staticmethod
    def _is_excluded_process(process_name: str) -> bool:
        excluded = {
            name.lower()
            for name in config.get("monitoring.exclude_processes", [])
            if str(name).strip()
        }
        return process_name.lower() in excluded

    @staticmethod
    def _sanitize_process_name(process_name: str) -> str:
        """Optionally anonymize process names before persistence."""
        normalized = process_name[:128]
        if config.get("monitoring.anonymize_process_names", False):
            return f"process-{abs(hash(normalized)) % 100000}"
        return normalized
