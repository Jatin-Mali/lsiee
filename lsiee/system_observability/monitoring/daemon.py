"""Monitoring daemon for continuous collection."""

from __future__ import annotations

import argparse
import atexit
import logging
import os
import signal
import sqlite3
import subprocess
import sys
import threading
import time
from pathlib import Path
from typing import Iterable, Optional

from lsiee.config import config, get_db_path
from lsiee.storage.schemas import configure_connection, initialize_database
from lsiee.system_observability.monitoring.process_monitor import ProcessMonitor
from lsiee.temporal_intelligence.events import EventLogger

logger = logging.getLogger(__name__)


def get_monitor_pid_path(db_path: Optional[Path] = None) -> Path:
    """Return the PID file location for the monitoring daemon."""
    active_db_path = Path(db_path) if db_path else get_db_path()
    return active_db_path.parent / "monitor.pid"


def read_pid(pid_path: Optional[Path] = None) -> Optional[int]:
    """Read the daemon PID from disk if present."""
    path = Path(pid_path) if pid_path else get_monitor_pid_path()
    if not path.exists():
        return None

    try:
        return int(path.read_text(encoding="utf-8").strip())
    except (OSError, ValueError):
        return None


def is_pid_running(pid: Optional[int]) -> bool:
    """Check whether a PID is currently alive."""
    if pid is None or pid <= 0:
        return False

    try:
        os.kill(pid, 0)
    except OSError:
        return False
    return True


class MonitoringDaemon:
    """Background monitoring daemon."""

    def __init__(
        self,
        db_path: Optional[Path] = None,
        interval: Optional[float] = None,
        monitor: Optional[ProcessMonitor] = None,
    ):
        """Initialize daemon state."""
        self.db_path = Path(db_path) if db_path else get_db_path()
        self.monitor = monitor or ProcessMonitor()
        self.interval = float(
            interval if interval is not None else config.get("monitoring.interval_seconds", 5)
        )
        self.running = False
        self.thread: Optional[threading.Thread] = None
        self._stop_event = threading.Event()
        self.event_logger = EventLogger(self.db_path)
        self._ensure_database()

    def start(self, blocking: bool = False, iterations: Optional[int] = None):
        """Start the monitoring loop."""
        if self.running:
            logger.warning("Monitoring daemon already running")
            return

        self.running = True
        self._stop_event.clear()
        self.event_logger.log_event(
            event_type="monitoring_started",
            source="monitoring_daemon",
            data={"interval_seconds": self.interval, "blocking": blocking},
            tags=["system_observability", "monitoring"],
        )

        if blocking:
            self._monitoring_loop(iterations=iterations)
            return

        self.thread = threading.Thread(
            target=self._monitoring_loop,
            kwargs={"iterations": iterations},
            daemon=True,
        )
        self.thread.start()
        logger.info("Monitoring daemon started")

    def stop(self):
        """Stop the monitoring loop."""
        self.running = False
        self._stop_event.set()
        if self.thread and self.thread.is_alive():
            self.thread.join(timeout=max(5.0, self.interval * 2))
        logger.info("Monitoring daemon stopped")

    def _monitoring_loop(self, iterations: Optional[int] = None):
        """Run snapshot collection until stopped."""
        collected = 0
        try:
            while not self._stop_event.is_set():
                try:
                    snapshot = self.monitor.capture_snapshot()
                    self._store_snapshot(snapshot)
                    collected += 1
                except Exception as exc:  # pragma: no cover - defensive path
                    logger.exception("Monitoring error: %s", exc)

                if iterations is not None and collected >= iterations:
                    break

                self._stop_event.wait(self.interval)
        finally:
            self.running = False
            self.event_logger.log_event(
                event_type="monitoring_stopped",
                source="monitoring_daemon",
                data={"interval_seconds": self.interval, "iterations_collected": collected},
                tags=["system_observability", "monitoring"],
            )

    def _ensure_database(self):
        """Ensure the SQLite schema exists."""
        schema = initialize_database(self.db_path)
        schema.disconnect()

    def _store_snapshot(self, snapshot: Iterable[dict]):
        """Store snapshot rows in the process history table."""
        rows = [
            (
                proc["timestamp"],
                proc["pid"],
                proc["name"],
                proc.get("exe_path"),
                proc.get("cmdline"),
                proc["cpu_percent"],
                proc["memory_mb"],
                proc["memory_percent"],
                proc.get("io_read_bytes"),
                proc.get("io_write_bytes"),
                proc["status"],
                proc["num_threads"],
                proc["create_time"],
                proc.get("parent_pid"),
            )
            for proc in snapshot
        ]

        if not rows:
            return

        with sqlite3.connect(self.db_path) as conn:
            configure_connection(conn)
            conn.executemany(
                """
                INSERT INTO process_snapshots
                (timestamp, pid, name, exe_path, cmdline, cpu_percent, memory_mb,
                 memory_percent, io_read_bytes, io_write_bytes, status, num_threads,
                 create_time, parent_pid)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                rows,
            )
            conn.commit()

        logger.debug("Stored %s process snapshots", len(rows))


def spawn_background_daemon(
    db_path: Optional[Path] = None, interval: Optional[float] = None
) -> int:
    """Spawn the monitoring daemon as a detached subprocess."""
    active_db_path = Path(db_path) if db_path else get_db_path()
    pid_path = get_monitor_pid_path(active_db_path)
    existing_pid = read_pid(pid_path)

    if is_pid_running(existing_pid):
        return existing_pid  # pragma: no cover - idempotent external path

    active_db_path.parent.mkdir(parents=True, exist_ok=True)

    command = [
        sys.executable,
        "-m",
        "lsiee.system_observability.monitoring.daemon",
        "--run-foreground",
        "--db-path",
        str(active_db_path),
        "--pid-file",
        str(pid_path),
    ]

    if interval is not None:
        command.extend(["--interval", str(interval)])

    process = subprocess.Popen(
        command,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
        stdin=subprocess.DEVNULL,
        close_fds=True,
        start_new_session=True,
    )
    pid_path.write_text(str(process.pid), encoding="utf-8")
    return process.pid


def stop_background_daemon(db_path: Optional[Path] = None) -> bool:
    """Stop a detached monitoring daemon if one is running."""
    pid_path = get_monitor_pid_path(db_path)
    pid = read_pid(pid_path)

    if not is_pid_running(pid):
        _cleanup_pid_file(pid_path)
        return False

    os.kill(pid, signal.SIGTERM)

    for _ in range(20):
        if not is_pid_running(pid):
            break
        time.sleep(0.1)

    _cleanup_pid_file(pid_path)
    return True


def get_daemon_status(db_path: Optional[Path] = None) -> dict:
    """Return daemon status information."""
    pid_path = get_monitor_pid_path(db_path)
    pid = read_pid(pid_path)
    running = is_pid_running(pid)

    if not running:
        _cleanup_pid_file(pid_path)
        pid = None

    return {
        "running": running,
        "pid": pid,
        "pid_path": str(pid_path),
        "db_path": str(Path(db_path) if db_path else get_db_path()),
        "interval_seconds": config.get("monitoring.interval_seconds", 5),
    }


def run_foreground_daemon(
    db_path: Path,
    interval: Optional[float] = None,
    pid_file: Optional[Path] = None,
):
    """Run the monitoring daemon until a signal requests shutdown."""
    pid_path = Path(pid_file) if pid_file else get_monitor_pid_path(db_path)
    daemon = MonitoringDaemon(db_path=db_path, interval=interval)

    pid_path.parent.mkdir(parents=True, exist_ok=True)
    pid_path.write_text(str(os.getpid()), encoding="utf-8")

    def handle_stop(*_args):
        daemon.stop()

    atexit.register(_cleanup_pid_file, pid_path)
    signal.signal(signal.SIGTERM, handle_stop)
    signal.signal(signal.SIGINT, handle_stop)

    daemon.start(blocking=True)


def _cleanup_pid_file(pid_path: Path):
    """Remove the PID file if present."""
    try:
        pid_path.unlink(missing_ok=True)
    except OSError:  # pragma: no cover - best effort cleanup
        logger.debug("Could not remove PID file %s", pid_path)


def _parse_args(argv: Optional[list[str]] = None) -> argparse.Namespace:
    """Parse daemon runner arguments."""
    parser = argparse.ArgumentParser(description="LSIEE monitoring daemon")
    parser.add_argument("--run-foreground", action="store_true")
    parser.add_argument("--db-path", type=Path, required=True)
    parser.add_argument("--pid-file", type=Path, default=None)
    parser.add_argument("--interval", type=float, default=None)
    return parser.parse_args(argv)


def main(argv: Optional[list[str]] = None) -> int:
    """CLI entry point for the detached daemon process."""
    args = _parse_args(argv)
    if not args.run_foreground:
        return 1

    run_foreground_daemon(
        db_path=args.db_path,
        interval=args.interval,
        pid_file=args.pid_file,
    )
    return 0


if __name__ == "__main__":  # pragma: no cover - subprocess entry point
    raise SystemExit(main())
