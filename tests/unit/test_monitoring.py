"""Tests for process monitoring and system metrics."""

from __future__ import annotations

import sqlite3
import time

from lsiee.storage.schemas import initialize_database
from lsiee.system_observability.monitoring.daemon import MonitoringDaemon, get_daemon_status
from lsiee.system_observability.monitoring.history import ProcessHistory
from lsiee.system_observability.monitoring.process_monitor import ProcessMonitor
from lsiee.system_observability.monitoring.system_metrics import SystemMetrics


def test_process_snapshot():
    """Process snapshots should include core fields."""
    snapshot = ProcessMonitor().capture_snapshot()

    assert snapshot
    assert "pid" in snapshot[0]
    assert "cpu_percent" in snapshot[0]
    assert "memory_mb" in snapshot[0]


def test_system_metrics():
    """System metrics should include CPU and memory data."""
    metrics = SystemMetrics()
    cpu = metrics.get_cpu_usage()
    memory = metrics.get_memory_usage()

    assert "percent" in cpu
    assert 0 <= cpu["percent"] <= 100
    assert "total_gb" in memory
    assert memory["total_gb"] >= 0


def test_process_history_queries(tmp_path):
    """Process history should return rows for both history and timeline queries."""
    db_path = tmp_path / "lsiee.db"
    schema = initialize_database(db_path)
    schema.disconnect()

    now = time.time()
    rows = [
        (
            now - 10,
            1234,
            "python",
            "/usr/bin/python",
            "python app.py",
            10.0,
            100.0,
            1.2,
            10,
            20,
            "running",
            4,
            now - 100,
            1,
        ),
        (
            now - 5,
            1234,
            "python",
            "/usr/bin/python",
            "python app.py",
            15.0,
            110.0,
            1.3,
            15,
            25,
            "sleeping",
            4,
            now - 100,
            1,
        ),
    ]

    with sqlite3.connect(db_path) as conn:
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

    history = ProcessHistory(db_path)
    process_rows = history.get_process_history(1234, now - 60, now)
    timeline = history.get_cpu_timeline("python", hours=1)

    assert len(process_rows) == 2
    assert len(timeline) == 2
    assert timeline[-1][1] == 15.0


def test_monitoring_daemon_stores_snapshots(tmp_path):
    """The daemon should write captured snapshots into SQLite."""
    db_path = tmp_path / "lsiee.db"
    now = time.time()

    class StubMonitor:
        def capture_snapshot(self):
            return [
                {
                    "timestamp": now,
                    "pid": 999,
                    "name": "stub",
                    "exe_path": None,
                    "cmdline": "stub --run",
                    "cpu_percent": 1.5,
                    "memory_mb": 20.0,
                    "memory_percent": 0.5,
                    "io_read_bytes": 10,
                    "io_write_bytes": 20,
                    "status": "running",
                    "num_threads": 2,
                    "create_time": now - 5,
                    "parent_pid": 1,
                }
            ]

    daemon = MonitoringDaemon(db_path=db_path, interval=0.01, monitor=StubMonitor())
    daemon.start(blocking=True, iterations=1)

    with sqlite3.connect(db_path) as conn:
        count = conn.execute("SELECT COUNT(*) FROM process_snapshots").fetchone()[0]
        pid = conn.execute("SELECT pid FROM process_snapshots LIMIT 1").fetchone()[0]

    assert count == 1
    assert pid == 999


def test_get_daemon_status_cleans_stale_pid_file(tmp_path, monkeypatch):
    """A stale PID file should be removed when the daemon is not running."""
    db_path = tmp_path / "lsiee.db"
    pid_path = tmp_path / "monitor.pid"
    pid_path.write_text("999999", encoding="utf-8")

    monkeypatch.setenv("LSIEE_DB_PATH", str(db_path))

    status = get_daemon_status(db_path=db_path)

    assert status["running"] is False
    assert status["pid"] is None
    assert not pid_path.exists()
