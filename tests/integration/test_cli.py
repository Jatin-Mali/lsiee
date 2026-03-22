"""Integration tests for CLI."""

import sqlite3
import shutil
import tempfile
import time
from pathlib import Path

import pytest
from click.testing import CliRunner

from lsiee.cli import main
from lsiee.config import config
from lsiee.storage.schemas import initialize_database


@pytest.fixture
def temp_test_dir():
    """Create temporary test directory."""
    test_dir = Path(tempfile.mkdtemp())

    # Create test files
    (test_dir / "file1.txt").write_text("Test 1")
    (test_dir / "file2.txt").write_text("Test 2")
    (test_dir / "file3.txt").write_text("Test 3")

    yield test_dir

    shutil.rmtree(test_dir)


@pytest.fixture
def temp_environment(tmp_path, monkeypatch):
    """Create an isolated LSIEE environment."""
    db_path = tmp_path / "lsiee.db"
    vector_db_path = tmp_path / "vectors"
    config_dir = tmp_path / "config"

    initialize_database(db_path)

    monkeypatch.setenv("LSIEE_DB_PATH", str(db_path))
    monkeypatch.setenv("LSIEE_VECTOR_DB_PATH", str(vector_db_path))
    monkeypatch.setenv("LSIEE_CONFIG_DIR", str(config_dir))
    config._config = config._default_config()

    return {
        "db_path": db_path,
        "vector_db_path": vector_db_path,
    }


def test_index_command(temp_test_dir, temp_environment):
    """Test index command."""
    runner = CliRunner()
    result = runner.invoke(main, ["index", str(temp_test_dir), "--no-progress"])

    assert result.exit_code == 0
    assert "Indexing complete" in result.output
    assert "3" in result.output


def test_status_command(temp_test_dir, temp_environment):
    """Test status command."""
    runner = CliRunner()

    # Index first
    runner.invoke(main, ["index", str(temp_test_dir), "--no-progress"])

    # Check status
    result = runner.invoke(main, ["status"])

    assert result.exit_code == 0
    assert "Status" in result.output or "Statistics" in result.output


def test_help_command():
    """Test help command."""
    runner = CliRunner()
    result = runner.invoke(main, ["--help"])

    assert result.exit_code == 0
    assert "LSIEE" in result.output
    assert "index" in result.output


def test_search_command(temp_test_dir, temp_environment):
    """Test semantic search command."""
    runner = CliRunner()

    index_result = runner.invoke(main, ["index", str(temp_test_dir), "--no-progress"])
    assert index_result.exit_code == 0

    result = runner.invoke(main, ["search", "Test 1"])

    assert result.exit_code == 0
    assert "file1.txt" in result.output


def test_inspect_csv_command(tmp_path, temp_environment):
    """Test CSV inspection command."""
    filepath = tmp_path / "people.csv"
    filepath.write_text("name,age\nAlice,25\nBob,30\n", encoding="utf-8")

    runner = CliRunner()
    result = runner.invoke(main, ["inspect", str(filepath)])

    assert result.exit_code == 0
    assert "CSV File" in result.output
    assert "Schema" in result.output
    assert "name" in result.output


def test_inspect_json_command_with_path(tmp_path, temp_environment):
    """Test JSON inspection with JSON-path extraction."""
    filepath = tmp_path / "data.json"
    filepath.write_text('{"data": {"users": [{"name": "Alice"}]}}', encoding="utf-8")

    runner = CliRunner()
    result = runner.invoke(main, ["inspect", str(filepath), "--json-path", "data.users[0].name"])

    assert result.exit_code == 0
    assert "JSON Path" in result.output
    assert "Alice" in result.output


def test_query_command_returns_scalar_result(tmp_path, temp_environment):
    """Test query command for scalar aggregations."""
    filepath = tmp_path / "ages.csv"
    filepath.write_text("age\n20\n30\n40\n", encoding="utf-8")

    runner = CliRunner()
    result = runner.invoke(main, ["query", str(filepath), "average age"])

    assert result.exit_code == 0
    assert "Results" in result.output
    assert "30.0" in result.output


def test_query_command_exports_results(tmp_path, temp_environment):
    """Test query command exporting filtered rows."""
    filepath = tmp_path / "sales.csv"
    export_path = tmp_path / "results.csv"
    filepath.write_text("amount,region\n100,east\n250,west\n", encoding="utf-8")

    runner = CliRunner()
    result = runner.invoke(
        main,
        ["query", str(filepath), "filter amount > 150", "--export", str(export_path)],
    )

    assert result.exit_code == 0
    assert "Exported to" in result.output
    assert export_path.exists()
    assert "250" in export_path.read_text(encoding="utf-8")


def test_monitor_status_command(temp_environment):
    """Test monitor status output."""
    runner = CliRunner()
    result = runner.invoke(main, ["monitor", "--status"])

    assert result.exit_code == 0
    assert "Monitoring Status" in result.output
    assert "Stopped" in result.output


def test_monitor_top_cpu_command(temp_environment):
    """Test live top-CPU monitoring output."""
    runner = CliRunner()
    result = runner.invoke(main, ["monitor", "--top-cpu", "--limit", "5"])

    assert result.exit_code == 0
    assert "Top CPU Processes" in result.output


def test_monitor_start_and_history_commands(temp_environment):
    """Test bounded monitoring collection and history lookup."""
    runner = CliRunner()
    start_result = runner.invoke(
        main, ["monitor", "--start", "--iterations", "1", "--interval", "0.01"]
    )

    assert start_result.exit_code == 0
    assert "Collected 1 monitoring iteration" in start_result.output

    with sqlite3.connect(temp_environment["db_path"]) as conn:
        row = conn.execute(
            "SELECT pid FROM process_snapshots ORDER BY timestamp DESC LIMIT 1"
        ).fetchone()

    assert row is not None

    history_result = runner.invoke(main, ["monitor", "--history-pid", str(row[0]), "--hours", "1"])

    assert history_result.exit_code == 0
    assert "History for PID" in history_result.output


def test_monitor_detect_anomalies_and_show_alerts(temp_environment, monkeypatch):
    """The monitor CLI should detect anomalies and display stored alerts."""
    now = time.time()
    rows = [
        (
            now - (50 - index),
            2000 + index,
            "python",
            "/usr/bin/python",
            "python worker.py",
            10.0 + (index % 3),
            120.0 + index,
            5.0 + (index % 4) * 0.2,
            1000 + index * 10,
            900 + index * 8,
            "running",
            4,
            now - 1000,
            1,
        )
        for index in range(40)
    ]

    with sqlite3.connect(temp_environment["db_path"]) as conn:
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

    def fake_snapshot(self):
        return [
            {
                "timestamp": now,
                "pid": 9999,
                "name": "stress-ng",
                "exe_path": "/usr/bin/stress-ng",
                "cmdline": "stress-ng --cpu 8",
                "cpu_percent": 99.0,
                "memory_mb": 2048.0,
                "memory_percent": 92.0,
                "io_read_bytes": 7_000_000,
                "io_write_bytes": 9_000_000,
                "status": "running",
                "num_threads": 64,
                "create_time": now - 30,
                "parent_pid": 1,
            }
        ]

    monkeypatch.setattr(
        "lsiee.system_observability.monitoring.process_monitor.ProcessMonitor.capture_snapshot",
        fake_snapshot,
    )

    runner = CliRunner()
    detect_result = runner.invoke(
        main, ["monitor", "--detect-anomalies", "--hours", "1", "--limit", "5"]
    )

    assert detect_result.exit_code == 0
    assert "Detected Anomalies" in detect_result.output
    assert "stress-ng" in detect_result.output

    alert_result = runner.invoke(
        main, ["monitor", "--alert-history", "--hours", "1", "--limit", "5"]
    )

    assert alert_result.exit_code == 0
    assert "Anomaly Alerts" in alert_result.output
    assert "anomaly_detected" in alert_result.output


def test_explain_command_reports_root_causes(temp_environment):
    """The explain CLI should render root causes and recommendations."""
    incident_time = 1_700_000_000.0

    with sqlite3.connect(temp_environment["db_path"]) as conn:
        conn.executemany(
            """
            INSERT INTO process_snapshots
            (timestamp, pid, name, exe_path, cmdline, cpu_percent, memory_mb,
             memory_percent, io_read_bytes, io_write_bytes, status, num_threads,
             create_time, parent_pid)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            [
                (
                    incident_time - 30,
                    4242,
                    "backup.exe",
                    "/usr/bin/backup",
                    "backup --run",
                    96.0,
                    2048.0,
                    82.0,
                    10_000,
                    15_000,
                    "running",
                    24,
                    incident_time - 600,
                    1,
                ),
                (
                    incident_time - 3600,
                    4242,
                    "backup.exe",
                    "/usr/bin/backup",
                    "backup --run",
                    88.0,
                    1500.0,
                    70.0,
                    8_000,
                    9_000,
                    "running",
                    20,
                    incident_time - 4200,
                    1,
                ),
            ],
        )
        conn.commit()

    runner = CliRunner()
    result = runner.invoke(
        main,
        ["explain", "system slowdown", "--time", str(incident_time)],
    )

    assert result.exit_code == 0
    assert "Root Causes" in result.output
    assert "backup.exe" in result.output
    assert "Recommendations" in result.output
