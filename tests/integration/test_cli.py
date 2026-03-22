"""Integration tests for CLI."""

import shutil
import tempfile
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


def test_monitor_placeholder():
    """Test monitor placeholder."""
    runner = CliRunner()
    result = runner.invoke(main, ["monitor"])

    assert result.exit_code == 0
    assert "Week 5" in result.output
