"""Integration tests for CLI."""

import pytest
from pathlib import Path
import tempfile
import shutil
from click.testing import CliRunner

from lsiee.cli import main
from lsiee.storage.schemas import initialize_database


@pytest.fixture
def temp_test_dir():
    """Create temporary test directory."""
    test_dir = Path(tempfile.mkdtemp())
    
    # Create test files
    (test_dir / 'file1.txt').write_text('Test 1')
    (test_dir / 'file2.txt').write_text('Test 2')
    (test_dir / 'file3.txt').write_text('Test 3')
    
    yield test_dir
    
    shutil.rmtree(test_dir)


@pytest.fixture
def temp_db(monkeypatch):
    """Create temporary database."""
    db_path = Path(tempfile.mktemp(suffix=".db"))
    initialize_database(db_path)
    
    # Monkeypatch the database path
    monkeypatch.setenv('LSIEE_DB_PATH', str(db_path))
    
    yield db_path
    
    if db_path.exists():
        db_path.unlink()


def test_index_command(temp_test_dir, temp_db):
    """Test index command."""
    runner = CliRunner()
    result = runner.invoke(main, ['index', str(temp_test_dir), '--no-progress'])
    
    assert result.exit_code == 0
    assert "Indexing complete" in result.output
    assert "3" in result.output  # 3 files


def test_status_command(temp_test_dir, temp_db):
    """Test status command."""
    runner = CliRunner()
    
    # Index first
    runner.invoke(main, ['index', str(temp_test_dir), '--no-progress'])
    
    # Check status
    result = runner.invoke(main, ['status'])
    
    assert result.exit_code == 0
    assert "Status" in result.output or "Statistics" in result.output


def test_help_command():
    """Test help command."""
    runner = CliRunner()
    result = runner.invoke(main, ['--help'])
    
    assert result.exit_code == 0
    assert "LSIEE" in result.output
    assert "index" in result.output


def test_search_placeholder():
    """Test search placeholder."""
    runner = CliRunner()
    result = runner.invoke(main, ['search', 'test query'])
    
    assert result.exit_code == 0
    assert "Week 2" in result.output


def test_monitor_placeholder():
    """Test monitor placeholder."""
    runner = CliRunner()
    result = runner.invoke(main, ['monitor'])
    
    assert result.exit_code == 0
    assert "Week 5" in result.output