"""Pytest configuration and fixtures."""

import pytest
from pathlib import Path
import tempfile
import shutil

@pytest.fixture
def temp_dir():
    """Create a temporary directory for tests."""
    temp_path = Path(tempfile.mkdtemp())
    yield temp_path
    shutil.rmtree(temp_path)

@pytest.fixture
def sample_files(temp_dir):
    """Create sample files for testing."""
    # Create some test files
    (temp_dir / "test.txt").write_text("This is a test file.")
    (temp_dir / "data.csv").write_text("name,value\ntest,123")
    return temp_dir
