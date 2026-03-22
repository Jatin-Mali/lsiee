"""Tests for configuration module."""

from lsiee.config import Config


def test_config_creation(temp_dir, monkeypatch):
    """Test configuration creation."""
    monkeypatch.setenv("HOME", str(temp_dir))
    config = Config()
    assert config.get("search.default_result_limit") == 10


def test_config_set_get(temp_dir, monkeypatch):
    """Test setting and getting configuration values."""
    monkeypatch.setenv("HOME", str(temp_dir))
    config = Config()
    config.set("test.value", 42)
    assert config.get("test.value") == 42
