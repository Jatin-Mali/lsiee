"""Tests for configuration module."""

import os

import yaml

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


def test_config_file_is_created_with_restrictive_permissions(temp_dir, monkeypatch):
    """Saved config files should not be world-readable."""
    monkeypatch.setenv("LSIEE_DATA_DIR", str(temp_dir / "data"))
    monkeypatch.setenv("LSIEE_CONFIG_DIR", str(temp_dir / "config"))

    config = Config()
    mode = os.stat(config.config_file).st_mode & 0o777

    assert mode == 0o600


def test_invalid_config_values_are_coerced_to_safe_defaults(temp_dir, monkeypatch):
    """Malformed or unsafe config values should be clamped to secure defaults."""
    data_dir = temp_dir / "data"
    config_dir = temp_dir / "config"
    config_dir.mkdir(parents=True)
    config_file = config_dir / "config.yaml"
    config_file.write_text(
        yaml.safe_dump(
            {
                "logging": {"level": "verbose", "max_file_size_mb": -1},
                "monitoring": {
                    "interval_seconds": 0,
                    "store_cmdline": True,
                    "retention_days": -5,
                },
                "retention": {
                    "process_snapshots_days": -1,
                    "events_days": 999999,
                },
                "security": {
                    "max_query_length": 5,
                    "max_event_data_bytes": -100,
                },
            }
        ),
        encoding="utf-8",
    )

    monkeypatch.setenv("LSIEE_DATA_DIR", str(data_dir))
    monkeypatch.setenv("LSIEE_CONFIG_DIR", str(config_dir))

    config = Config()

    assert config.get("logging.level") == "WARNING"
    assert config.get("logging.max_file_size_mb") == 1
    assert config.get("monitoring.interval_seconds") == 0.01
    assert config.get("monitoring.store_cmdline") is True
    assert config.get("monitoring.retention_days") == 1
    assert config.get("retention.process_snapshots_days") == 1
    assert config.get("retention.events_days") == 3650
    assert config.get("security.max_query_length") == 10
    assert config.get("security.max_event_data_bytes") == 256
