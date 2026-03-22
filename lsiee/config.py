"""Configuration management for LSIEE."""

import os
from pathlib import Path
from typing import Dict, Any

import yaml


def get_data_dir() -> Path:
    """Return the LSIEE data directory."""
    return Path(os.environ.get("LSIEE_DATA_DIR", Path.home() / ".lsiee"))


def get_db_path() -> Path:
    """Return the metadata database path."""
    default_path = get_data_dir() / "lsiee.db"
    return Path(os.environ.get("LSIEE_DB_PATH", default_path))


def get_vector_db_path() -> Path:
    """Return the semantic search storage path."""
    default_path = get_data_dir() / "vectors"
    return Path(os.environ.get("LSIEE_VECTOR_DB_PATH", default_path))


class Config:
    """LSIEE configuration manager."""

    def __init__(self):
        self._config: Dict[str, Any] = {}
        self._load_config()

    @property
    def config_dir(self) -> Path:
        """Return the configuration directory."""
        return Path(os.environ.get("LSIEE_CONFIG_DIR", str(get_data_dir())))

    @property
    def config_file(self) -> Path:
        """Return the configuration file path."""
        return self.config_dir / "config.yaml"

    @property
    def data_dir(self) -> Path:
        """Return the active data directory."""
        return get_data_dir()

    def _load_config(self):
        """Load configuration from file."""
        if self.config_file.exists():
            with open(self.config_file, "r") as f:
                self._config = yaml.safe_load(f) or {}
        else:
            self._config = self._default_config()
            self.save()

    def _default_config(self) -> Dict[str, Any]:
        """Return default configuration."""
        return {
            "index": {
                "directories": [],
                "excluded_patterns": ["node_modules", ".git", "__pycache__", "*.tmp"],
                "max_file_size_mb": 50,
            },
            "search": {
                "default_result_limit": 10,
                "max_results": 10,
                "min_confidence_threshold": 0.0,
            },
            "models": {"embedding_model": "all-MiniLM-L6-v2", "device": "cpu"},
            "monitoring": {"interval_seconds": 5, "enabled": False},
            "anomaly_detection": {
                "enabled": True,
                "contamination": 0.1,
                "min_training_samples": 25,
                "history_window": 100,
                "retrain_interval": 25,
                "cpu_threshold": 80.0,
                "memory_threshold": 80.0,
                "anomaly_score_threshold": -0.5,
            },
            "temporal_intelligence": {
                "correlation_window_seconds": 60.0,
                "correlation_min_support": 0.01,
                "pattern_sequence_min_count": 2,
                "pattern_burst_window_seconds": 30.0,
                "pattern_burst_min_events": 5,
                "explanation_window_seconds": 300.0,
            },
        }

    def save(self):
        """Save configuration to file."""
        self.config_dir.mkdir(parents=True, exist_ok=True)
        with open(self.config_file, "w") as f:
            yaml.dump(self._config, f, default_flow_style=False)

    def get(self, key: str, default: Any = None) -> Any:
        """Get configuration value."""
        keys = key.split(".")
        value = self._config
        for k in keys:
            if isinstance(value, dict):
                value = value.get(k, default)
            else:
                return default
        return value

    def set(self, key: str, value: Any):
        """Set configuration value."""
        keys = key.split(".")
        config = self._config
        for k in keys[:-1]:
            if k not in config:
                config[k] = {}
            config = config[k]
        config[keys[-1]] = value
        self.save()


# Global config instance
config = Config()
