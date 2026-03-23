"""Configuration management for LSIEE."""

import logging
import os
from pathlib import Path
from typing import Any, Dict

import yaml

from lsiee.security import atomic_write_text, ensure_safe_directory, read_secure_text

logger = logging.getLogger(__name__)


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
        if not self.config_file.exists():
            self._config = self._default_config()
            self.save()
            return

        try:
            payload = read_secure_text(self.config_file, max_bytes=1024 * 1024)
            loaded = yaml.safe_load(payload) or {}
            if not isinstance(loaded, dict):
                raise ValueError("Configuration root must be a mapping")
            self._config = self._validate_config(loaded)
        except Exception as exc:
            logger.warning("Invalid configuration detected; restoring defaults: %s", exc)
            self._config = self._default_config()
            self.save()

    def _default_config(self) -> Dict[str, Any]:
        """Return default configuration."""
        return {
            "index": {
                "directories": [],
                "excluded_patterns": ["node_modules", ".git", "__pycache__", "*.tmp"],
                "excluded_directories": [],
                "max_file_size_mb": 50,
            },
            "search": {
                "default_result_limit": 10,
                "max_results": 10,
                "min_confidence_threshold": 0.0,
            },
            "logging": {
                "level": "WARNING",
                "max_file_size_mb": 10,
                "backup_count": 5,
            },
            "models": {"embedding_model": "all-MiniLM-L6-v2", "device": "cpu"},
            "monitoring": {
                "interval_seconds": 5,
                "enabled": False,
                "current_user_only": True,
                "exclude_processes": [],
                "store_cmdline": False,
                "store_exe_path": False,
                "anonymize_process_names": False,
                "retention_days": 30,
            },
            "retention": {
                "process_snapshots_days": 30,
                "events_days": 90,
                "auto_cleanup_enabled": False,
            },
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
            "security": {
                "max_index_file_size_mb": 50,
                "max_parse_file_size_mb": 100,
                "max_query_length": 500,
                "max_query_conditions": 3,
                "max_query_results": 1000,
                "max_text_extract_bytes": 1024 * 1024,
                "max_json_bytes": 2 * 1024 * 1024,
                "max_vector_store_bytes": 50 * 1024 * 1024,
                "max_event_data_bytes": 16 * 1024,
                "max_log_message_chars": 1024,
                "min_search_document_chars": 10,
                "max_search_document_chars": 4000,
            },
        }

    def _validate_config(self, loaded: Dict[str, Any]) -> Dict[str, Any]:
        """Merge user config with secure defaults and coerce known settings."""
        merged = self._merge_dicts(self._default_config(), loaded)

        logging_cfg = merged["logging"]
        logging_cfg["level"] = self._coerce_log_level(logging_cfg.get("level"))
        logging_cfg["max_file_size_mb"] = self._coerce_int(
            logging_cfg.get("max_file_size_mb"),
            minimum=1,
            maximum=1024,
            default=10,
        )
        logging_cfg["backup_count"] = self._coerce_int(
            logging_cfg.get("backup_count"),
            minimum=1,
            maximum=20,
            default=5,
        )

        search_cfg = merged["search"]
        search_cfg["default_result_limit"] = self._coerce_int(
            search_cfg.get("default_result_limit"),
            minimum=1,
            maximum=100,
            default=10,
        )
        search_cfg["max_results"] = self._coerce_int(
            search_cfg.get("max_results"),
            minimum=1,
            maximum=1000,
            default=10,
        )
        search_cfg["min_confidence_threshold"] = self._coerce_float(
            search_cfg.get("min_confidence_threshold"),
            minimum=0.0,
            maximum=1.0,
            default=0.0,
        )

        index_cfg = merged["index"]
        index_cfg["excluded_patterns"] = self._coerce_string_list(
            index_cfg.get("excluded_patterns", [])
        )
        index_cfg["excluded_directories"] = self._coerce_string_list(
            index_cfg.get("excluded_directories", [])
        )
        index_cfg["max_file_size_mb"] = self._coerce_int(
            index_cfg.get("max_file_size_mb"),
            minimum=1,
            maximum=1024,
            default=50,
        )

        monitoring_cfg = merged["monitoring"]
        monitoring_cfg["interval_seconds"] = self._coerce_float(
            monitoring_cfg.get("interval_seconds"),
            minimum=0.01,
            maximum=3600.0,
            default=5.0,
        )
        monitoring_cfg["enabled"] = bool(monitoring_cfg.get("enabled", False))
        monitoring_cfg["current_user_only"] = bool(monitoring_cfg.get("current_user_only", True))
        monitoring_cfg["exclude_processes"] = self._coerce_string_list(
            monitoring_cfg.get("exclude_processes", [])
        )
        monitoring_cfg["store_cmdline"] = bool(monitoring_cfg.get("store_cmdline", False))
        monitoring_cfg["store_exe_path"] = bool(monitoring_cfg.get("store_exe_path", False))
        monitoring_cfg["anonymize_process_names"] = bool(
            monitoring_cfg.get("anonymize_process_names", False)
        )
        monitoring_cfg["retention_days"] = self._coerce_int(
            monitoring_cfg.get("retention_days"),
            minimum=1,
            maximum=3650,
            default=30,
        )

        retention_cfg = merged["retention"]
        retention_cfg["process_snapshots_days"] = self._coerce_int(
            retention_cfg.get("process_snapshots_days"),
            minimum=1,
            maximum=3650,
            default=30,
        )
        retention_cfg["events_days"] = self._coerce_int(
            retention_cfg.get("events_days"),
            minimum=1,
            maximum=3650,
            default=90,
        )
        retention_cfg["auto_cleanup_enabled"] = bool(
            retention_cfg.get("auto_cleanup_enabled", False)
        )

        anomaly_cfg = merged["anomaly_detection"]
        anomaly_cfg["enabled"] = bool(anomaly_cfg.get("enabled", True))
        anomaly_cfg["contamination"] = self._coerce_float(
            anomaly_cfg.get("contamination"),
            minimum=0.001,
            maximum=0.5,
            default=0.1,
        )
        anomaly_cfg["min_training_samples"] = self._coerce_int(
            anomaly_cfg.get("min_training_samples"),
            minimum=5,
            maximum=100000,
            default=25,
        )
        anomaly_cfg["history_window"] = self._coerce_int(
            anomaly_cfg.get("history_window"),
            minimum=10,
            maximum=100000,
            default=100,
        )
        anomaly_cfg["retrain_interval"] = self._coerce_int(
            anomaly_cfg.get("retrain_interval"),
            minimum=1,
            maximum=100000,
            default=25,
        )
        anomaly_cfg["cpu_threshold"] = self._coerce_float(
            anomaly_cfg.get("cpu_threshold"),
            minimum=1.0,
            maximum=100.0,
            default=80.0,
        )
        anomaly_cfg["memory_threshold"] = self._coerce_float(
            anomaly_cfg.get("memory_threshold"),
            minimum=1.0,
            maximum=100.0,
            default=80.0,
        )
        anomaly_cfg["anomaly_score_threshold"] = self._coerce_float(
            anomaly_cfg.get("anomaly_score_threshold"),
            minimum=-1000.0,
            maximum=1000.0,
            default=-0.5,
        )

        temporal_cfg = merged["temporal_intelligence"]
        temporal_cfg["correlation_window_seconds"] = self._coerce_float(
            temporal_cfg.get("correlation_window_seconds"),
            minimum=1.0,
            maximum=86400.0,
            default=60.0,
        )
        temporal_cfg["correlation_min_support"] = self._coerce_float(
            temporal_cfg.get("correlation_min_support"),
            minimum=0.001,
            maximum=1.0,
            default=0.01,
        )
        temporal_cfg["pattern_sequence_min_count"] = self._coerce_int(
            temporal_cfg.get("pattern_sequence_min_count"),
            minimum=1,
            maximum=1000,
            default=2,
        )
        temporal_cfg["pattern_burst_window_seconds"] = self._coerce_float(
            temporal_cfg.get("pattern_burst_window_seconds"),
            minimum=1.0,
            maximum=86400.0,
            default=30.0,
        )
        temporal_cfg["pattern_burst_min_events"] = self._coerce_int(
            temporal_cfg.get("pattern_burst_min_events"),
            minimum=2,
            maximum=100000,
            default=5,
        )
        temporal_cfg["explanation_window_seconds"] = self._coerce_float(
            temporal_cfg.get("explanation_window_seconds"),
            minimum=10.0,
            maximum=86400.0,
            default=300.0,
        )

        security_cfg = merged["security"]
        int_defaults = {
            "max_index_file_size_mb": (1, 1024, 50),
            "max_parse_file_size_mb": (1, 1024, 100),
            "max_query_length": (10, 10000, 500),
            "max_query_conditions": (1, 20, 3),
            "max_query_results": (1, 10000, 1000),
            "max_text_extract_bytes": (1024, 50 * 1024 * 1024, 1024 * 1024),
            "max_json_bytes": (1024, 50 * 1024 * 1024, 2 * 1024 * 1024),
            "max_vector_store_bytes": (1024, 500 * 1024 * 1024, 50 * 1024 * 1024),
            "max_event_data_bytes": (256, 1024 * 1024, 16 * 1024),
            "max_log_message_chars": (128, 16384, 1024),
            "min_search_document_chars": (1, 1000, 10),
            "max_search_document_chars": (128, 100000, 4000),
        }
        for key, (minimum, maximum, default) in int_defaults.items():
            security_cfg[key] = self._coerce_int(
                security_cfg.get(key),
                minimum=minimum,
                maximum=maximum,
                default=default,
            )

        return merged

    def _merge_dicts(self, base: Dict[str, Any], override: Dict[str, Any]) -> Dict[str, Any]:
        """Recursively merge dictionaries."""
        merged = dict(base)
        for key, value in override.items():
            if isinstance(value, dict) and isinstance(merged.get(key), dict):
                merged[key] = self._merge_dicts(merged[key], value)
            else:
                merged[key] = value
        return merged

    @staticmethod
    def _coerce_int(value: Any, *, minimum: int, maximum: int, default: int) -> int:
        try:
            normalized = int(value)
        except (TypeError, ValueError):
            return default
        return min(max(normalized, minimum), maximum)

    @staticmethod
    def _coerce_float(value: Any, *, minimum: float, maximum: float, default: float) -> float:
        try:
            normalized = float(value)
        except (TypeError, ValueError):
            return default
        return min(max(normalized, minimum), maximum)

    @staticmethod
    def _coerce_log_level(value: Any) -> str:
        normalized = str(value or "WARNING").upper()
        return (
            normalized
            if normalized in {"DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"}
            else "WARNING"
        )

    @staticmethod
    def _coerce_string_list(value: Any) -> list[str]:
        if not isinstance(value, list):
            return []
        cleaned = []
        for item in value:
            text = " ".join(str(item).strip().split())
            if text:
                cleaned.append(text[:128])
        return cleaned

    def save(self):
        """Save configuration to file."""
        ensure_safe_directory(self.config_dir, must_exist=False)
        self.config_dir.mkdir(parents=True, exist_ok=True)
        atomic_write_text(
            self.config_file,
            yaml.safe_dump(self._config, default_flow_style=False),
        )

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
