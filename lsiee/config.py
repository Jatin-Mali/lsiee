"""Configuration management for LSIEE."""

import os
from pathlib import Path
from typing import Dict, Any

import yaml


class Config:
    """LSIEE configuration manager."""
    
    def __init__(self):
        self.config_dir = Path.home() / ".lsiee"
        self.config_file = self.config_dir / "config.yaml"
        self.data_dir = self.config_dir
        
        self._config: Dict[str, Any] = {}
        self._load_config()
    
    def _load_config(self):
        """Load configuration from file."""
        if self.config_file.exists():
            with open(self.config_file, 'r') as f:
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
                "max_file_size_mb": 50
            },
            "search": {
                "default_result_limit": 10,
                "min_confidence_threshold": 0.5
            },
            "models": {
                "embedding_model": "all-MiniLM-L6-v2",
                "device": "cpu"
            },
            "monitoring": {
                "interval_seconds": 5,
                "enabled": False
            }
        }
    
    def save(self):
        """Save configuration to file."""
        self.config_dir.mkdir(parents=True, exist_ok=True)
        with open(self.config_file, 'w') as f:
            yaml.dump(self._config, f, default_flow_style=False)
    
    def get(self, key: str, default: Any = None) -> Any:
        """Get configuration value."""
        keys = key.split('.')
        value = self._config
        for k in keys:
            if isinstance(value, dict):
                value = value.get(k, default)
            else:
                return default
        return value
    
    def set(self, key: str, value: Any):
        """Set configuration value."""
        keys = key.split('.')
        config = self._config
        for k in keys[:-1]:
            if k not in config:
                config[k] = {}
            config = config[k]
        config[keys[-1]] = value
        self.save()


# Global config instance
config = Config()
