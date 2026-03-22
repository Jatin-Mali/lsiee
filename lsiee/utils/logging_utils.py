"""Logging utilities for LSIEE."""

import logging
import sys
from pathlib import Path


def setup_logging(level=logging.INFO):
    """Set up logging configuration."""
    log_dir = Path.home() / ".lsiee" / "logs"
    log_dir.mkdir(parents=True, exist_ok=True)

    log_file = log_dir / "lsiee.log"

    # Create formatter
    formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")

    # File handler
    file_handler = logging.FileHandler(log_file)
    file_handler.setLevel(level)
    file_handler.setFormatter(formatter)

    # Console handler
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(logging.WARNING)
    console_handler.setFormatter(formatter)

    # Root logger
    root_logger = logging.getLogger()
    root_logger.setLevel(level)
    root_logger.addHandler(file_handler)
    root_logger.addHandler(console_handler)

    return root_logger
