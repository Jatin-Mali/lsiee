"""Logging utilities for LSIEE."""

import logging
import os
import re
import sys
from logging.handlers import RotatingFileHandler

from lsiee.config import config, get_data_dir
from lsiee.security import display_path, ensure_safe_directory, sanitize_terminal_text


class SensitiveDataFilter(logging.Filter):
    """Redact common secrets and absolute paths before log emission."""

    _SECRET_RE = re.compile(
        r"(?i)\b(password|token|secret|api[_-]?key|authorization|cookie|session)\b\s*[:=]\s*\S+"
    )

    def filter(self, record: logging.LogRecord) -> bool:
        message = record.getMessage()
        record.msg = self._redact(message)
        record.args = ()
        return True

    def _redact(self, message: str) -> str:
        redacted = self._SECRET_RE.sub(r"\1=[REDACTED]", message)
        for token in re.findall(r"(~?/[^\s]+)", redacted):
            try:
                redacted = redacted.replace(token, display_path(token))
            except Exception:
                continue
        max_chars = int(config.get("security.max_log_message_chars", 1024))
        return sanitize_terminal_text(redacted, max_length=max_chars, single_line=False)


def setup_logging(level=logging.INFO):
    """Set up logging configuration."""
    log_dir = get_data_dir() / "logs"
    # Create formatter
    formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
    log_filter = SensitiveDataFilter()

    # Console handler
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(max(level, logging.WARNING))
    console_handler.setFormatter(formatter)
    console_handler.addFilter(log_filter)
    console_handler.set_name("lsiee-console")

    # Root logger
    root_logger = logging.getLogger()
    root_logger.setLevel(level)
    existing_handlers = {handler.get_name() for handler in root_logger.handlers}
    if "lsiee-console" not in existing_handlers:
        root_logger.addHandler(console_handler)

    if "lsiee-file" not in existing_handlers:
        try:
            ensure_safe_directory(log_dir.parent, must_exist=False)
            log_dir.mkdir(parents=True, exist_ok=True)
            try:
                os.chmod(log_dir, 0o700)
            except OSError:
                pass
            log_file = log_dir / "lsiee.log"
            file_handler = RotatingFileHandler(
                log_file,
                maxBytes=int(config.get("logging.max_file_size_mb", 10) * 1024 * 1024),
                backupCount=int(config.get("logging.backup_count", 5)),
                encoding="utf-8",
            )
            file_handler.setLevel(level)
            file_handler.setFormatter(formatter)
            file_handler.addFilter(log_filter)
            file_handler.set_name("lsiee-file")
            try:
                os.chmod(log_file, 0o600)
            except OSError:
                pass
            root_logger.addHandler(file_handler)
        except OSError:
            # Some tests and restricted environments do not allow writes to the
            # default data directory. Keep console logging available instead.
            pass

    return root_logger
