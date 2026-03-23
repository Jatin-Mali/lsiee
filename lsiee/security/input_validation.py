"""Input validation helpers for LSIEE."""

from __future__ import annotations

import re
from typing import Optional

MAX_QUERY_LENGTH = 500
MAX_QUERY_CONDITIONS = 3
MAX_IDENTIFIER_LENGTH = 64
MAX_JSON_PATH_LENGTH = 256
MAX_GENERIC_TEXT_LENGTH = 512

_IDENTIFIER_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_ ]{0,63}$")
_JSON_PATH_RE = re.compile(r"^[A-Za-z0-9_.\[\]-]+$")
_SUSPICIOUS_QUERY_SHELL_RE = re.compile(r"[;&|`]|(?:\$\()")
_SUSPICIOUS_GENERIC_SHELL_RE = re.compile(r"[;&|`<>]|(?:\$\()")


class SecurityValidationError(ValueError):
    """Raised when untrusted input fails validation."""


def validate_query_text(
    query: str,
    *,
    max_length: int = MAX_QUERY_LENGTH,
    max_conditions: int = MAX_QUERY_CONDITIONS,
) -> str:
    """Validate free-form query text before parsing."""
    normalized = " ".join(query.strip().split())
    if not normalized:
        raise SecurityValidationError("Query cannot be empty")
    if len(normalized) > max_length:
        raise SecurityValidationError("Query exceeds the maximum supported length")
    if _SUSPICIOUS_QUERY_SHELL_RE.search(normalized):
        raise SecurityValidationError("Query contains unsupported shell metacharacters")

    conditions = len(re.findall(r"(?:>=|<=|==|!=|=|>|<)", normalized))
    if conditions > max_conditions:
        raise SecurityValidationError("Query is too complex")

    return normalized


def validate_column_identifier(value: str, *, allow_empty: bool = False) -> str:
    """Validate a column-like identifier used for column resolution."""
    normalized = " ".join(value.strip().split())
    if not normalized:
        if allow_empty:
            return normalized
        raise SecurityValidationError("Column name cannot be empty")
    if not _IDENTIFIER_RE.fullmatch(normalized):
        raise SecurityValidationError("Column name contains unsupported characters")
    return normalized


def validate_json_path(json_path: Optional[str]) -> Optional[str]:
    """Validate dot/bracket JSON path input."""
    if json_path is None:
        return None

    normalized = json_path.strip()
    if not normalized:
        raise SecurityValidationError("JSON path cannot be empty")
    if len(normalized) > MAX_JSON_PATH_LENGTH:
        raise SecurityValidationError("JSON path is too long")
    if not _JSON_PATH_RE.fullmatch(normalized):
        raise SecurityValidationError("JSON path contains unsupported characters")
    if ".." in normalized or "[-" in normalized:
        raise SecurityValidationError("JSON path contains an invalid segment")
    return normalized


def validate_positive_int(
    value: int,
    *,
    name: str,
    minimum: int = 1,
    maximum: int = 1_000_000,
) -> int:
    """Validate bounded integer inputs."""
    if not isinstance(value, int):
        raise SecurityValidationError(f"{name} must be an integer")
    if not (minimum <= value <= maximum):
        raise SecurityValidationError(f"{name} must be between {minimum} and {maximum}")
    return value


def validate_positive_float(
    value: float,
    *,
    name: str,
    minimum: float = 0.0,
    maximum: float = 1_000_000.0,
) -> float:
    """Validate bounded numeric float inputs."""
    try:
        normalized = float(value)
    except (TypeError, ValueError) as exc:
        raise SecurityValidationError(f"{name} must be a number") from exc
    if not (minimum <= normalized <= maximum):
        raise SecurityValidationError(f"{name} must be between {minimum} and {maximum}")
    return normalized


def validate_generic_text(
    value: Optional[str],
    *,
    name: str,
    max_length: int = MAX_GENERIC_TEXT_LENGTH,
    allow_empty: bool = False,
    reject_shell_metacharacters: bool = False,
) -> str:
    """Validate general CLI text input that is not a structured query."""
    normalized = " ".join(str(value or "").strip().split())
    if not normalized:
        if allow_empty:
            return normalized
        raise SecurityValidationError(f"{name} cannot be empty")
    if len(normalized) > max_length:
        raise SecurityValidationError(f"{name} exceeds the maximum supported length")
    if any(ord(char) < 32 for char in normalized):
        raise SecurityValidationError(f"{name} contains unsupported control characters")
    if reject_shell_metacharacters and _SUSPICIOUS_GENERIC_SHELL_RE.search(normalized):
        raise SecurityValidationError(f"{name} contains unsupported shell metacharacters")
    return normalized
