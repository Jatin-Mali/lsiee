"""Output sanitization helpers for terminal-safe rendering."""

from __future__ import annotations

import re
import unicodedata
from typing import Any

from rich.markup import escape

ANSI_ESCAPE_RE = re.compile(r"\x1B(?:[@-Z\\-_]|\[[0-?]*[ -/]*[@-~])")
OSC_ESCAPE_RE = re.compile(r"\x1b\][^\x07\x1b]*(?:\x07|\x1b\\)")


def strip_ansi(text: str) -> str:
    """Remove ANSI/OSC terminal escape sequences."""
    stripped = OSC_ESCAPE_RE.sub("", str(text))
    return ANSI_ESCAPE_RE.sub("", stripped)


def sanitize_terminal_text(
    value: Any,
    *,
    max_length: int = 4096,
    single_line: bool = True,
) -> str:
    """Normalize untrusted text before rendering to the terminal."""
    normalized = unicodedata.normalize("NFKC", str(value or ""))
    stripped = strip_ansi(normalized)

    cleaned_chars = []
    for char in stripped:
        if char in {"\n", "\t"} and not single_line:
            cleaned_chars.append(char)
            continue
        if ord(char) < 32 or ord(char) == 127:
            continue
        cleaned_chars.append(char)

    cleaned = "".join(cleaned_chars)
    if single_line:
        cleaned = " ".join(cleaned.split())

    return cleaned[:max_length]


def sanitize_terminal_data(value: Any) -> Any:
    """Recursively sanitize data structures destined for terminal output."""
    if isinstance(value, dict):
        return {
            sanitize_terminal_text(key): sanitize_terminal_data(item) for key, item in value.items()
        }
    if isinstance(value, list):
        return [sanitize_terminal_data(item) for item in value]
    if isinstance(value, tuple):
        return tuple(sanitize_terminal_data(item) for item in value)
    if isinstance(value, str):
        return sanitize_terminal_text(value, single_line=False)
    return value


def safe_rich_text(value: Any, *, max_length: int = 4096, single_line: bool = True) -> str:
    """Return terminal-safe text escaped for Rich markup rendering."""
    return escape(
        sanitize_terminal_text(
            value,
            max_length=max_length,
            single_line=single_line,
        )
    )
