"""Format and export structured query results."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pandas as pd

from lsiee.security import atomic_write_text, ensure_safe_output_path, sanitize_terminal_text


class ResultFormatter:
    """Format query results for display or export."""

    def format_table(self, results: Any) -> str:
        """Format results as a simple text table."""
        if results in (None, [], {}):
            return "No results"

        if isinstance(results, list):
            df = pd.DataFrame(self._sanitize_rows(results))
            return df.to_string(index=False) if not df.empty else "No results"

        if isinstance(results, dict):
            df = pd.DataFrame(
                [
                    (sanitize_terminal_text(key), sanitize_terminal_text(value))
                    for key, value in results.items()
                ],
                columns=["key", "value"],
            )
            return df.to_string(index=False)

        return sanitize_terminal_text(results)

    def format_json(self, results: Any) -> str:
        """Format results as JSON."""
        return json.dumps(results, indent=2, default=str)

    def format_csv(self, results: Any) -> str:
        """Format results as CSV."""
        return self._to_dataframe(results).to_csv(index=False)

    def export_to_file(self, results: Any, output_path: Path, format: str = "csv") -> None:
        """Export results to CSV or JSON."""
        safe_path = ensure_safe_output_path(output_path)

        if format == "json":
            atomic_write_text(safe_path, self.format_json(results))
            return

        atomic_write_text(safe_path, self._to_dataframe(results).to_csv(index=False))

    def _to_dataframe(self, results: Any) -> pd.DataFrame:
        """Convert supported result shapes into a DataFrame."""
        if isinstance(results, list):
            return pd.DataFrame(results)

        if isinstance(results, dict):
            return pd.DataFrame(results.items(), columns=["key", "value"])

        return pd.DataFrame([{"value": results}])

    @staticmethod
    def _sanitize_rows(results: list[Any]) -> list[Any]:
        """Sanitize string cells when rendering untrusted rows to the terminal."""
        sanitized = []
        for row in results:
            if isinstance(row, dict):
                sanitized.append(
                    {
                        sanitize_terminal_text(key): (
                            sanitize_terminal_text(value) if isinstance(value, str) else value
                        )
                        for key, value in row.items()
                    }
                )
            else:
                sanitized.append(row)
        return sanitized
