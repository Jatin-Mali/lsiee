"""Format and export structured query results."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pandas as pd


class ResultFormatter:
    """Format query results for display or export."""

    def format_table(self, results: Any) -> str:
        """Format results as a simple text table."""
        if results in (None, [], {}):
            return "No results"

        if isinstance(results, list):
            df = pd.DataFrame(results)
            return df.to_string(index=False) if not df.empty else "No results"

        if isinstance(results, dict):
            df = pd.DataFrame(results.items(), columns=["key", "value"])
            return df.to_string(index=False)

        return str(results)

    def format_json(self, results: Any) -> str:
        """Format results as JSON."""
        return json.dumps(results, indent=2, default=str)

    def format_csv(self, results: Any) -> str:
        """Format results as CSV."""
        return self._to_dataframe(results).to_csv(index=False)

    def export_to_file(self, results: Any, output_path: Path, format: str = "csv") -> None:
        """Export results to CSV or JSON."""
        output_path.parent.mkdir(parents=True, exist_ok=True)

        if format == "json":
            output_path.write_text(self.format_json(results), encoding="utf-8")
            return

        self._to_dataframe(results).to_csv(output_path, index=False)

    def _to_dataframe(self, results: Any) -> pd.DataFrame:
        """Convert supported result shapes into a DataFrame."""
        if isinstance(results, list):
            return pd.DataFrame(results)

        if isinstance(results, dict):
            return pd.DataFrame(results.items(), columns=["key", "value"])

        return pd.DataFrame([{"value": results}])
