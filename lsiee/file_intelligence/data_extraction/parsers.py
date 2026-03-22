"""Parsers for structured data files."""

import json
import logging
from pathlib import Path
from typing import Any, Dict, Optional

import pandas as pd

logger = logging.getLogger(__name__)


class StructuredDataParser:
    """Parse structured data files and summarize their contents."""

    def parse_csv(self, filepath: Path) -> Dict[str, Any]:
        """Parse a CSV file."""
        try:
            df = pd.read_csv(filepath)
            return self._build_dataframe_result(df)
        except Exception as exc:
            logger.error("Error parsing CSV %s: %s", filepath, exc)
            return {"error": str(exc)}

    def parse_excel(self, filepath: Path, sheet_name: Optional[str] = None) -> Dict[str, Any]:
        """Parse an Excel file."""
        try:
            excel_file = pd.ExcelFile(filepath)

            if sheet_name:
                df = pd.read_excel(filepath, sheet_name=sheet_name)
                result = self._build_dataframe_result(df)
                result["sheet"] = sheet_name
                return result

            result: Dict[str, Any] = {
                "sheet_count": len(excel_file.sheet_names),
                "sheets": {},
            }
            for sheet in excel_file.sheet_names:
                df = pd.read_excel(filepath, sheet_name=sheet)
                result["sheets"][sheet] = {
                    "row_count": len(df),
                    "column_count": len(df.columns),
                    "columns": [str(column) for column in df.columns.tolist()],
                    "summary": self._generate_summary(df),
                }
            return result
        except Exception as exc:
            logger.error("Error parsing Excel %s: %s", filepath, exc)
            return {"error": str(exc)}

    def parse_json(self, filepath: Path) -> Dict[str, Any]:
        """Parse a JSON file."""
        try:
            with open(filepath, "r", encoding="utf-8") as file:
                data = json.load(file)

            return {
                "type": type(data).__name__,
                "structure": self._analyze_json_structure(data),
                "sample": json.dumps(data, indent=2, default=str)[:500],
            }
        except Exception as exc:
            logger.error("Error parsing JSON %s: %s", filepath, exc)
            return {"error": str(exc)}

    def extract_json_path(self, filepath: Path, json_path: str) -> Any:
        """Extract a value from a JSON file using dot notation with list indexes."""
        with open(filepath, "r", encoding="utf-8") as file:
            value: Any = json.load(file)

        for part in json_path.split("."):
            if not part:
                continue

            cursor = part
            while cursor:
                if "[" in cursor:
                    key, remainder = cursor.split("[", 1)
                    if key:
                        value = value[key]
                    index_text, cursor = remainder.split("]", 1)
                    value = value[int(index_text)]
                else:
                    value = value[cursor]
                    cursor = ""

        return value

    def _build_dataframe_result(self, df: pd.DataFrame) -> Dict[str, Any]:
        """Build a normalized result payload for a DataFrame."""
        return {
            "row_count": len(df),
            "column_count": len(df.columns),
            "columns": [str(column) for column in df.columns.tolist()],
            "dtypes": {
                str(key): str(value) for key, value in df.dtypes.astype(str).to_dict().items()
            },
            "head": self._normalize_records(df.head(5).to_dict("records")),
            "summary": self._generate_summary(df),
        }

    def _generate_summary(self, df: pd.DataFrame) -> Dict[str, Any]:
        """Generate summary statistics for numeric columns."""
        summary: Dict[str, Any] = {}
        numeric_columns = df.select_dtypes(include=["number"]).columns

        for column in numeric_columns:
            series = df[column].dropna()
            if series.empty:
                continue
            summary[str(column)] = {
                "mean": float(series.mean()),
                "median": float(series.median()),
                "min": float(series.min()),
                "max": float(series.max()),
            }

        return summary

    def _analyze_json_structure(self, data: Any, max_depth: int = 3) -> Dict[str, Any]:
        """Analyze JSON structure recursively."""
        if max_depth == 0:
            return {"type": type(data).__name__}

        if isinstance(data, dict):
            return {
                "type": "object",
                "keys": list(data.keys())[:10],
                "sample_structure": {
                    key: self._analyze_json_structure(value, max_depth - 1)
                    for key, value in list(data.items())[:3]
                },
            }

        if isinstance(data, list):
            structure: Dict[str, Any] = {
                "type": "array",
                "length": len(data),
                "item_type": type(data[0]).__name__ if data else "empty",
            }
            if data and max_depth > 1:
                structure["sample_structure"] = self._analyze_json_structure(data[0], max_depth - 1)
            return structure

        return {"type": type(data).__name__}

    def _normalize_records(self, records: list[dict[str, Any]]) -> list[dict[str, Any]]:
        """Convert DataFrame records into JSON-friendly values."""
        normalized = []
        for record in records:
            normalized.append({str(key): self._to_python(value) for key, value in record.items()})
        return normalized

    def _to_python(self, value: Any) -> Any:
        """Convert pandas/numpy scalars into JSON-friendly Python values."""
        if hasattr(value, "item"):
            try:
                return value.item()
            except Exception:
                return value
        return value
