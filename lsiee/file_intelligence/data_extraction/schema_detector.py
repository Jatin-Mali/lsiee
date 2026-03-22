"""Detect schema of structured data files."""

import logging
from pathlib import Path
from typing import Any, Dict, List, Optional

import pandas as pd

logger = logging.getLogger(__name__)


class SchemaDetector:
    """Detect schema from structured data files."""

    def detect_csv_schema(self, filepath: Path) -> List[Dict[str, Any]]:
        """Detect schema from a CSV file."""
        try:
            df = pd.read_csv(filepath, nrows=1000)
            return self._analyze_dataframe(df)
        except Exception as exc:
            logger.error("Error detecting CSV schema for %s: %s", filepath, exc)
            return []

    def detect_excel_schema(
        self,
        filepath: Path,
        sheet_name: Optional[str] = None,
    ) -> Dict[str, List[Dict[str, Any]]]:
        """Detect schema from an Excel file."""
        try:
            if sheet_name:
                df = pd.read_excel(filepath, sheet_name=sheet_name, nrows=1000)
                return {sheet_name: self._analyze_dataframe(df)}

            excel_file = pd.ExcelFile(filepath)
            schemas: Dict[str, List[Dict[str, Any]]] = {}
            for sheet in excel_file.sheet_names:
                df = pd.read_excel(filepath, sheet_name=sheet, nrows=1000)
                schemas[sheet] = self._analyze_dataframe(df)
            return schemas
        except Exception as exc:
            logger.error("Error detecting Excel schema for %s: %s", filepath, exc)
            return {}

    def _analyze_dataframe(self, df: pd.DataFrame) -> List[Dict[str, Any]]:
        """Analyze a DataFrame and return column schema details."""
        schema = []

        for index, column in enumerate(df.columns):
            series = df[column]
            column_info: Dict[str, Any] = {
                "column_index": index,
                "column_name": str(column),
                "column_type": str(series.dtype),
                "null_count": int(series.isnull().sum()),
                "unique_count": int(series.nunique(dropna=True)),
                "sample_values": [
                    self._to_python(value) for value in series.dropna().head(5).tolist()
                ],
            }

            if pd.api.types.is_numeric_dtype(series):
                numeric_series = series.dropna()
                if not numeric_series.empty:
                    column_info["min_value"] = float(numeric_series.min())
                    column_info["max_value"] = float(numeric_series.max())

            schema.append(column_info)

        return schema

    def _to_python(self, value: Any) -> Any:
        """Convert pandas/numpy scalars into JSON-friendly Python values."""
        if hasattr(value, "item"):
            try:
                return value.item()
            except Exception:
                return value
        return value
