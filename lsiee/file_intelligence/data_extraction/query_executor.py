"""Execute natural-language queries on structured data files."""

from __future__ import annotations

import logging
import re
import signal
from io import BytesIO, StringIO
from pathlib import Path
from typing import Any, Dict

import pandas as pd

from lsiee.config import config
from lsiee.security import (
    PathSecurityError,
    read_secure_bytes,
    read_secure_text,
    validate_column_identifier,
    validate_positive_int,
    validate_query_text,
)

logger = logging.getLogger(__name__)


class QueryExecutor:
    """Parse and execute safe read-only queries on tabular data."""

    def __init__(self, max_result_rows: int = 1000):
        """Initialize the executor with a result-size limit."""
        self.max_result_rows = min(
            max_result_rows,
            int(config.get("security.max_query_results", 1000)),
        )
        self.supported_operations = [
            "filter",
            "sum",
            "average",
            "count",
            "group by",
            "max",
            "min",
            "sort",
        ]

    def execute_query(self, filepath: Path, query: str) -> Dict[str, Any]:
        """Execute a natural-language query on a CSV or Excel file."""
        try:
            query_text = validate_query_text(
                query,
                max_length=int(config.get("security.max_query_length", 500)),
                max_conditions=int(config.get("security.max_query_conditions", 3)),
            )
        except ValueError as exc:
            return {"error": str(exc)}

        df = self._load_file(filepath)
        if df is None:
            return {"error": "Could not load file"}

        operation = self._parse_query(query_text)

        try:
            result = self._execute_operation(df, operation)
        except Exception as exc:
            logger.error("Query execution error for %s: %s", filepath.name, exc)
            return {"error": "Query execution failed"}

        return {
            "success": True,
            "query": query_text,
            "operation": operation,
            "result": result,
        }

    def execute_query_safe(
        self,
        filepath: Path,
        query: str,
        timeout: int = 30,
    ) -> Dict[str, Any]:
        """Execute a query with a timeout guard."""
        try:
            validate_positive_int(timeout, name="timeout", maximum=300)
        except ValueError as exc:
            return {"error": str(exc)}

        def timeout_handler(signum, frame):  # pragma: no cover - signal callback
            raise TimeoutError("Query execution timeout")

        try:
            previous_handler = signal.getsignal(signal.SIGALRM)
            signal.signal(signal.SIGALRM, timeout_handler)
            signal.alarm(timeout)
        except ValueError:
            # Fallback when not running in the main thread.
            return self.execute_query(filepath, query)

        try:
            result = self.execute_query(filepath, query)
            signal.alarm(0)
            return result
        except TimeoutError:
            signal.alarm(0)
            return {"error": "Query timeout - operation took too long"}
        except Exception as exc:
            signal.alarm(0)
            return {"error": str(exc)}
        finally:
            signal.signal(signal.SIGALRM, previous_handler)

    def _load_file(self, filepath: Path) -> pd.DataFrame | None:
        """Load a supported tabular file into a DataFrame."""
        extension = filepath.suffix.lower()
        max_bytes = int(config.get("security.max_parse_file_size_mb", 100) * 1024 * 1024)

        try:
            if extension == ".csv":
                payload = read_secure_text(filepath, max_bytes=max_bytes)
                return pd.read_csv(StringIO(payload))
            if extension in {".xlsx", ".xls"}:
                payload = read_secure_bytes(filepath, max_bytes=max_bytes)
                return pd.read_excel(BytesIO(payload))
            return None
        except (PathSecurityError, OSError, ValueError) as exc:
            logger.error("Error loading %s: %s", filepath.name, exc)
            return None

    def _parse_query(self, query: str) -> Dict[str, Any]:
        """Parse a natural-language query into a safe operation description."""
        query_text = query.strip()
        lowered = query_text.lower()

        group_match = re.search(
            (
                r"\b(sum|average|avg|mean|max|min|count)\b(?:\s+of)?\s+"
                r"([\w\s]+?)\s+\bby\b\s+([\w\s]+)$"
            ),
            lowered,
        )
        if group_match:
            agg_func, agg_col, group_col = group_match.groups()
            return {
                "type": "groupby",
                "group_column": validate_column_identifier(group_col),
                "agg_column": validate_column_identifier(agg_col),
                "agg_function": self._normalize_agg_function(agg_func),
            }

        sort_match = re.search(r"\bsort\s+by\s+([\w\s]+?)(?:\s+(asc|desc))?$", lowered)
        if sort_match:
            column, direction = sort_match.groups()
            return {
                "type": "sort",
                "column": validate_column_identifier(column),
                "ascending": (direction or "asc") != "desc",
            }

        filter_source = lowered.split(" where ", 1)[1] if " where " in lowered else lowered
        filter_match = re.search(
            r"([\w\s]+?)\s*(>=|<=|==|=|!=|>|<)\s*(.+)$",
            filter_source,
        )
        if filter_match and (
            any(word in lowered for word in ["filter", "where", "get", "show"])
            or filter_source == lowered
        ):
            column, operator, raw_value = filter_match.groups()
            return {
                "type": "filter",
                "column": validate_column_identifier(
                    column.replace("all rows", "").replace("rows", "").strip()
                ),
                "operator": "==" if operator == "=" else operator,
                "value": self._parse_value(raw_value),
            }

        agg_match = re.search(
            r"\b(sum|average|avg|mean|max|min)\b(?:\s+of)?\s+([\w\s]+)$",
            lowered,
        )
        if agg_match:
            agg_func, column = agg_match.groups()
            return {
                "type": self._normalize_agg_function(agg_func),
                "column": validate_column_identifier(column),
            }

        if re.search(r"\bcount\b", lowered):
            return {"type": "count"}

        return {"type": "all"}

    def _execute_operation(self, df: pd.DataFrame, operation: Dict[str, Any]) -> Any:
        """Execute a parsed operation on the DataFrame."""
        op_type = operation["type"]

        if op_type == "filter":
            column = self._resolve_column(df, operation["column"])
            mask = self._build_filter_mask(df[column], operation["operator"], operation["value"])
            result_df = df.loc[mask].head(self.max_result_rows)
            return self._records(result_df)

        if op_type in {"sum", "average", "mean", "max", "min"}:
            column = self._resolve_column(df, operation["column"])
            series = pd.to_numeric(df[column], errors="coerce").dropna()
            if series.empty:
                raise ValueError(f"Column '{column}' does not contain numeric data")

            if op_type == "sum":
                return float(series.sum())
            if op_type in {"average", "mean"}:
                return float(series.mean())
            if op_type == "max":
                return float(series.max())
            return float(series.min())

        if op_type == "count":
            return int(len(df))

        if op_type == "groupby":
            group_column = self._resolve_column(df, operation["group_column"])
            agg_function = operation["agg_function"]

            if agg_function == "count":
                grouped = df.groupby(group_column).size()
            else:
                agg_column = self._resolve_column(df, operation["agg_column"])
                grouped = df.groupby(group_column)[agg_column].agg(agg_function)

            return {str(key): self._to_python(value) for key, value in grouped.to_dict().items()}

        if op_type == "sort":
            column = self._resolve_column(df, operation["column"])
            result_df = df.sort_values(by=column, ascending=operation["ascending"]).head(
                self.max_result_rows
            )
            return self._records(result_df)

        if op_type == "all":
            return self._records(df.head(min(10, self.max_result_rows)))

        raise ValueError(f"Unknown operation type: {op_type}")

    def _resolve_column(self, df: pd.DataFrame, requested: str) -> str:
        """Resolve a user-provided column name against DataFrame columns."""
        normalized_map = {self._normalize_name(str(column)): str(column) for column in df.columns}
        key = self._normalize_name(requested)

        if key in normalized_map:
            return normalized_map[key]

        partial_matches = [
            column
            for normalized, column in normalized_map.items()
            if key in normalized or normalized in key
        ]
        if len(partial_matches) == 1:
            return partial_matches[0]

        raise ValueError(f"Column '{requested}' not found")

    def _build_filter_mask(self, series: pd.Series, operator: str, value: Any) -> pd.Series:
        """Build a boolean mask for a filter expression."""
        numeric_series = pd.to_numeric(series, errors="coerce")

        if pd.notna(numeric_series).any() and isinstance(value, (int, float)):
            comparable = numeric_series
            comparison_value = value
        else:
            comparable = series.astype(str).str.strip().str.casefold()
            comparison_value = str(value).strip().casefold()

        if operator == ">":
            return comparable > comparison_value
        if operator == "<":
            return comparable < comparison_value
        if operator == ">=":
            return comparable >= comparison_value
        if operator == "<=":
            return comparable <= comparison_value
        if operator == "!=":
            return comparable != comparison_value
        if operator == "==":
            return comparable == comparison_value

        raise ValueError(f"Unsupported operator: {operator}")

    def _records(self, df: pd.DataFrame) -> list[dict[str, Any]]:
        """Convert DataFrame rows into JSON-friendly records."""
        return [
            {str(key): self._to_python(value) for key, value in record.items()}
            for record in df.to_dict("records")
        ]

    def _parse_value(self, raw_value: str) -> Any:
        """Parse a scalar value from query text."""
        value = raw_value.strip().strip("'\"")

        if value.lower() in {"true", "false"}:
            return value.lower() == "true"

        try:
            if "." in value:
                return float(value)
            return int(value)
        except ValueError:
            return value

    def _normalize_agg_function(self, name: str) -> str:
        """Map natural-language aggregation names to pandas aggregations."""
        aliases = {
            "avg": "mean",
            "average": "mean",
            "mean": "mean",
            "sum": "sum",
            "max": "max",
            "min": "min",
            "count": "count",
        }
        return aliases[name]

    def _normalize_name(self, value: str) -> str:
        """Normalize column-like text for matching."""
        return re.sub(r"[^a-z0-9]+", "", value.lower())

    def _to_python(self, value: Any) -> Any:
        """Convert pandas/numpy values into plain Python types."""
        if hasattr(value, "item"):
            try:
                return value.item()
            except Exception:
                return value
        return value
