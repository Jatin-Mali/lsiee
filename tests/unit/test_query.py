"""Tests for structured query execution and formatting."""

from pathlib import Path

import pandas as pd

from lsiee.file_intelligence.data_extraction.query_executor import QueryExecutor
from lsiee.file_intelligence.data_extraction.result_formatter import ResultFormatter


def test_filter_query_returns_matching_rows(tmp_path):
    """Filtering should return only rows that match the condition."""
    filepath = tmp_path / "sales.csv"
    pd.DataFrame({"amount": [100, 200, 300], "region": ["east", "west", "west"]}).to_csv(
        filepath,
        index=False,
    )

    result = QueryExecutor().execute_query(filepath, "get all rows where amount > 150")

    assert result["success"] is True
    assert len(result["result"]) == 2
    assert result["result"][0]["amount"] == 200


def test_sum_query_returns_numeric_total(tmp_path):
    """Sum queries should aggregate numeric columns."""
    filepath = tmp_path / "values.csv"
    pd.DataFrame({"value": [10, 20, 30]}).to_csv(filepath, index=False)

    result = QueryExecutor().execute_query(filepath, "sum of value")

    assert result["result"] == 60.0


def test_average_query_returns_numeric_mean(tmp_path):
    """Average queries should compute the column mean."""
    filepath = tmp_path / "ages.csv"
    pd.DataFrame({"age": [20, 30, 40]}).to_csv(filepath, index=False)

    result = QueryExecutor().execute_query(filepath, "average age")

    assert result["result"] == 30.0


def test_groupby_query_returns_aggregated_mapping(tmp_path):
    """Group-by queries should return grouped aggregation results."""
    filepath = tmp_path / "sales.csv"
    pd.DataFrame(
        {
            "region": ["east", "east", "west"],
            "sales": [10, 15, 20],
        }
    ).to_csv(filepath, index=False)

    result = QueryExecutor().execute_query(filepath, "sum of sales by region")

    assert result["result"] == {"east": 25, "west": 20}


def test_string_filter_query_matches_case_insensitively(tmp_path):
    """String filters should work without exact case matching."""
    filepath = tmp_path / "people.csv"
    pd.DataFrame({"name": ["Alice", "Bob"], "city": ["NYC", "la"]}).to_csv(filepath, index=False)

    result = QueryExecutor().execute_query(filepath, "filter city = LA")

    assert len(result["result"]) == 1
    assert result["result"][0]["name"] == "Bob"


def test_missing_column_returns_error(tmp_path):
    """Unknown columns should return a structured error."""
    filepath = tmp_path / "values.csv"
    pd.DataFrame({"value": [1, 2, 3]}).to_csv(filepath, index=False)

    result = QueryExecutor().execute_query(filepath, "sum of missing")

    assert "error" in result


def test_result_formatter_can_export_csv_and_json(tmp_path):
    """Formatter should export both CSV and JSON outputs."""
    formatter = ResultFormatter()
    csv_path = tmp_path / "results.csv"
    json_path = tmp_path / "results.json"
    results = [{"region": "east", "sales": 10}, {"region": "west", "sales": 20}]

    formatter.export_to_file(results, csv_path, format="csv")
    formatter.export_to_file(results, json_path, format="json")

    assert "region,sales" in csv_path.read_text(encoding="utf-8")
    assert '"region": "east"' in json_path.read_text(encoding="utf-8")


def test_execute_query_safe_uses_timeout_wrapper(tmp_path, monkeypatch):
    """Safe execution should surface timeout failures cleanly."""
    filepath = tmp_path / "values.csv"
    pd.DataFrame({"value": [1]}).to_csv(filepath, index=False)
    executor = QueryExecutor()

    def raise_timeout(_: Path, __: str):
        raise TimeoutError("slow")

    monkeypatch.setattr(executor, "execute_query", raise_timeout)

    result = executor.execute_query_safe(filepath, "count", timeout=1)

    assert result["error"] == "Query timeout - operation took too long"
