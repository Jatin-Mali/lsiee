"""Tests for structured data parsers."""

import json

import pandas as pd

from lsiee.file_intelligence.data_extraction.parsers import StructuredDataParser
from lsiee.file_intelligence.data_extraction.schema_detector import SchemaDetector


def test_csv_parsing(tmp_path):
    """CSV parsing should return shape and columns."""
    filepath = tmp_path / "people.csv"
    pd.DataFrame(
        {
            "name": ["Alice", "Bob", "Charlie"],
            "age": [25, 30, 35],
            "city": ["NYC", "LA", "SF"],
        }
    ).to_csv(filepath, index=False)

    result = StructuredDataParser().parse_csv(filepath)

    assert result["row_count"] == 3
    assert result["column_count"] == 3
    assert result["columns"] == ["name", "age", "city"]


def test_csv_parsing_includes_numeric_summary(tmp_path):
    """CSV parsing should summarize numeric columns."""
    filepath = tmp_path / "metrics.csv"
    pd.DataFrame({"value": [10, 20, 30]}).to_csv(filepath, index=False)

    result = StructuredDataParser().parse_csv(filepath)

    assert result["summary"]["value"]["mean"] == 20.0
    assert result["summary"]["value"]["max"] == 30.0


def test_schema_detection_for_csv(tmp_path):
    """Schema detector should infer column details for CSV."""
    filepath = tmp_path / "people.csv"
    pd.DataFrame(
        {
            "name": ["Alice", "Bob", None],
            "age": [25, 30, 35],
        }
    ).to_csv(filepath, index=False)

    schema = SchemaDetector().detect_csv_schema(filepath)

    assert len(schema) == 2
    assert schema[0]["column_name"] == "name"
    assert schema[0]["null_count"] == 1
    assert schema[1]["column_type"] in {"int64", "Int64"}
    assert schema[1]["min_value"] == 25.0
    assert schema[1]["max_value"] == 35.0


def test_excel_parse_all_sheets(tmp_path):
    """Excel parsing should include all sheets when no sheet is selected."""
    filepath = tmp_path / "workbook.xlsx"
    with pd.ExcelWriter(filepath) as writer:
        pd.DataFrame({"name": ["Alice", "Bob"]}).to_excel(writer, sheet_name="People", index=False)
        pd.DataFrame({"score": [90, 95]}).to_excel(writer, sheet_name="Scores", index=False)

    result = StructuredDataParser().parse_excel(filepath)

    assert result["sheet_count"] == 2
    assert set(result["sheets"].keys()) == {"People", "Scores"}
    assert result["sheets"]["Scores"]["row_count"] == 2


def test_excel_parse_specific_sheet(tmp_path):
    """Excel parsing should return DataFrame details for one sheet."""
    filepath = tmp_path / "workbook.xlsx"
    with pd.ExcelWriter(filepath) as writer:
        pd.DataFrame({"name": ["Alice", "Bob"]}).to_excel(writer, sheet_name="People", index=False)
        pd.DataFrame({"score": [90, 95]}).to_excel(writer, sheet_name="Scores", index=False)

    result = StructuredDataParser().parse_excel(filepath, sheet_name="Scores")

    assert result["sheet"] == "Scores"
    assert result["row_count"] == 2
    assert result["columns"] == ["score"]


def test_excel_schema_detection(tmp_path):
    """Excel schema detection should return per-sheet schema details."""
    filepath = tmp_path / "schema.xlsx"
    with pd.ExcelWriter(filepath) as writer:
        pd.DataFrame({"amount": [1.5, 2.5, 3.5]}).to_excel(writer, sheet_name="Totals", index=False)

    schemas = SchemaDetector().detect_excel_schema(filepath)

    assert "Totals" in schemas
    assert schemas["Totals"][0]["column_name"] == "amount"
    assert schemas["Totals"][0]["min_value"] == 1.5


def test_json_parsing_reports_structure(tmp_path):
    """JSON parsing should describe nested structure."""
    filepath = tmp_path / "data.json"
    payload = {
        "data": {
            "users": [{"name": "Alice", "active": True}],
            "count": 1,
        }
    }
    filepath.write_text(json.dumps(payload), encoding="utf-8")

    result = StructuredDataParser().parse_json(filepath)

    assert result["type"] == "dict"
    assert result["structure"]["type"] == "object"
    assert "data" in result["structure"]["keys"]


def test_json_path_extraction_from_nested_data(tmp_path):
    """JSON path extraction should support nested dict/list access."""
    filepath = tmp_path / "data.json"
    payload = {"data": {"users": [{"name": "Alice"}, {"name": "Bob"}]}}
    filepath.write_text(json.dumps(payload), encoding="utf-8")

    value = StructuredDataParser().extract_json_path(filepath, "data.users[1].name")

    assert value == "Bob"


def test_json_path_extraction_can_return_object(tmp_path):
    """JSON path extraction should return objects, not only scalars."""
    filepath = tmp_path / "data.json"
    payload = {"meta": {"summary": {"total": 2, "ok": True}}}
    filepath.write_text(json.dumps(payload), encoding="utf-8")

    value = StructuredDataParser().extract_json_path(filepath, "meta.summary")

    assert value == {"total": 2, "ok": True}


def test_parse_invalid_json_returns_error(tmp_path):
    """Invalid JSON should return an error payload."""
    filepath = tmp_path / "broken.json"
    filepath.write_text("{invalid json", encoding="utf-8")

    result = StructuredDataParser().parse_json(filepath)

    assert "error" in result


def test_parse_invalid_excel_sheet_returns_error(tmp_path):
    """Selecting a missing sheet should surface an error payload."""
    filepath = tmp_path / "workbook.xlsx"
    with pd.ExcelWriter(filepath) as writer:
        pd.DataFrame({"name": ["Alice"]}).to_excel(writer, sheet_name="People", index=False)

    result = StructuredDataParser().parse_excel(filepath, sheet_name="Missing")

    assert "error" in result
