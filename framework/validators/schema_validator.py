"""Schema validator. Pure databricks-sql-connector -- no pyspark/databricks-connect
imports, since this runs against the Silver table over SQL, not a live Spark session.
"""
from __future__ import annotations

from typing import Any, Dict, List

from framework.config_loader import TableConfig
from framework.db_connection import describe_table
from framework.validators.base import ValidationResult

VALIDATOR_NAME = "schema_validator"

PIPELINE_METADATA_COLUMNS = ["batch_id", "load_timestamp", "_bronze_ingest_timestamp", "_source_file"]


def _numeric_business_rule_columns(business_rules: Dict[str, Any]) -> List[str]:
    numeric_keys = {"min", "max"}
    return [
        col_name
        for col_name, rules in (business_rules or {}).items()
        if isinstance(rules, dict) and numeric_keys & set(rules.keys())
    ]


def _build_expected_columns(table_config: TableConfig) -> List[str]:
    columns = list(table_config.expected_file_format.get("expected_headers", []))
    columns += PIPELINE_METADATA_COLUMNS
    if table_config.scd_type == "SCD2":
        columns += [table_config.effective_start_col, table_config.effective_end_col, table_config.is_current_col]
    return columns


def _actual_columns(table_config: TableConfig) -> Dict[str, str]:
    """col_name -> data_type, skipping DESCRIBE TABLE's blank/'#'-section rows."""
    rows = describe_table(table_config.silver_target)
    actual = {}
    for row in rows:
        col_name = row.get("col_name")
        if not col_name or col_name.startswith("#"):
            continue
        actual[col_name] = row.get("data_type", "")
    return actual


def validate_schema(table_config: TableConfig) -> ValidationResult:
    expected_columns = _build_expected_columns(table_config)
    expected_set = set(expected_columns)

    actual_columns = _actual_columns(table_config)
    actual_set = set(actual_columns.keys())

    issues: List[str] = []

    for col_name in sorted(expected_set - actual_set):
        issues.append(f"Missing expected column: {col_name}")

    for col_name in sorted(actual_set - expected_set):
        issues.append(f"Unexpected column found: {col_name}")

    for col_name in table_config.date_columns or []:
        actual_type = actual_columns.get(col_name)
        if actual_type is None:
            continue  # already reported above as missing
        if actual_type.lower() != "date":
            issues.append(f"Column {col_name} expected type DATE, found {actual_type}")

    for col_name in _numeric_business_rule_columns(table_config.business_rules):
        actual_type = actual_columns.get(col_name)
        if actual_type is None:
            continue  # already reported above as missing
        normalized = actual_type.lower()
        if not (normalized.startswith("decimal") or normalized == "double"):
            issues.append(f"Column {col_name} expected type DECIMAL or DOUBLE, found {actual_type}")

    return ValidationResult(
        table_name=table_config.table_name,
        validator_name=VALIDATOR_NAME,
        passed=len(issues) == 0,
        issues=issues,
        details={
            "expected_column_count": len(expected_columns),
            "actual_column_count": len(actual_columns),
        },
    )
