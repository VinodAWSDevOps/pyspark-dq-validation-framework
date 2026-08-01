"""Completeness validator. Pure databricks-sql-connector -- no pyspark imports."""
from __future__ import annotations

from typing import Dict, List

from framework.config_loader import TableConfig
from framework.db_connection import run_query
from framework.validators.base import ValidationResult

VALIDATOR_NAME = "completeness_validator"


def validate_completeness(table_config: TableConfig) -> ValidationResult:
    mandatory_columns = table_config.mandatory_columns or []
    mandatory_column_count = len(mandatory_columns)

    if not mandatory_columns:
        total_row_count = run_query(f"SELECT COUNT(*) AS total_row_count FROM {table_config.silver_target}")[0][
            "total_row_count"
        ]
        return ValidationResult(
            table_name=table_config.table_name,
            validator_name=VALIDATOR_NAME,
            passed=True,
            issues=[],
            details={"total_row_count": total_row_count, "null_counts": {}},
        )

    # One query, one COUNT(CASE WHEN ... IS NULL THEN 1 END) per mandatory column.
    null_count_exprs = ", ".join(
        f"COUNT(CASE WHEN {col} IS NULL THEN 1 END) AS `{col}`" for col in mandatory_columns
    )
    sql = f"SELECT COUNT(*) AS total_row_count, {null_count_exprs} FROM {table_config.silver_target}"
    row = run_query(sql)[0]

    total_row_count = row["total_row_count"]
    null_counts: Dict[str, int] = {col: row[col] for col in mandatory_columns}

    issues: List[str] = []
    for col, count in null_counts.items():
        if count > 0:
            issues.append(f"Column {col} has {count} NULL values in {mandatory_column_count} mandatory column, expected 0")

    return ValidationResult(
        table_name=table_config.table_name,
        validator_name=VALIDATOR_NAME,
        passed=len(issues) == 0,
        issues=issues,
        details={"total_row_count": total_row_count, "null_counts": null_counts},
    )
