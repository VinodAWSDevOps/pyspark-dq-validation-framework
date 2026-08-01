"""SCD (slowly changing dimension) integrity validator. Pure databricks-sql-connector
-- no pyspark imports.
"""
from __future__ import annotations

from typing import List

from framework.config_loader import TableConfig
from framework.db_connection import run_query
from framework.validators.base import ValidationResult

VALIDATOR_NAME = "scd_validator"


def validate_scd(table_config: TableConfig) -> ValidationResult:
    if table_config.scd_type in ("SCD1", "none"):
        return ValidationResult(
            table_name=table_config.table_name,
            validator_name=VALIDATOR_NAME,
            passed=True,
            issues=[],
            details={},
        )

    primary_key = table_config.primary_key
    is_current_col = table_config.is_current_col
    effective_end_col = table_config.effective_end_col
    silver_target = table_config.silver_target

    issues: List[str] = []

    # a. Every primary key must have exactly one is_current=true row.
    bad_current_keys = run_query(
        f"SELECT {primary_key}, COUNT(*) as current_count FROM {silver_target} "
        f"WHERE {is_current_col} = true GROUP BY {primary_key} HAVING COUNT(*) != 1"
    )
    keys_without_exactly_one_current = len(bad_current_keys)
    if keys_without_exactly_one_current > 0:
        issues.append(
            f"{keys_without_exactly_one_current} primary key(s) have zero or multiple is_current=true rows, "
            f"expected exactly 1"
        )

    # b. Current rows must have an open (9999-12-31) effective_end_date.
    current_rows_with_wrong_end_date = run_query(
        f"SELECT COUNT(*) AS violation_count FROM {silver_target} "
        f"WHERE {is_current_col} = true AND {effective_end_col} != DATE'9999-12-31'"
    )[0]["violation_count"]
    if current_rows_with_wrong_end_date > 0:
        issues.append(
            f"{current_rows_with_wrong_end_date} rows with is_current=true have an effective_end_date "
            f"other than 9999-12-31"
        )

    # c. Historical (superseded) rows must have a real closure date, not still-open.
    historical_rows_with_open_end_date = run_query(
        f"SELECT COUNT(*) AS violation_count FROM {silver_target} "
        f"WHERE {is_current_col} = false AND {effective_end_col} = DATE'9999-12-31'"
    )[0]["violation_count"]
    if historical_rows_with_open_end_date > 0:
        issues.append(
            f"{historical_rows_with_open_end_date} rows with is_current=false still have an open-ended "
            f"effective_end_date of 9999-12-31, expected a real closure date"
        )

    details = {
        "keys_without_exactly_one_current": keys_without_exactly_one_current,
        "current_rows_with_wrong_end_date": current_rows_with_wrong_end_date,
        "historical_rows_with_open_end_date": historical_rows_with_open_end_date,
    }

    return ValidationResult(
        table_name=table_config.table_name,
        validator_name=VALIDATOR_NAME,
        passed=len(issues) == 0,
        issues=issues,
        details=details,
    )
