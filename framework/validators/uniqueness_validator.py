"""Uniqueness validator. Pure databricks-sql-connector -- no pyspark imports."""
from __future__ import annotations

from typing import List

from framework.config_loader import TableConfig
from framework.db_connection import run_query
from framework.validators.base import ValidationResult

VALIDATOR_NAME = "uniqueness_validator"


def validate_uniqueness(table_config: TableConfig) -> ValidationResult:
    primary_key = table_config.primary_key
    is_scd2 = table_config.scd_type == "SCD2"

    # SCD2 tables legitimately keep multiple physical rows per key (one per
    # historical version) -- without this filter every versioned key would
    # falsely read as a "duplicate". SCD1/none have exactly one row per key
    # already, so the query is unchanged for them.
    where_clause = f" WHERE {table_config.is_current_col} = true" if is_scd2 else ""
    sql = (
        f"SELECT {primary_key}, COUNT(*) as cnt FROM {table_config.silver_target}{where_clause} "
        f"GROUP BY {primary_key} HAVING COUNT(*) > 1"
    )
    duplicate_rows = run_query(sql)
    duplicate_key_count = len(duplicate_rows)  # distinct duplicated key values, not total duplicate rows

    issues: List[str] = []
    if duplicate_key_count > 0:
        if is_scd2:
            issues.append(
                f"Primary key {primary_key} has {duplicate_key_count} duplicate current-version value(s) "
                f"in Silver (filtered to {table_config.is_current_col} = true), expected 0"
            )
        else:
            issues.append(
                f"Primary key {primary_key} has {duplicate_key_count} duplicate value(s) in Silver, expected 0"
            )

    details = {
        "duplicate_key_count": duplicate_key_count,
        "scd2_current_filter_applied": is_scd2,
    }
    if duplicate_key_count > 0:
        details["sample_duplicate_keys"] = [row[primary_key] for row in duplicate_rows[:5]]

    return ValidationResult(
        table_name=table_config.table_name,
        validator_name=VALIDATOR_NAME,
        passed=len(issues) == 0,
        issues=issues,
        details=details,
    )
