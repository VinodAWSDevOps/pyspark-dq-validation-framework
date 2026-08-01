"""SCD1 transformation-correctness validator. Pure databricks-sql-connector
-- no pyspark imports. Confirms Silver's current row for each key actually
reflects Bronze's latest incoming values for that key's tracked_columns.
"""
from __future__ import annotations

from typing import Dict, List

from framework.config_loader import TableConfig
from framework.db_connection import run_query
from framework.validators.base import ValidationResult

VALIDATOR_NAME = "transformation_validator"


def validate_transformation(table_config: TableConfig) -> ValidationResult:
    tracked_columns = table_config.tracked_columns or []

    if table_config.scd_type != "SCD1" or not tracked_columns:
        return ValidationResult(
            table_name=table_config.table_name,
            validator_name=VALIDATOR_NAME,
            passed=True,
            issues=[],
            details={},
        )

    primary_key = table_config.primary_key
    tracked_column_select = ", ".join(tracked_columns)

    # A later Bronze row that's an exact duplicate (same tracked_column values)
    # of an earlier row for the same key is a row Silver's own dedup step would
    # have quarantined as duplicate_primary_key, not a genuine update -- ranking
    # naively by load_timestamp would let it masquerade as "the latest value"
    # and produce a false-positive mismatch. So: fingerprint each row's tracked
    # values, keep only each (key, fingerprint) group's *first* occurrence
    # (MIN(load_timestamp)), and only rank/compare among those real, distinct
    # changes.
    fingerprint_parts = ", ".join(f"COALESCE(CAST({col} AS STRING), '<<NULL>>')" for col in tracked_columns)
    value_fingerprint_expr = f"CONCAT_WS('||', {fingerprint_parts})"

    mismatch_exprs = ", ".join(
        f"COUNT(CASE WHEN NOT (b.{col} <=> s.{col}) THEN 1 END) AS `{col}`" for col in tracked_columns
    )
    sql = (
        f"WITH bronze_versions AS ("
        f"SELECT {primary_key}, {tracked_column_select}, load_timestamp, "
        f"{value_fingerprint_expr} AS value_fingerprint "
        f"FROM {table_config.bronze_target}"
        f"), "
        f"bronze_first_occurrence AS ("
        f"SELECT *, MIN(load_timestamp) OVER (PARTITION BY {primary_key}, value_fingerprint) AS first_seen_timestamp "
        f"FROM bronze_versions"
        f"), "
        f"bronze_distinct_versions AS ("
        f"SELECT * FROM bronze_first_occurrence WHERE load_timestamp = first_seen_timestamp"
        f"), "
        f"bronze_latest AS ("
        f"SELECT *, ROW_NUMBER() OVER (PARTITION BY {primary_key} ORDER BY load_timestamp DESC) AS rn "
        f"FROM bronze_distinct_versions"
        f") "
        f"SELECT {mismatch_exprs} "
        f"FROM bronze_latest b "
        f"JOIN {table_config.silver_target} s ON b.{primary_key} = s.{primary_key} "
        f"WHERE b.rn = 1"
    )
    row = run_query(sql)[0]

    issues: List[str] = []
    mismatch_counts: Dict[str, int] = {}
    for col in tracked_columns:
        count = row[col]
        mismatch_counts[col] = count
        if count > 0:
            issues.append(
                f"Column {col} has {count} rows where Silver's current value does not match "
                f"Bronze's latest value, expected 0"
            )

    return ValidationResult(
        table_name=table_config.table_name,
        validator_name=VALIDATOR_NAME,
        passed=len(issues) == 0,
        issues=issues,
        details={"mismatch_counts": mismatch_counts},
    )
