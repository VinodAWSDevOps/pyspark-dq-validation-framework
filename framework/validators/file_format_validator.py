"""Landing-zone file format / schema-drift validator. Pure databricks-sql-connector
-- no pyspark imports, no local file reading. Reads the CSVs directly via
Databricks SQL's read_files() table function, not through Bronze.
"""
from __future__ import annotations

from typing import List

from framework.config_loader import TableConfig
from framework.db_connection import get_query_columns
from framework.validators.base import ValidationResult

VALIDATOR_NAME = "file_format_validator"

VOLUME_ROOT = "/Volumes/insurance_dq_qa/bronze/landing_zone"

# batch_id/load_timestamp: data_generation/*.py deliberately adds these to
# every landing CSV; not in expected_file_format.expected_headers (business
# columns only), but always expected, not drift. Same exception as
# 01_bronze_ingest.py's ALWAYS_EXPECTED_EXTRA_COLUMNS.
# _rescued_data: added automatically by read_files() itself (inferred/non-
# explicit schema) -- a read_files artifact, not landing CSV content.
ALWAYS_EXPECTED_EXTRA_COLUMNS = {"batch_id", "load_timestamp", "_rescued_data"}


def validate_file_format(table_config: TableConfig) -> ValidationResult:
    volume_path = f"{VOLUME_ROOT}/{table_config.volume_folder}/"

    # LIMIT 0 -- we only need the result schema (read_files merges headers
    # across every file in the folder, surfacing the union), not the data.
    # delimiter/encoding aren't independently checkable this way; they're
    # assumed consistent since Bronze ingestion already succeeded reading
    # these same files with those settings.
    sql = f"SELECT * FROM read_files('{volume_path}*.csv', format => 'csv', header => true) LIMIT 0"
    actual_columns = get_query_columns(sql)

    expected_headers = table_config.expected_file_format.get("expected_headers", [])

    expected_set = set(expected_headers)
    actual_set = set(actual_columns)

    issues: List[str] = []
    for col_name in sorted(expected_set - actual_set):
        issues.append(f"Missing expected column in landing files: {col_name}")
    for col_name in sorted(actual_set - expected_set - ALWAYS_EXPECTED_EXTRA_COLUMNS):
        issues.append(f"Unexpected column found in landing files: {col_name}")

    return ValidationResult(
        table_name=table_config.table_name,
        validator_name=VALIDATOR_NAME,
        passed=len(issues) == 0,
        issues=issues,
        details={"expected_columns": expected_headers, "actual_columns": actual_columns},
    )
