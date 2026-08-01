"""Referential integrity validator. Pure databricks-sql-connector -- no pyspark imports."""
from __future__ import annotations

from typing import Dict, List, Tuple

from framework.config_loader import TableConfig, get_table_config
from framework.db_connection import run_query
from framework.validators.base import ValidationResult

VALIDATOR_NAME = "referential_integrity_validator"

# (local_column, parent_table_name, parent_column_name)
ForeignKeySpec = Tuple[str, str, str]


def _parse_foreign_keys(foreign_keys: Dict[str, str]) -> List[ForeignKeySpec]:
    specs: List[ForeignKeySpec] = []
    for local_column, reference in (foreign_keys or {}).items():
        parent_table_name, parent_column_name = reference.split(".", 1)
        specs.append((local_column, parent_table_name, parent_column_name))
    return specs


def validate_referential_integrity(table_config: TableConfig) -> ValidationResult:
    fk_specs = _parse_foreign_keys(table_config.foreign_keys)

    if not fk_specs:
        total_row_count = run_query(f"SELECT COUNT(*) AS total_row_count FROM {table_config.silver_target}")[0][
            "total_row_count"
        ]
        return ValidationResult(
            table_name=table_config.table_name,
            validator_name=VALIDATOR_NAME,
            passed=True,
            issues=[],
            details={"total_row_count": total_row_count, "orphan_counts": {}},
        )

    # One query, one COUNT(CASE WHEN <not-in-parent> THEN 1 END) per foreign key.
    aliases = [f"fk_{i}" for i in range(len(fk_specs))]
    count_exprs = []
    for alias, (local_column, parent_table_name, parent_column_name) in zip(aliases, fk_specs):
        parent_silver_target = get_table_config(parent_table_name).silver_target
        condition = (
            f"c.{local_column} IS NOT NULL AND c.{local_column} NOT IN "
            f"(SELECT {parent_column_name} FROM {parent_silver_target})"
        )
        count_exprs.append(f"COUNT(CASE WHEN {condition} THEN 1 END) AS `{alias}`")

    sql = f"SELECT COUNT(*) AS total_row_count, {', '.join(count_exprs)} FROM {table_config.silver_target} c"
    row = run_query(sql)[0]

    total_row_count = row["total_row_count"]

    issues: List[str] = []
    orphan_counts: Dict[str, int] = {}
    for alias, (local_column, parent_table_name, parent_column_name) in zip(aliases, fk_specs):
        count = row[alias]
        orphan_counts[local_column] = count
        if count > 0:
            issues.append(
                f"Column {local_column} has {count} rows referencing non-existent "
                f"{parent_table_name}.{parent_column_name}, expected 0"
            )

    return ValidationResult(
        table_name=table_config.table_name,
        validator_name=VALIDATOR_NAME,
        passed=len(issues) == 0,
        issues=issues,
        details={"total_row_count": total_row_count, "orphan_counts": orphan_counts},
    )
