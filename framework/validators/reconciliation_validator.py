"""Gold-vs-Silver reconciliation validator. Pure databricks-sql-connector --
no pyspark imports. Takes a GoldMapping (from load_gold_mappings()), not a
TableConfig, since it validates a Gold table against its Silver source(s).
"""
from __future__ import annotations

from typing import Any, Dict, List, Tuple

from framework.config_loader import GoldMapping, get_table_config
from framework.db_connection import run_query
from framework.validators.base import ValidationResult

VALIDATOR_NAME = "reconciliation_validator"

# Gold tables store the aggregated alias (e.g. total_claim_amount), not the raw
# source column -- same override table as pipeline/03_gold_aggregate.py, since
# aggregate_columns in the YAML doesn't carry an output name.
GOLD_AGGREGATE_ALIAS_OVERRIDES = {
    ("claim_amount", "sum"): "total_claim_amount",
    ("claim_id", "count"): "claim_count",
}

SQL_FUNCTION_BY_NAME = {"sum": "SUM", "count": "COUNT"}


def _gold_alias(column: str, function: str) -> str:
    return GOLD_AGGREGATE_ALIAS_OVERRIDES.get((column, function), f"{function}_{column}")


def _table_name_portion(fully_qualified_table_name: str) -> str:
    return fully_qualified_table_name.rsplit(".", 1)[-1]


def _resolve_source_table_for_column(gold_mapping: GoldMapping, column: str) -> str:
    """Which of this mapping's source_tables actually has `column`, per that
    table's config -- so claims_summary_by_policy's two sources resolve
    claim_amount/claim_id to claims specifically, not policies."""
    for source_table in gold_mapping.source_tables:
        source_config = get_table_config(_table_name_portion(source_table))
        if column in source_config.expected_file_format.get("expected_headers", []):
            return source_table

    if len(gold_mapping.source_tables) == 1:
        return gold_mapping.source_tables[0]

    raise ValueError(f"Could not resolve a source table containing column '{column}' among {gold_mapping.source_tables}")


def _validate_passthrough(gold_mapping: GoldMapping) -> Tuple[List[str], Dict[str, Any]]:
    source_table = gold_mapping.source_tables[0]
    source_config = get_table_config(_table_name_portion(source_table))

    where_clause = f" WHERE {source_config.is_current_col} = true" if source_config.scd_type == "SCD2" else ""

    gold_count = run_query(f"SELECT COUNT(*) AS row_count FROM {gold_mapping.gold_table}")[0]["row_count"]
    silver_count = run_query(f"SELECT COUNT(*) AS row_count FROM {source_table}{where_clause}")[0]["row_count"]

    issues: List[str] = []
    if gold_count != silver_count:
        issues.append(f"Row count mismatch: gold has {gold_count}, silver source has {silver_count}")

    return issues, {"gold_count": gold_count, "silver_count": silver_count}


def _validate_aggregate(gold_mapping: GoldMapping) -> Tuple[List[str], Dict[str, Any]]:
    issues: List[str] = []
    details: Dict[str, Any] = {}

    for agg in gold_mapping.aggregate_columns:
        column, function = agg["column"], agg["function"]
        func_sql = SQL_FUNCTION_BY_NAME.get(function)
        if func_sql is None:
            raise ValueError(f"Unsupported aggregate function '{function}' for reconciliation")

        gold_column = _gold_alias(column, function)
        source_table = _resolve_source_table_for_column(gold_mapping, column)

        # Gold is already pre-aggregated per group -- SUM its column again to
        # collapse across groups into one grand total, comparable to the
        # ungrouped aggregate over the whole silver source table.
        gold_total = run_query(f"SELECT SUM({gold_column}) AS grand_total FROM {gold_mapping.gold_table}")[0][
            "grand_total"
        ] or 0
        silver_total = run_query(f"SELECT {func_sql}({column}) AS grand_total FROM {source_table}")[0][
            "grand_total"
        ] or 0

        details[f"{column}.{function}"] = {"gold_total": gold_total, "silver_total": silver_total}

        if abs(float(gold_total) - float(silver_total)) >= 0.01:
            issues.append(
                f"Aggregate mismatch on {column} ({function}): gold total = {gold_total}, "
                f"silver total = {silver_total}"
            )

    return issues, details


def validate_reconciliation(gold_mapping: GoldMapping) -> ValidationResult:
    if gold_mapping.aggregation_type == "passthrough":
        issues, details = _validate_passthrough(gold_mapping)
    elif gold_mapping.aggregation_type in ("join_aggregate", "group_aggregate"):
        issues, details = _validate_aggregate(gold_mapping)
    else:
        raise ValueError(f"Unknown aggregation_type '{gold_mapping.aggregation_type}' for reconciliation")

    return ValidationResult(
        table_name=gold_mapping.gold_table,
        validator_name=VALIDATOR_NAME,
        passed=len(issues) == 0,
        issues=issues,
        details=details,
    )
