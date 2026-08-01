"""Business rule validator. Pure databricks-sql-connector -- no pyspark imports."""
from __future__ import annotations

from typing import Any, Dict, List, Tuple

from framework.config_loader import TableConfig
from framework.db_connection import run_query
from framework.validators.base import ValidationResult

VALIDATOR_NAME = "business_rule_validator"

# (column, rule_type, description, sql_condition)
RuleSpec = Tuple[str, str, str, str]


def _build_rule_specs(business_rules: Dict[str, Any]) -> List[RuleSpec]:
    specs: List[RuleSpec] = []
    for column, rules in (business_rules or {}).items():
        if not isinstance(rules, dict):
            continue

        if "min" in rules:
            min_value = rules["min"]
            specs.append((column, "min", f"min {min_value}", f"{column} < {min_value}"))

        if "max" in rules:
            max_value = rules["max"]
            specs.append((column, "max", f"max {max_value}", f"{column} > {max_value}"))

        if rules.get("max_date") == "today":
            specs.append((column, "max_date", "max_date today", f"{column} > CURRENT_DATE()"))

        if "after" in rules:
            other_column = rules["after"]
            specs.append((column, "after", f"after {other_column}", f"{column} < {other_column}"))

        if "enum" in rules:
            allowed_values = rules["enum"]
            quoted_list = ", ".join(f"'{value}'" for value in allowed_values)
            display_list = ", ".join(str(value) for value in allowed_values)
            specs.append((column, "enum", f"enum [{display_list}]", f"{column} NOT IN ({quoted_list})"))

    return specs


def validate_business_rules(table_config: TableConfig) -> ValidationResult:
    rule_specs = _build_rule_specs(table_config.business_rules)

    if not rule_specs:
        total_row_count = run_query(f"SELECT COUNT(*) AS total_row_count FROM {table_config.silver_target}")[0][
            "total_row_count"
        ]
        return ValidationResult(
            table_name=table_config.table_name,
            validator_name=VALIDATOR_NAME,
            passed=True,
            issues=[],
            details={"total_row_count": total_row_count, "violation_counts": {}},
        )

    # One query, one COUNT(CASE WHEN <condition> THEN 1 END) per rule. Positional
    # aliases (not column names) since a column can have more than one rule.
    aliases = [f"rule_{i}" for i in range(len(rule_specs))]
    count_exprs = ", ".join(
        f"COUNT(CASE WHEN {condition} THEN 1 END) AS `{alias}`"
        for alias, (_column, _rule_type, _description, condition) in zip(aliases, rule_specs)
    )
    sql = f"SELECT COUNT(*) AS total_row_count, {count_exprs} FROM {table_config.silver_target}"
    row = run_query(sql)[0]

    total_row_count = row["total_row_count"]

    issues: List[str] = []
    violation_counts: Dict[str, int] = {}
    for alias, (column, rule_type, description, _condition) in zip(aliases, rule_specs):
        count = row[alias]
        violation_counts[f"{column}.{rule_type}"] = count
        if count > 0:
            issues.append(f"Column {column} has {count} rows violating {description}")

    return ValidationResult(
        table_name=table_config.table_name,
        validator_name=VALIDATOR_NAME,
        passed=len(issues) == 0,
        issues=issues,
        details={"total_row_count": total_row_count, "violation_counts": violation_counts},
    )
