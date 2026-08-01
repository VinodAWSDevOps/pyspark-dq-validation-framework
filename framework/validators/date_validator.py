"""Cross-table date consistency validator. Pure databricks-sql-connector -- no
pyspark imports.

Currently hardcodes the one meaningful relationship we have: a claim's
claim_date shouldn't precede its policy's policy_start_date. Our config
schema doesn't capture "which date column relates to which parent date
column" generically yet -- if more cross-table date relationships come up
later, add a date_relationship field to config and make this generic
instead of hardcoded per table.
"""
from __future__ import annotations

from typing import Dict, List

from framework.config_loader import TableConfig, get_table_config
from framework.db_connection import run_query
from framework.validators.base import ValidationResult

VALIDATOR_NAME = "date_validator"


def validate_date_consistency(table_config: TableConfig) -> ValidationResult:
    if not table_config.foreign_keys:
        return ValidationResult(
            table_name=table_config.table_name,
            validator_name=VALIDATOR_NAME,
            passed=True,
            issues=[],
            details={},
        )

    issues: List[str] = []
    details: Dict[str, int] = {}

    # Hardcoded to the one cross-table date relationship we currently care about.
    if table_config.table_name == "claims" and "policy_id" in table_config.foreign_keys:
        parent_table_name = table_config.foreign_keys["policy_id"].split(".", 1)[0]
        parent_silver_target = get_table_config(parent_table_name).silver_target

        sql = (
            f"SELECT COUNT(*) AS violation_count FROM {table_config.silver_target} c "
            f"JOIN {parent_silver_target} p ON c.policy_id = p.policy_id "
            f"WHERE c.claim_date < p.policy_start_date"
        )
        violation_count = run_query(sql)[0]["violation_count"]
        details["claim_date_before_policy_start_date_count"] = violation_count
        if violation_count > 0:
            issues.append(
                f"Column claim_date has {violation_count} rows dated before the related policy's "
                f"policy_start_date, expected 0"
            )

    return ValidationResult(
        table_name=table_config.table_name,
        validator_name=VALIDATOR_NAME,
        passed=len(issues) == 0,
        issues=issues,
        details=details,
    )
