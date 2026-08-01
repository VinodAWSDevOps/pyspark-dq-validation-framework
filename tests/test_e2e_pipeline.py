import allure
import pytest

from framework.db_connection import run_query
from framework.validators.reconciliation_validator import validate_reconciliation

REJECTS_TABLE = "insurance_dq_qa.silver.rejects"


def get_bronze_count(table_config) -> int:
    return run_query(f"SELECT COUNT(*) AS row_count FROM {table_config.bronze_target}")[0]["row_count"]


def get_silver_count(table_config) -> int:
    return run_query(f"SELECT COUNT(*) AS row_count FROM {table_config.silver_target}")[0]["row_count"]


def get_rejects_count(table_name: str) -> int:
    sql = f"SELECT COUNT(*) AS row_count FROM {REJECTS_TABLE} WHERE source_table = '{table_name}'"
    return run_query(sql)[0]["row_count"]


@allure.feature("End-to-End Pipeline")
@allure.severity(allure.severity_level.CRITICAL)
@pytest.mark.e2e
def test_row_conservation(table_config):
    allure.dynamic.story(table_config.table_name)

    bronze_count = get_bronze_count(table_config)
    silver_count = get_silver_count(table_config)
    rejects_count = get_rejects_count(table_config.table_name)

    if table_config.scd_type == "SCD1":
        # SCD1 collapses every Bronze version of a primary key down to one
        # current Silver row (no history kept), so multiple Bronze rows can
        # legitimately map to a single Silver row -- exact conservation
        # doesn't apply. Collapsing can only ever reduce the row count, never
        # increase it, so bronze must still be >= silver+rejects; anything
        # less means rows went missing beyond what collapsing + rejection
        # accounts for.
        conserved = bronze_count >= silver_count + rejects_count
    else:
        conserved = bronze_count == silver_count + rejects_count

    if not conserved:
        pytest.fail(
            f"Row conservation failed for '{table_config.table_name}' (scd_type={table_config.scd_type}): "
            f"bronze={bronze_count}, silver={silver_count}, rejects={rejects_count}",
            pytrace=False,
        )


@allure.feature("End-to-End Pipeline")
@allure.severity(allure.severity_level.CRITICAL)
@pytest.mark.e2e
def test_gold_reconciliation(all_gold_mappings):
    failures = []
    for gold_mapping in all_gold_mappings:
        result = validate_reconciliation(gold_mapping)
        if not result.passed:
            failures.append(f"{gold_mapping.gold_table}: " + "; ".join(result.issues))

    if failures:
        pytest.fail("\n".join(failures), pytrace=False)
