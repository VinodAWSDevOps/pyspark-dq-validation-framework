import time

import allure
import pytest

from framework.db_connection import run_query

PERFORMANCE_THRESHOLD_SECONDS = 30

# Same alias convention as reconciliation_validator.py / pipeline/03_gold_aggregate.py
# (duplicated locally -- small enough that importing a "private" helper across
# a validator/test boundary isn't worth the coupling).
GOLD_AGGREGATE_ALIAS_OVERRIDES = {
    ("claim_amount", "sum"): "total_claim_amount",
    ("claim_id", "count"): "claim_count",
}


def timed_query(sql: str):
    start = time.perf_counter()
    result = run_query(sql)
    elapsed_seconds = time.perf_counter() - start
    return result, elapsed_seconds


def _gold_alias(column: str, function: str) -> str:
    return GOLD_AGGREGATE_ALIAS_OVERRIDES.get((column, function), f"{function}_{column}")


def _build_representative_aggregate_sql(gold_mapping) -> str:
    if not gold_mapping.aggregate_columns:
        return f"SELECT COUNT(*) AS row_count FROM {gold_mapping.gold_table}"

    sum_exprs = [
        f"SUM({_gold_alias(agg['column'], agg['function'])}) AS sum_{_gold_alias(agg['column'], agg['function'])}"
        for agg in gold_mapping.aggregate_columns
    ]
    return f"SELECT COUNT(*) AS row_count, {', '.join(sum_exprs)} FROM {gold_mapping.gold_table}"


@allure.feature("Performance Validation")
@allure.severity(allure.severity_level.NORMAL)
@pytest.mark.regression
def test_query_performance(table_config):
    allure.dynamic.story(table_config.table_name)

    sql = f"SELECT COUNT(*) AS row_count FROM {table_config.silver_target}"
    _, elapsed_seconds = timed_query(sql)

    print(f"[{table_config.table_name}] COUNT(*) query took {elapsed_seconds:.2f}s")

    if elapsed_seconds >= PERFORMANCE_THRESHOLD_SECONDS:
        pytest.fail(
            f"Query for '{table_config.table_name}' took {elapsed_seconds:.2f}s, "
            f"expected < {PERFORMANCE_THRESHOLD_SECONDS}s",
            pytrace=False,
        )


@allure.feature("Performance Validation")
@allure.severity(allure.severity_level.NORMAL)
@pytest.mark.regression
def test_aggregate_query_performance(gold_mapping):
    allure.dynamic.story(gold_mapping.gold_table)

    sql = _build_representative_aggregate_sql(gold_mapping)
    _, elapsed_seconds = timed_query(sql)

    print(f"[{gold_mapping.gold_table}] aggregate query took {elapsed_seconds:.2f}s")

    if elapsed_seconds >= PERFORMANCE_THRESHOLD_SECONDS:
        pytest.fail(
            f"Aggregate query for '{gold_mapping.gold_table}' took {elapsed_seconds:.2f}s, "
            f"expected < {PERFORMANCE_THRESHOLD_SECONDS}s",
            pytrace=False,
        )
