"""Great Expectations validator using GX's modern Fluent API (ExpectationSuite
/ ValidationDefinition / Batch Definitions against a Databricks SQL
datasource) -- NOT the legacy SqlAlchemyDataset/ge.dataset API, which does
not exist in the installed great_expectations version (confirmed:
`great_expectations.dataset` raises ImportError on this install). Every API
call used here was verified live against the real workspace while writing
this file, not assumed from documentation.
"""
from __future__ import annotations

import os
from datetime import date
from typing import Any, Dict, List, Tuple

import great_expectations as gx
from dotenv import load_dotenv
from great_expectations.expectations import (
    ExpectColumnPairValuesAToBeGreaterThanB,
    ExpectColumnValuesToBeBetween,
    ExpectColumnValuesToBeInSet,
    ExpectColumnValuesToBeUnique,
    ExpectColumnValuesToNotBeNull,
    ExpectTableColumnsToMatchSet,
)

from framework.config_loader import TableConfig
from framework.db_connection import ENV_PATH, REQUIRED_ENV_VARS
from framework.validators.base import ValidationResult

VALIDATOR_NAME = "ge_runner"

PIPELINE_METADATA_COLUMNS = ["batch_id", "load_timestamp", "_bronze_ingest_timestamp", "_source_file"]

print(
    f"[ge_runner] great_expectations version: {gx.__version__} -- using the modern GX Fluent API "
    f"(ExpectationSuite / ValidationDefinition / Batch Definitions). The legacy SqlAlchemyDataset API "
    f"(great_expectations.dataset) does not exist in this version, so that API is not an option here."
)


def _build_connection_string() -> str:
    load_dotenv(dotenv_path=ENV_PATH)
    missing = [var for var in REQUIRED_ENV_VARS if not os.environ.get(var)]
    if missing:
        raise RuntimeError(f"Missing required environment variable(s) in .env: {', '.join(missing)}")

    hostname = os.environ["DATABRICKS_SERVER_HOSTNAME"]
    http_path = os.environ["DATABRICKS_HTTP_PATH"]
    token = os.environ["DATABRICKS_ACCESS_TOKEN"]
    return (
        f"databricks://token:{token}@{hostname}:443/default"
        f"?http_path={http_path}&catalog=insurance_dq_qa&schema=silver"
    )


def _build_expected_columns(table_config: TableConfig) -> List[str]:
    columns = list(table_config.expected_file_format.get("expected_headers", []))
    columns += PIPELINE_METADATA_COLUMNS
    if table_config.scd_type == "SCD2":
        columns += [table_config.effective_start_col, table_config.effective_end_col, table_config.is_current_col]
    return columns


def _numeric_min_business_rules(business_rules: Dict[str, Any]) -> List[Tuple[str, Any]]:
    return [
        (column, rules["min"])
        for column, rules in (business_rules or {}).items()
        if isinstance(rules, dict) and "min" in rules
    ]


def _enum_business_rules(business_rules: Dict[str, Any]) -> List[Tuple[str, List[Any]]]:
    return [
        (column, list(rules["enum"]))
        for column, rules in (business_rules or {}).items()
        if isinstance(rules, dict) and "enum" in rules
    ]


def _max_date_today_business_rules(business_rules: Dict[str, Any]) -> List[str]:
    return [
        column
        for column, rules in (business_rules or {}).items()
        if isinstance(rules, dict) and rules.get("max_date") == "today"
    ]


def _after_business_rules(business_rules: Dict[str, Any]) -> List[Tuple[str, str]]:
    return [
        (column, rules["after"])
        for column, rules in (business_rules or {}).items()
        if isinstance(rules, dict) and "after" in rules
    ]


def _build_expectations(table_config: TableConfig) -> List:
    expectations = [ExpectTableColumnsToMatchSet(column_set=_build_expected_columns(table_config), exact_match=True)]

    for column in table_config.mandatory_columns or []:
        expectations.append(ExpectColumnValuesToNotBeNull(column=column))

    expectations.append(ExpectColumnValuesToBeUnique(column=table_config.primary_key))

    for column, min_value in _numeric_min_business_rules(table_config.business_rules):
        expectations.append(ExpectColumnValuesToBeBetween(column=column, min_value=min_value, max_value=None))

    for column, allowed_values in _enum_business_rules(table_config.business_rules):
        expectations.append(ExpectColumnValuesToBeInSet(column=column, value_set=allowed_values))

    for column in _max_date_today_business_rules(table_config.business_rules):
        expectations.append(ExpectColumnValuesToBeBetween(column=column, min_value=None, max_value=date.today()))

    for column, other_column in _after_business_rules(table_config.business_rules):
        expectations.append(
            ExpectColumnPairValuesAToBeGreaterThanB(column_A=column, column_B=other_column, or_equal=False)
        )

    return expectations


def _build_batch_definition(datasource, table_config: TableConfig):
    """Whole table normally; for SCD2, filtered to is_current=true up front
    so every expectation in the suite runs against Silver's current state --
    same is_current-filter-before-anything-else principle as
    uniqueness_validator.py/reconciliation_validator.py, just applied to the
    whole batch rather than per-check, since GX validates one batch per
    suite run and mixing filtered/unfiltered checks in one suite isn't a
    clean option here."""
    if table_config.scd_type == "SCD2":
        query = f"SELECT * FROM {table_config.silver_target} WHERE {table_config.is_current_col} = true"
        asset = datasource.add_query_asset(name=f"{table_config.table_name}_asset", query=query)
    else:
        bare_table_name = table_config.silver_target.rsplit(".", 1)[-1]
        asset = datasource.add_table_asset(name=f"{table_config.table_name}_asset", table_name=bare_table_name)

    return asset.add_batch_definition_whole_table(name=f"{table_config.table_name}_batch")


def _describe_failure(result_dict: Dict[str, Any]) -> str:
    config = result_dict["expectation_config"]
    result = result_dict["result"]
    expectation_type = config["type"]
    kwargs = config["kwargs"]

    if "observed_value" in result:
        return f"{expectation_type} failed: observed columns {result['observed_value']}"

    unexpected_count = result.get("unexpected_count")
    element_count = result.get("element_count")
    sample = result.get("partial_unexpected_list", [])

    if expectation_type == "expect_column_pair_values_a_to_be_greater_than_b":
        # partial_unexpected_list is a list of [value_A, value_B] pairs here,
        # not single scalar values -- pull both column names in so the
        # sample is legible instead of a bare list of two-element lists.
        column_a, column_b = kwargs["column_A"], kwargs["column_B"]
        pair_sample = [f"{column_a}={a}, {column_b}={b}" for a, b in sample[:5]]
        return (
            f"{expectation_type} ({column_a} > {column_b}) failed: {unexpected_count} of {element_count} rows "
            f"violated it (sample pairs: {pair_sample})"
        )

    column = kwargs.get("column")
    return (
        f"{expectation_type} on column '{column}' failed: {unexpected_count} of {element_count} rows "
        f"violated it (sample values: {sample[:5]})"
    )


def run_ge_validation(table_config: TableConfig) -> ValidationResult:
    connection_string = _build_connection_string()

    context = gx.get_context(mode="ephemeral")
    datasource = context.data_sources.add_databricks_sql(
        name=f"{table_config.table_name}_datasource", connection_string=connection_string
    )
    batch_definition = _build_batch_definition(datasource, table_config)

    suite = context.suites.add(
        gx.ExpectationSuite(name=f"{table_config.table_name}_suite", expectations=_build_expectations(table_config))
    )
    validation_definition = context.validation_definitions.add(
        gx.ValidationDefinition(name=f"{table_config.table_name}_validation", data=batch_definition, suite=suite)
    )

    result = validation_definition.run()
    result_dicts = [r.to_json_dict() for r in result.results]

    issues = [_describe_failure(r) for r in result_dicts if not r["success"]]

    return ValidationResult(
        table_name=table_config.table_name,
        validator_name=VALIDATOR_NAME,
        passed=bool(result.success),
        issues=issues,
        details={"statistics": result.statistics, "expectation_results": result_dicts},
    )
