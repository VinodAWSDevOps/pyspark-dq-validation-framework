# Databricks notebook source
# MAGIC %md
# MAGIC # Deequ Checks
# MAGIC Runs an Amazon Deequ `VerificationSuite` (via `pydeequ`) against each Silver
# MAGIC table, plus a `ConstraintSuggestionRunner` profiling pass to surface
# MAGIC constraints we might have missed. Results are written to
# MAGIC `insurance_dq_qa.silver.deequ_results` (overwritten each run -- this table
# MAGIC reflects the latest run's findings, not cumulative history like rejects).
# MAGIC
# MAGIC **Prerequisite:** the Deequ JAR (`com.amazon.deequ:deequ:<version>` matching
# MAGIC the cluster's Spark/Scala version) must be installed as a cluster library.
# MAGIC `pydeequ` is only the Python API wrapper -- it doesn't provision the JAR
# MAGIC itself, and `spark.conf` can't be changed after the session (already
# MAGIC created by Databricks) has started.

# COMMAND ----------

import os
import sys
from pathlib import Path

try:
    _NOTEBOOK_PATH = Path(__file__).resolve()
except NameError:
    _NOTEBOOK_PATH = Path.cwd()
REPO_ROOT = _NOTEBOOK_PATH.parent.parent
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

# pydeequ reads SPARK_VERSION at import time to pick the right Deequ API
# bindings -- must be set before importing pydeequ's modules.
os.environ["SPARK_VERSION"] = spark.version

from framework.config_loader import load_all_table_configs

# COMMAND ----------

from pydeequ.checks import Check, CheckLevel
from pydeequ.suggestions import DEFAULT, ConstraintSuggestionRunner
from pydeequ.verification import VerificationResult, VerificationSuite
from pyspark.sql.functions import col, current_timestamp, lit

# COMMAND ----------

DEEQU_RESULTS_TABLE = "insurance_dq_qa.silver.deequ_results"


def read_silver_df(table_config):
    df = spark.table(table_config.silver_target)
    if table_config.scd_type == "SCD2":
        df = df.filter(col(table_config.is_current_col) == True)  # noqa: E712
    return df


def build_check(table_config):
    check = Check(spark, CheckLevel.Error, f"{table_config.table_name} checks")

    for column in table_config.mandatory_columns or []:
        check = check.hasCompleteness(column, lambda x: x == 1.0)

    check = check.isUnique(table_config.primary_key)

    for column, rules in (table_config.business_rules or {}).items():
        if not (isinstance(rules, dict) and "min" in rules):
            continue
        min_value = rules["min"]
        if min_value == 0:
            check = check.isNonNegative(column)
        else:
            check = check.satisfies(f"{column} >= {min_value}", f"{column}_gte_{min_value}", lambda x: x == 1.0)

    check = check.hasSize(lambda x: x > 0)
    return check


def run_verification(df, check):
    """Raw Deequ result columns: check, check_level, check_status, constraint,
    constraint_status, constraint_message."""
    verification_result = VerificationSuite(spark).onData(df).addCheck(check).run()
    return VerificationResult.checkResultsAsDataFrame(spark, verification_result)


def run_suggestions(df):
    suggestion_result = ConstraintSuggestionRunner(spark).onData(df).addConstraintRule(DEFAULT).run()
    return suggestion_result.get("constraint_suggestions", [])


# COMMAND ----------

def process_table(table_config):
    df = read_silver_df(table_config)
    check = build_check(table_config)

    raw_result_df = run_verification(df, check)
    suggestions = run_suggestions(df)

    results_df = raw_result_df.select(
        lit(table_config.table_name).alias("table_name"),
        current_timestamp().alias("check_timestamp"),
        col("constraint"),
        col("constraint_status").alias("status"),
        col("constraint_message"),
    )

    status_counts = {row["constraint_status"]: row["count"] for row in raw_result_df.groupBy("constraint_status").count().collect()}

    return {
        "table_name": table_config.table_name,
        "results_df": results_df,
        "passed": status_counts.get("Success", 0),
        "failed": status_counts.get("Failure", 0),
        "suggestions": suggestions,
    }


def print_table_summary(result):
    print(f"\n=== Summary: {result['table_name']} ===")
    print(f"  Checks passed: {result['passed']}")
    print(f"  Checks failed: {result['failed']}")

    suggestions = result["suggestions"]
    print(f"  Top {min(3, len(suggestions))} constraint suggestions (of {len(suggestions)} found):")
    if not suggestions:
        print("    (none)")
    for suggestion in suggestions[:3]:
        print(f"    - {suggestion.get('description', suggestion)}")


# COMMAND ----------

def main():
    table_configs = load_all_table_configs()
    print(f"Loaded {len(table_configs)} table configs (sequence order).")

    all_results_frames = []

    for table_config in table_configs:
        try:
            print(f"\n--- Running Deequ checks for '{table_config.table_name}' ---")
            result = process_table(table_config)
            all_results_frames.append(result["results_df"])
            print_table_summary(result)
        except Exception as exc:
            print(f"\nERROR: Deequ checks failed for table '{table_config.table_name}': {exc}")
            continue

    if all_results_frames:
        combined_results_df = all_results_frames[0]
        for frame in all_results_frames[1:]:
            combined_results_df = combined_results_df.unionByName(frame)
        combined_results_df.write.format("delta").mode("overwrite").saveAsTable(DEEQU_RESULTS_TABLE)
        print(f"\nWrote {combined_results_df.count()} constraint results to {DEEQU_RESULTS_TABLE}")
    else:
        print("\nNo tables processed successfully; nothing written to deequ_results.")

    print("\nDeequ checks complete.")


# COMMAND ----------

if __name__ == "__main__":
    main()
