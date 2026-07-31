# Databricks notebook source
# MAGIC %md
# MAGIC # Gold Aggregate
# MAGIC Builds each gold table in config/gold_mappings.yaml from Silver, branching
# MAGIC on `aggregation_type`. Gold always rebuilds fully with `overwrite` --
# MAGIC unlike Silver's incremental MERGE, there's no history to preserve here.

# COMMAND ----------

import sys
from pathlib import Path

from pyspark.sql import functions as F
from pyspark.sql.functions import col, date_format

# COMMAND ----------

try:
    _NOTEBOOK_PATH = Path(__file__).resolve()
except NameError:
    _NOTEBOOK_PATH = Path.cwd()
REPO_ROOT = _NOTEBOOK_PATH.parent.parent
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from framework.config_loader import load_all_table_configs, load_gold_mappings

# COMMAND ----------

# Aliases the config doesn't spell out (aggregate_columns is just {column, function}
# pairs, no output name) -- both gold tables that aggregate happen to use exactly
# these two combinations.
AGGREGATE_ALIAS_OVERRIDES = {
    ("claim_amount", "sum"): "total_claim_amount",
    ("claim_id", "count"): "claim_count",
}


def get_aggregate_alias(column: str, function: str) -> str:
    return AGGREGATE_ALIAS_OVERRIDES.get((column, function), f"{function}_{column}")


def build_aggregate_exprs(aggregate_columns):
    """Build agg() expressions straight from the mapping's aggregate_columns,
    so the aggregation performed always matches what the config declares."""
    exprs = []
    for agg in aggregate_columns:
        column, function = agg["column"], agg["function"]
        func = getattr(F, function)
        exprs.append(func(col(column)).alias(get_aggregate_alias(column, function)))
    return exprs


def zero_fill_sum_columns(df, aggregate_columns):
    """A left join can leave a sum() as NULL for a group with no matching rows
    (e.g. a policy with zero claims) -- that should read as 0, not null."""
    sum_aliases = [
        get_aggregate_alias(agg["column"], agg["function"]) for agg in aggregate_columns if agg["function"] == "sum"
    ]
    return df.fillna(0, subset=sum_aliases) if sum_aliases else df


def compute_totals(df, columns):
    if not columns:
        return {}
    totals_row = df.select(*[F.sum(col(c)).alias(c) for c in columns]).collect()[0]
    return {c: totals_row[c] for c in columns}


def build_silver_target_lookup():
    return {config.table_name: config.silver_target for config in load_all_table_configs()}


# COMMAND ----------

def build_customer_360(spark, mapping, silver_targets):
    """passthrough: every customers column as-is, enriched with policy_count,
    claim_count, and total_claim_amount (claims reached via each customer's policies)."""
    customers_df = spark.table(silver_targets["customers"]).filter(col("is_current") == True)  # noqa: E712
    policies_df = spark.table(silver_targets["policies"])
    claims_df = spark.table(silver_targets["claims"])

    policy_counts = policies_df.groupBy("customer_id").agg(F.count("policy_id").alias("policy_count"))

    claims_per_customer = (
        policies_df.select("policy_id", "customer_id")
        .join(claims_df.select("policy_id", "claim_id", "claim_amount"), "policy_id", "left")
        .groupBy("customer_id")
        .agg(F.count("claim_id").alias("claim_count"), F.sum("claim_amount").alias("total_claim_amount"))
    )

    result_df = (
        customers_df.join(policy_counts, "customer_id", "left")
        .join(claims_per_customer, "customer_id", "left")
        .fillna({"policy_count": 0, "claim_count": 0, "total_claim_amount": 0})
    )

    result_df.write.format("delta").mode("overwrite").saveAsTable(mapping.gold_table)

    totals_columns = ["policy_count", "claim_count", "total_claim_amount"]
    return {
        "source_counts": {
            "customers (is_current=true)": customers_df.count(),
            "policies": policies_df.count(),
            "claims": claims_df.count(),
        },
        "output_row_count": result_df.count(),
        "totals": compute_totals(result_df, totals_columns),
    }


# COMMAND ----------

def build_claims_summary_by_policy(spark, mapping, silver_targets):
    """join_aggregate: policies joined to claims, grouped by policy_id, carrying
    through policy_type/policy_status, aggregated per mapping.aggregate_columns."""
    policies_df = spark.table(silver_targets["policies"])
    claims_df = spark.table(silver_targets["claims"])

    joined_df = policies_df.join(claims_df, "policy_id", "left")

    result_df = joined_df.groupBy(*mapping.group_by_columns).agg(
        F.first(col("policy_type")).alias("policy_type"),
        F.first(col("policy_status")).alias("policy_status"),
        *build_aggregate_exprs(mapping.aggregate_columns),
    )
    result_df = zero_fill_sum_columns(result_df, mapping.aggregate_columns)

    result_df.write.format("delta").mode("overwrite").saveAsTable(mapping.gold_table)

    totals_columns = [get_aggregate_alias(a["column"], a["function"]) for a in mapping.aggregate_columns]
    return {
        "source_counts": {"policies": policies_df.count(), "claims": claims_df.count()},
        "output_row_count": result_df.count(),
        "totals": compute_totals(result_df, totals_columns),
    }


# COMMAND ----------

def build_monthly_claims_trend(spark, mapping, silver_targets):
    """group_aggregate: claims truncated to claim_month, aggregated per
    mapping.aggregate_columns."""
    claims_df = spark.table(silver_targets["claims"])
    claims_with_month = claims_df.withColumn("claim_month", date_format(col("claim_date"), "yyyy-MM"))

    result_df = (
        claims_with_month.groupBy(*mapping.group_by_columns)
        .agg(*build_aggregate_exprs(mapping.aggregate_columns))
        .orderBy(*mapping.group_by_columns)
    )
    result_df = zero_fill_sum_columns(result_df, mapping.aggregate_columns)

    result_df.write.format("delta").mode("overwrite").saveAsTable(mapping.gold_table)

    totals_columns = [get_aggregate_alias(a["column"], a["function"]) for a in mapping.aggregate_columns]
    return {
        "source_counts": {"claims": claims_df.count()},
        "output_row_count": result_df.count(),
        "totals": compute_totals(result_df, totals_columns),
    }


# COMMAND ----------

BUILDERS = {
    "passthrough": build_customer_360,
    "join_aggregate": build_claims_summary_by_policy,
    "group_aggregate": build_monthly_claims_trend,
}


def print_gold_summary(gold_table, result):
    print(f"\n=== Summary: {gold_table} ===")
    for source_name, count in result["source_counts"].items():
        print(f"  Source rows ({source_name}): {count}")
    print(f"  Output rows: {result['output_row_count']}")
    for column_name, total in result["totals"].items():
        print(f"  Total {column_name} (summed across all rows): {total}")


def main():
    gold_mappings = load_gold_mappings()
    silver_targets = build_silver_target_lookup()
    print(f"Loaded {len(gold_mappings)} gold mappings.")

    for mapping in gold_mappings:
        try:
            builder = BUILDERS.get(mapping.aggregation_type)
            if builder is None:
                raise ValueError(f"Unknown aggregation_type '{mapping.aggregation_type}'")

            print(f"\n--- Building '{mapping.gold_table}' ({mapping.aggregation_type}) ---")
            result = builder(spark, mapping, silver_targets)
            print_gold_summary(mapping.gold_table, result)
        except Exception as exc:
            print(f"\nERROR: gold aggregation failed for '{mapping.gold_table}': {exc}")
            continue

    print("\nGold aggregation complete.")


# COMMAND ----------

if __name__ == "__main__":
    main()
