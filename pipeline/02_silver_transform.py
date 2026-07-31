# Databricks notebook source
# MAGIC %md
# MAGIC # Silver Transform
# MAGIC Casts Bronze columns to real types, enforces referential integrity and
# MAGIC primary-key uniqueness, applies SCD1/SCD2/append semantics per table
# MAGIC config, and quarantines anything that fails structural checks to
# MAGIC `insurance_dq_qa.silver.rejects`.
# MAGIC
# MAGIC **Important design note:** Bronze holds the *full* historical union of
# MAGIC every landing file (01_bronze_ingest overwrites Bronze from scratch each
# MAGIC run), not one incremental micro-batch. That means a single Silver run can
# MAGIC see multiple historical versions of the same primary key at once (e.g. a
# MAGIC customer's batch_001 row and its batch_002 address change, together). For
# MAGIC SCD2 tables this script replays each distinct `batch_id` in
# MAGIC chronological order (by earliest `load_timestamp`) against Silver, so
# MAGIC history is versioned correctly instead of collapsed. SCD1 tables have no
# MAGIC history to preserve, so it's correct (and simpler) to just collapse to
# MAGIC the latest row per primary key before merging.
# MAGIC
# MAGIC Value-level business rules (negative amounts, invalid enums, future
# MAGIC dates, min/max) are deliberately **not** enforced here -- that's
# MAGIC `framework/validators/business_rule_validator.py`'s job at test time.
# MAGIC This script only guards structural integrity: parseable dates,
# MAGIC referential integrity, and primary-key uniqueness.

# COMMAND ----------

import sys
from functools import reduce
from pathlib import Path

from delta.tables import DeltaTable
from pyspark.sql import Window
from pyspark.sql.functions import (
    coalesce,
    col,
    concat,
    concat_ws,
    current_timestamp,
    lit,
    monotonically_increasing_id,
    row_number,
    sha2,
    struct,
    to_date,
    to_json,
    trim,
    when,
)
from pyspark.sql.types import DecimalType

# COMMAND ----------

try:
    _NOTEBOOK_PATH = Path(__file__).resolve()
except NameError:
    _NOTEBOOK_PATH = Path.cwd()
REPO_ROOT = _NOTEBOOK_PATH.parent.parent
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from framework.config_loader import load_all_table_configs

# COMMAND ----------

REJECTS_TABLE = "insurance_dq_qa.silver.rejects"
SCD2_END_DATE = "9999-12-31"


def get_numeric_business_rule_columns(business_rules):
    """Columns with a numeric min/max business rule get cast to Decimal; the
    rules themselves aren't enforced here, just used to pick cast targets."""
    numeric_keys = {"min", "max"}
    return [
        col_name
        for col_name, rules in (business_rules or {}).items()
        if isinstance(rules, dict) and numeric_keys & set(rules.keys())
    ]


def finalize_rejects(df, table_name, reject_reason, detail_col):
    """Project any failed-rows dataframe down to the standard rejects schema."""
    return df.select(
        lit(table_name).alias("source_table"),
        lit(reject_reason).alias("reject_reason"),
        col(detail_col).cast("string").alias("reject_detail"),
        col("_original_row_json").alias("original_row_data"),
        current_timestamp().alias("rejected_at"),
    )


# COMMAND ----------

def cast_columns(df, table_config):
    """Cast numeric business-rule columns to Decimal and date_columns to Date.
    Rows with an unparseable (non-empty) date value are quarantined; empty/
    null dates are left null and are a completeness_validator concern instead.
    """
    for numeric_col in get_numeric_business_rule_columns(table_config.business_rules):
        if numeric_col in df.columns:
            df = df.withColumn(numeric_col, col(numeric_col).cast(DecimalType(18, 2)))

    date_columns = table_config.date_columns or []
    for date_col in date_columns:
        df = df.withColumn(f"__cast_{date_col}", to_date(col(date_col)))
        df = df.withColumn(
            f"__fail_{date_col}",
            col(date_col).isNotNull() & (trim(col(date_col)) != "") & col(f"__cast_{date_col}").isNull(),
        )

    if date_columns:
        any_failed = reduce(lambda a, b: a | b, [col(f"__fail_{c}") for c in date_columns])
        detail_expr = concat_ws(
            "; ",
            *[when(col(f"__fail_{c}"), concat(lit(f"{c}="), coalesce(col(c), lit("NULL")))) for c in date_columns],
        )
    else:
        any_failed = lit(False)
        detail_expr = lit(None).cast("string")

    df = df.withColumn("__date_cast_failed", any_failed).withColumn("__date_cast_detail", detail_expr)

    rejects_df = finalize_rejects(
        df.filter(col("__date_cast_failed")), table_config.table_name, "invalid_date_format", "__date_cast_detail"
    )

    passed_df = df.filter(~col("__date_cast_failed"))
    for date_col in date_columns:
        passed_df = passed_df.withColumn(date_col, col(f"__cast_{date_col}"))

    drop_cols = [f"__cast_{c}" for c in date_columns] + [f"__fail_{c}" for c in date_columns]
    drop_cols += ["__date_cast_failed", "__date_cast_detail"]
    passed_df = passed_df.drop(*[c for c in drop_cols if c in passed_df.columns])

    return passed_df, rejects_df


# COMMAND ----------

def check_referential_integrity(df, table_config, silver_tables_cache):
    """Each fk value must exist in the parent's already-processed Silver table.
    Safe because tables are processed in sequence order (parents first)."""
    reject_frames = []
    passed_df = df

    for fk_col, fk_reference in (table_config.foreign_keys or {}).items():
        parent_table, parent_pk_col = fk_reference.split(".", 1)
        parent_df = silver_tables_cache.get(parent_table)
        if parent_df is None:
            raise RuntimeError(
                f"parent table '{parent_table}' not available in Silver for FK check on column "
                f"'{fk_col}' (it may have failed earlier in this run)"
            )

        valid_keys_df = parent_df.select(col(parent_pk_col).alias("__valid_fk_key")).distinct()
        joined = passed_df.join(valid_keys_df, passed_df[fk_col] == valid_keys_df["__valid_fk_key"], "left")

        is_orphan = col("__valid_fk_key").isNull()
        detail_expr = concat(
            lit(f"foreign key '{fk_col}'='"),
            coalesce(passed_df[fk_col], lit("NULL")),
            lit(f"' not found in {parent_table}.{parent_pk_col}"),
        )
        reject_batch = joined.filter(is_orphan).withColumn("__fk_reject_detail", detail_expr)
        reject_frames.append(
            finalize_rejects(reject_batch, table_config.table_name, "orphan_foreign_key", "__fk_reject_detail")
        )

        passed_df = joined.filter(~is_orphan).drop("__valid_fk_key")

    return passed_df, reject_frames


# COMMAND ----------

def deduplicate_primary_key(df, table_config, existing_silver_df):
    """Reject exact full-row duplicates (same pk, identical business columns):
    - always, if they repeat within the incoming batch
    - additionally against existing Silver, for scd_type=none tables only,
      since "none" has no update semantics -- any repeat of an existing pk is
      necessarily a duplicate rather than a legitimate SCD1/SCD2 change.
    """
    pk = table_config.primary_key
    business_cols = table_config.expected_file_format.get("expected_headers", [])

    hash_expr = sha2(
        concat_ws("||", *[coalesce(col(c).cast("string"), lit("<<NULL>>")) for c in business_cols]), 256
    )
    df = df.withColumn("__dedup_hash", hash_expr)

    window_spec = Window.partitionBy(pk, "__dedup_hash").orderBy(monotonically_increasing_id())
    df = df.withColumn("__dedup_rn", row_number().over(window_spec))
    within_batch_dupe = col("__dedup_rn") > 1

    if table_config.scd_type == "none" and existing_silver_df is not None:
        existing_keys_df = existing_silver_df.select(col(pk).alias("__existing_pk")).distinct()
        df = df.join(existing_keys_df, df[pk] == col("__existing_pk"), "left")
        already_in_silver = col("__existing_pk").isNotNull()
    else:
        df = df.withColumn("__existing_pk", lit(None).cast("string"))
        already_in_silver = lit(False)

    is_dupe = within_batch_dupe | already_in_silver
    detail_expr = when(
        already_in_silver,
        concat(lit(f"{pk}='"), col(pk), lit("' already present in Silver (scd_type=none has no update semantics)")),
    ).otherwise(concat(lit(f"duplicate {pk}='"), col(pk), lit("' within incoming batch")))

    df = df.withColumn("__dup_reject_detail", detail_expr)

    reject_df = finalize_rejects(
        df.filter(is_dupe), table_config.table_name, "duplicate_primary_key", "__dup_reject_detail"
    )
    passed_df = df.filter(~is_dupe).drop("__dedup_hash", "__dedup_rn", "__existing_pk", "__dup_reject_detail")

    return passed_df, reject_df


# COMMAND ----------

def _apply_scd2_single_batch(spark, batch_df, table_config):
    """Apply one batch_id's worth of SCD2 changes against the current Silver state."""
    pk = table_config.primary_key
    tracked_columns = table_config.tracked_columns or []
    silver_target = table_config.silver_target

    if not spark.catalog.tableExists(silver_target):
        insert_df = (
            batch_df.withColumn("effective_start_date", to_date(col("load_timestamp")))
            .withColumn("effective_end_date", lit(SCD2_END_DATE).cast("date"))
            .withColumn("is_current", lit(True))
        )
        insert_df.write.format("delta").mode("overwrite").saveAsTable(silver_target)
        return {"new_inserts": insert_df.count(), "new_versions": 0, "unchanged": 0}

    current_silver = spark.table(silver_target).filter(col("is_current") == True)  # noqa: E712

    staged = batch_df.alias("incoming").join(
        current_silver.select(pk, *tracked_columns).alias("current"),
        col(f"incoming.{pk}") == col(f"current.{pk}"),
        "left",
    )

    row_exists = col(f"current.{pk}").isNotNull()
    if tracked_columns:
        changed = reduce(
            lambda a, b: a | b,
            [~col(f"incoming.{tc}").eqNullSafe(col(f"current.{tc}")) for tc in tracked_columns],
        )
    else:
        changed = lit(False)

    staged = (
        staged.withColumn("__exists", row_exists)
        .withColumn("__changed", row_exists & changed)
        .withColumn("__new", ~row_exists)
        .withColumn("__unchanged", row_exists & ~changed)
    )

    incoming_col_refs = [col(f"incoming.{c}").alias(c) for c in batch_df.columns]
    to_insert = staged.filter(col("__new") | col("__changed")).select(
        *incoming_col_refs, col("__new"), col("__changed")
    )

    new_inserts_count = to_insert.filter(col("__new")).count()
    new_versions_count = to_insert.filter(col("__changed")).count()
    unchanged_count = staged.filter(col("__unchanged")).count()

    insert_df = (
        to_insert.drop("__new", "__changed")
        .withColumn("effective_start_date", to_date(col("load_timestamp")))
        .withColumn("effective_end_date", lit(SCD2_END_DATE).cast("date"))
        .withColumn("is_current", lit(True))
    )

    if new_versions_count > 0:
        keys_to_expire = (
            staged.filter(col("__changed"))
            .select(col(f"incoming.{pk}").alias(pk), to_date(col(f"incoming.load_timestamp")).alias("__new_end_date"))
            .distinct()
        )
        delta_table = DeltaTable.forName(spark, silver_target)
        (
            delta_table.alias("target")
            .merge(keys_to_expire.alias("source"), f"target.{pk} = source.{pk} AND target.is_current = true")
            .whenMatchedUpdate(set={"is_current": lit(False), "effective_end_date": col("source.__new_end_date")})
            .execute()
        )

    if new_inserts_count + new_versions_count > 0:
        insert_df.write.format("delta").mode("append").saveAsTable(silver_target)

    return {"new_inserts": new_inserts_count, "new_versions": new_versions_count, "unchanged": unchanged_count}


def apply_scd2(spark, incoming_df, table_config):
    """Replay each batch_id in chronological order so multi-version history
    (present all at once, since Bronze is a full union) is versioned correctly."""
    batch_order = [
        row["batch_id"]
        for row in (
            incoming_df.groupBy("batch_id").agg({"load_timestamp": "min"}).orderBy("min(load_timestamp)").collect()
        )
    ]

    totals = {"new_inserts": 0, "new_versions": 0, "unchanged": 0}
    for batch_id in batch_order:
        batch_result = _apply_scd2_single_batch(spark, incoming_df.filter(col("batch_id") == batch_id), table_config)
        for key in totals:
            totals[key] += batch_result[key]

    return {"rows_written": totals["new_inserts"] + totals["new_versions"], **totals}


# COMMAND ----------

def apply_scd1(spark, incoming_df, table_config):
    """No history to preserve, so collapse to the latest row per pk (by
    load_timestamp) first, then a single MERGE: update in place or insert."""
    pk = table_config.primary_key
    tracked_columns = table_config.tracked_columns or []
    silver_target = table_config.silver_target

    window_spec = Window.partitionBy(pk).orderBy(col("load_timestamp").desc())
    latest_df = (
        incoming_df.withColumn("__rn", row_number().over(window_spec)).filter(col("__rn") == 1).drop("__rn")
    )

    if not spark.catalog.tableExists(silver_target):
        latest_df.write.format("delta").mode("overwrite").saveAsTable(silver_target)
        return {"rows_written": latest_df.count()}

    delta_table = DeltaTable.forName(spark, silver_target)
    merge_builder = delta_table.alias("target").merge(latest_df.alias("source"), f"target.{pk} = source.{pk}")
    if tracked_columns:
        merge_builder = merge_builder.whenMatchedUpdate(set={c: f"source.{c}" for c in tracked_columns})
    merge_builder.whenNotMatchedInsertAll().execute()

    return {"rows_written": latest_df.count()}


# COMMAND ----------

def get_expected_silver_columns(table_config):
    """Business + pipeline-metadata columns that belong in this table's
    conformed Silver schema -- used to drop anything else (e.g. a drifted
    extra column like reason_notes, preserved from Bronze) before writing."""
    columns = list(table_config.expected_file_format.get("expected_headers", []))
    columns += ["batch_id", "load_timestamp", "_bronze_ingest_timestamp", "_source_file"]
    if table_config.scd_type == "SCD2":
        columns += ["effective_start_date", "effective_end_date", "is_current"]
    return columns


def apply_none(spark, incoming_df, table_config):
    """Append-only, no update semantics. Conform to the target schema before
    writing so a drifted extra column (e.g. reason_notes) doesn't persist
    into Silver's conformed schema."""
    silver_target = table_config.silver_target
    conformed_df = incoming_df.select(*get_expected_silver_columns(table_config))
    mode = "append" if spark.catalog.tableExists(silver_target) else "overwrite"
    conformed_df.write.format("delta").mode(mode).saveAsTable(silver_target)
    return {"rows_written": conformed_df.count()}


# COMMAND ----------

def process_table(spark, table_config, silver_tables_cache):
    table_name = table_config.table_name
    print(f"\n--- Processing Silver transform for '{table_name}' ---")

    bronze_df = spark.table(table_config.bronze_target)
    rows_read = bronze_df.count()

    working_df = bronze_df.withColumn(
        "_original_row_json", to_json(struct(*[col(c) for c in bronze_df.columns]))
    )

    reject_frames = []

    working_df, cast_rejects = cast_columns(working_df, table_config)
    reject_frames.append(cast_rejects)

    working_df, fk_reject_frames = check_referential_integrity(working_df, table_config, silver_tables_cache)
    reject_frames.extend(fk_reject_frames)

    existing_silver_df = (
        spark.table(table_config.silver_target) if spark.catalog.tableExists(table_config.silver_target) else None
    )
    working_df, dup_rejects = deduplicate_primary_key(working_df, table_config, existing_silver_df)
    reject_frames.append(dup_rejects)

    working_df = working_df.drop("_original_row_json")

    if table_config.scd_type == "SCD2":
        scd_result = apply_scd2(spark, working_df, table_config)
    elif table_config.scd_type == "SCD1":
        scd_result = apply_scd1(spark, working_df, table_config)
    else:
        scd_result = apply_none(spark, working_df, table_config)

    silver_tables_cache[table_name] = spark.table(table_config.silver_target)

    combined_rejects = reduce(lambda a, b: a.unionByName(b), reject_frames)
    reject_counts = {row["reject_reason"]: row["count"] for row in combined_rejects.groupBy("reject_reason").count().collect()}

    return {
        "table_name": table_name,
        "rows_read": rows_read,
        "rows_written": scd_result["rows_written"],
        "reject_counts": reject_counts,
        "total_rejects": sum(reject_counts.values()),
        "scd2_breakdown": scd_result if table_config.scd_type == "SCD2" else None,
        "rejects_df": combined_rejects,
    }


def print_table_summary(result):
    print(f"\n=== Summary: {result['table_name']} ===")
    print(f"  Rows read from Bronze:  {result['rows_read']}")
    print(f"  Rows written to Silver: {result['rows_written']}")
    print(f"  Rows quarantined:       {result['total_rejects']}")
    if result["reject_counts"]:
        for reason, count in sorted(result["reject_counts"].items()):
            print(f"    - {reason}: {count}")
    else:
        print("    (none)")

    if result["scd2_breakdown"] is not None:
        breakdown = result["scd2_breakdown"]
        print(
            f"  SCD2 breakdown -- new inserts: {breakdown['new_inserts']}, "
            f"new versions: {breakdown['new_versions']}, unchanged: {breakdown['unchanged']}"
        )


# COMMAND ----------

def main():
    table_configs = load_all_table_configs()
    print(f"Loaded {len(table_configs)} table configs (sequence order).")

    silver_tables_cache = {}
    all_rejects_frames = []

    for table_config in table_configs:
        try:
            result = process_table(spark, table_config, silver_tables_cache)
            all_rejects_frames.append(result["rejects_df"])
            print_table_summary(result)
        except Exception as exc:
            print(f"\nERROR: silver transform failed for table '{table_config.table_name}': {exc}")
            continue

    if all_rejects_frames:
        master_rejects_df = reduce(lambda a, b: a.unionByName(b), all_rejects_frames)
        rejects_mode = "append" if spark.catalog.tableExists(REJECTS_TABLE) else "overwrite"
        master_rejects_df.write.format("delta").mode(rejects_mode).saveAsTable(REJECTS_TABLE)
        print(f"\nWrote {master_rejects_df.count()} total quarantined rows to {REJECTS_TABLE}")
    else:
        print("\nNo tables processed successfully; nothing written to rejects table.")

    print("\nSilver transform complete.")


# COMMAND ----------

if __name__ == "__main__":
    main()
