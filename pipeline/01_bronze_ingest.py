# Databricks notebook source
# MAGIC %md
# MAGIC # Bronze Ingestion
# MAGIC Reads landing-zone CSVs for every table config (in sequence order),
# MAGIC aligns each file to the table's `expected_headers`, and writes the
# MAGIC combined result to `bronze_target` as a Delta table (overwrite).
# MAGIC
# MAGIC Bronze is intentionally raw: every column is read as a string. Casting
# MAGIC to real types happens in `02_silver_transform.py`.

# COMMAND ----------

import sys
from functools import reduce
from pathlib import Path

from pyspark.sql.functions import lit, current_timestamp

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

VOLUME_ROOT = "/Volumes/insurance_dq_qa/bronze/landing_zone"

# These two columns are always added by data_generation/*.py and are never
# treated as schema drift, even though they aren't part of expected_headers.
ALWAYS_EXPECTED_EXTRA_COLUMNS = {"batch_id", "load_timestamp"}


def align_schema(df, expected_headers, file_name, warnings_list):
    """Reorder/pad df to expected_headers; keep (and warn on) true extras."""
    df_columns = df.columns

    select_columns = []
    for header in expected_headers:
        if header in df_columns:
            select_columns.append(df[header])
        else:
            select_columns.append(lit(None).cast("string").alias(header))

    extra_columns = [c for c in df_columns if c not in expected_headers and c not in ALWAYS_EXPECTED_EXTRA_COLUMNS]
    for extra_col in extra_columns:
        message = f"file '{file_name}' has unexpected column '{extra_col}' not in expected_headers"
        print(f"WARNING: {message}")
        warnings_list.append(message)
        select_columns.append(df[extra_col])

    for meta_col in ALWAYS_EXPECTED_EXTRA_COLUMNS:
        if meta_col in df_columns and meta_col not in expected_headers:
            select_columns.append(df[meta_col])

    return df.select(*select_columns)


# COMMAND ----------

def ingest_table(table_config):
    """Read every CSV under this table's volume folder and write bronze_target."""
    warnings_list = []
    volume_path = f"{VOLUME_ROOT}/{table_config.volume_folder}/"

    print(f"\n--- Ingesting '{table_config.table_name}' from {volume_path} ---")

    csv_files = [f for f in dbutils.fs.ls(volume_path) if f.name.lower().endswith(".csv")]
    if not csv_files:
        print(f"WARNING: no CSV files found in {volume_path}")
        return {"table_name": table_config.table_name, "file_count": 0, "row_count": 0, "warnings": warnings_list}

    expected_headers = table_config.expected_file_format.get("expected_headers", [])
    delimiter = table_config.expected_file_format.get("delimiter", ",")
    encoding = table_config.expected_file_format.get("encoding", "utf-8")
    ingest_timestamp = current_timestamp()

    file_dataframes = []
    for file_info in csv_files:
        file_path = file_info.path
        file_name = file_info.name
        try:
            raw_df = (
                spark.read.option("header", True)
                .option("delimiter", delimiter)
                .option("encoding", encoding)
                .csv(file_path)
            )
            aligned_df = align_schema(raw_df, expected_headers, file_name, warnings_list)
            aligned_df = aligned_df.withColumn("_bronze_ingest_timestamp", ingest_timestamp).withColumn(
                "_source_file", lit(file_name)
            )
        except Exception as exc:
            raise RuntimeError(
                f"table '{table_config.table_name}': failed on file '{file_name}' ({file_path}): {exc}"
            ) from exc

        file_dataframes.append(aligned_df)

    combined_df = reduce(lambda left, right: left.unionByName(right, allowMissingColumns=True), file_dataframes)
    row_count = combined_df.count()

    combined_df.write.format("delta").mode("overwrite").saveAsTable(table_config.bronze_target)

    return {
        "table_name": table_config.table_name,
        "file_count": len(csv_files),
        "row_count": row_count,
        "warnings": warnings_list,
    }


# COMMAND ----------

def print_summary(summary):
    print(f"\n=== Summary: {summary['table_name']} ===")
    print(f"  Files ingested: {summary['file_count']}")
    print(f"  Rows written:   {summary['row_count']}")
    if summary["warnings"]:
        print(f"  Warnings ({len(summary['warnings'])}):")
        for warning in summary["warnings"]:
            print(f"    - {warning}")
    else:
        print("  Warnings: none")


def main():
    table_configs = load_all_table_configs()
    print(f"Loaded {len(table_configs)} table configs (sequence order).")

    for table_config in table_configs:
        try:
            summary = ingest_table(table_config)
            print_summary(summary)
        except Exception as exc:
            print(f"\nERROR: bronze ingestion failed for table '{table_config.table_name}': {exc}")
            continue

    print("\nBronze ingestion complete.")


# COMMAND ----------

if __name__ == "__main__":
    main()
