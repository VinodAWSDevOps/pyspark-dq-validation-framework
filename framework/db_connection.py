"""Short-lived Databricks SQL connections for the validator/test suite.

Each call opens a fresh connection, runs its query, and closes -- no pooling
or persistent session, since pytest will call these many times across many
tests and a long-lived connection isn't worth the state to manage.
"""
from __future__ import annotations

import os
from pathlib import Path

from databricks import sql
from dotenv import load_dotenv

ENV_PATH = Path(__file__).resolve().parent.parent / ".env"
REQUIRED_ENV_VARS = ("DATABRICKS_SERVER_HOSTNAME", "DATABRICKS_HTTP_PATH", "DATABRICKS_ACCESS_TOKEN")


def get_connection():
    """Read Databricks connection details from .env and open a connection."""
    load_dotenv(dotenv_path=ENV_PATH)

    missing = [var for var in REQUIRED_ENV_VARS if not os.environ.get(var)]
    if missing:
        raise RuntimeError(f"Missing required environment variable(s) in .env: {', '.join(missing)}")

    return sql.connect(
        server_hostname=os.environ["DATABRICKS_SERVER_HOSTNAME"],
        http_path=os.environ["DATABRICKS_HTTP_PATH"],
        access_token=os.environ["DATABRICKS_ACCESS_TOKEN"],
    )


def run_query(sql_text: str) -> list[dict]:
    """Open a connection, run sql_text, return rows as column_name -> value dicts."""
    connection = get_connection()
    try:
        cursor = connection.cursor()
        try:
            cursor.execute(sql_text)
            columns = [description[0] for description in cursor.description]
            return [dict(zip(columns, row)) for row in cursor.fetchall()]
        finally:
            cursor.close()
    finally:
        connection.close()


def get_query_columns(sql_text: str) -> list[str]:
    """Column names for sql_text's result set, even with zero rows returned
    (e.g. a LIMIT 0 query). cursor.description reflects the result schema
    regardless of row count, but run_query()'s list[dict] return has nothing
    to key off of when there are no rows -- used by file_format_validator to
    read read_files()'s inferred header union without fetching any data.
    """
    connection = get_connection()
    try:
        cursor = connection.cursor()
        try:
            cursor.execute(sql_text)
            return [description[0] for description in cursor.description]
        finally:
            cursor.close()
    finally:
        connection.close()


def describe_table(fully_qualified_table_name: str) -> list[dict]:
    """Column name/type/comment info for a table, used by schema_validator
    instead of a Spark DataFrame's .schema (no Spark session here)."""
    return run_query(f"DESCRIBE TABLE {fully_qualified_table_name}")
