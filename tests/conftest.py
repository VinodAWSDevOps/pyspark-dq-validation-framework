"""Shared pytest fixtures/hooks for the validator test suite."""
from __future__ import annotations

import logging
import shutil
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

# databricks-sql-connector logs every thrift request/response at DEBUG,
# which drowns out our own print statements and pytest's own reporting.
logging.getLogger("databricks.sql").setLevel(logging.WARNING)
logging.getLogger("databricks").setLevel(logging.WARNING)

import pytest

from framework.config_loader import load_all_table_configs, load_gold_mappings

REPORTS_DIR = REPO_ROOT / "reports"
ALLURE_RESULTS_DIR = REPORTS_DIR / "allure-results"
ALLURE_SUPPORT_FILES = ("categories.json", "environment.properties")


@pytest.fixture(scope="session", autouse=True)
def _seed_allure_results_dir():
    """Copies categories.json/environment.properties into allure-results/
    once per session, before any test runs, so `allure generate` always has
    them available afterward without a separate manual copy step."""
    ALLURE_RESULTS_DIR.mkdir(parents=True, exist_ok=True)
    for filename in ALLURE_SUPPORT_FILES:
        source = REPORTS_DIR / filename
        if source.exists():
            shutil.copy(source, ALLURE_RESULTS_DIR / filename)


@pytest.fixture(scope="session")
def all_table_configs():
    return load_all_table_configs()


@pytest.fixture(scope="session")
def all_gold_mappings():
    return load_gold_mappings()


def pytest_generate_tests(metafunc):
    """Any test function with a `table_config` parameter is automatically
    parametrized across every table config, with test IDs set to the
    table's table_name (e.g. test_schema[customers]) instead of a generic
    index (test_schema[config0]). Same idea for a `gold_mapping` parameter,
    parametrized across every gold mapping with IDs set to gold_table."""
    if "table_config" in metafunc.fixturenames:
        table_configs = load_all_table_configs()
        metafunc.parametrize(
            "table_config",
            table_configs,
            ids=[config.table_name for config in table_configs],
        )

    if "gold_mapping" in metafunc.fixturenames:
        gold_mappings = load_gold_mappings()
        metafunc.parametrize(
            "gold_mapping",
            gold_mappings,
            ids=[mapping.gold_table for mapping in gold_mappings],
        )
