"""Shared pytest fixtures/hooks for the validator test suite."""
from __future__ import annotations

import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

import pytest

from framework.config_loader import load_all_table_configs, load_gold_mappings


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
