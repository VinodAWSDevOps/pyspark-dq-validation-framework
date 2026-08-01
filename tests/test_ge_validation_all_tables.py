import pytest

from framework.validators.ge_runner import run_ge_validation


@pytest.mark.regression
def test_ge_validation(table_config):
    result = run_ge_validation(table_config)
    assert result.passed, (
        f"Great Expectations validation failed for '{table_config.table_name}':\n"
        + "\n".join(f"  - {issue}" for issue in result.issues)
    )
