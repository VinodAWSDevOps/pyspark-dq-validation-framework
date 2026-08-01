import pytest

from framework.validators.scd_validator import validate_scd


@pytest.mark.regression
def test_scd(table_config):
    result = validate_scd(table_config)
    assert result.passed, (
        f"SCD validation failed for '{table_config.table_name}':\n"
        + "\n".join(f"  - {issue}" for issue in result.issues)
    )
