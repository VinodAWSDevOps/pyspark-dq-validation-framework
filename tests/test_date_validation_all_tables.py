import pytest

from framework.validators.date_validator import validate_date_consistency


@pytest.mark.regression
def test_date_validation(table_config):
    result = validate_date_consistency(table_config)
    assert result.passed, (
        f"Date consistency validation failed for '{table_config.table_name}':\n"
        + "\n".join(f"  - {issue}" for issue in result.issues)
    )
