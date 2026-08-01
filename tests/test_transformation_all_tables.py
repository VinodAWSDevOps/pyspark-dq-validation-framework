import pytest

from framework.validators.transformation_validator import validate_transformation


@pytest.mark.regression
def test_transformation(table_config):
    result = validate_transformation(table_config)
    assert result.passed, (
        f"Transformation validation failed for '{table_config.table_name}':\n"
        + "\n".join(f"  - {issue}" for issue in result.issues)
    )
