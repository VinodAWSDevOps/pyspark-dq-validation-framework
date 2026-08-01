import pytest

from framework.validators.uniqueness_validator import validate_uniqueness


@pytest.mark.smoke
def test_uniqueness(table_config):
    result = validate_uniqueness(table_config)
    assert result.passed, (
        f"Uniqueness validation failed for '{table_config.table_name}':\n"
        + "\n".join(f"  - {issue}" for issue in result.issues)
    )
