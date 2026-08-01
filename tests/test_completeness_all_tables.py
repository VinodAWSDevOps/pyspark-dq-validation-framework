import pytest

from framework.validators.completeness_validator import validate_completeness


@pytest.mark.smoke
def test_completeness(table_config):
    result = validate_completeness(table_config)
    assert result.passed, (
        f"Completeness validation failed for '{table_config.table_name}':\n"
        + "\n".join(f"  - {issue}" for issue in result.issues)
    )
