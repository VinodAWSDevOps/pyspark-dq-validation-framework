import pytest

from framework.validators.schema_validator import validate_schema


@pytest.mark.smoke
def test_schema(table_config):
    result = validate_schema(table_config)
    assert result.passed, (
        f"Schema validation failed for '{table_config.table_name}':\n"
        + "\n".join(f"  - {issue}" for issue in result.issues)
    )
