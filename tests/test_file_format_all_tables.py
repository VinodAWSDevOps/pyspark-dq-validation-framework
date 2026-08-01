import pytest

from framework.validators.file_format_validator import validate_file_format


@pytest.mark.regression
def test_file_format(table_config):
    result = validate_file_format(table_config)
    assert result.passed, (
        f"File format validation failed for '{table_config.table_name}':\n"
        + "\n".join(f"  - {issue}" for issue in result.issues)
    )
