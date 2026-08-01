import pytest

from framework.validators.referential_integrity_validator import validate_referential_integrity


@pytest.mark.smoke
def test_referential_integrity(table_config):
    result = validate_referential_integrity(table_config)
    assert result.passed, (
        f"Referential integrity validation failed for '{table_config.table_name}':\n"
        + "\n".join(f"  - {issue}" for issue in result.issues)
    )
