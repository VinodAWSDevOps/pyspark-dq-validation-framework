import pytest

from framework.validators.business_rule_validator import validate_business_rules


@pytest.mark.regression
def test_business_rules(table_config):
    result = validate_business_rules(table_config)
    assert result.passed, (
        f"Business rule validation failed for '{table_config.table_name}':\n"
        + "\n".join(f"  - {issue}" for issue in result.issues)
    )
