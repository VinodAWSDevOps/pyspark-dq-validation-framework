import allure
import pytest

from framework.validators.schema_validator import validate_schema


@allure.feature("Schema Validation")
@allure.severity(allure.severity_level.CRITICAL)
@pytest.mark.smoke
def test_schema(table_config):
    allure.dynamic.story(table_config.table_name)
    result = validate_schema(table_config)
    if not result.passed:
        pytest.fail("\n".join(result.issues), pytrace=False)
