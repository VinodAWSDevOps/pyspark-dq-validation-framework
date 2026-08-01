import allure
import pytest

from framework.validators.uniqueness_validator import validate_uniqueness


@allure.feature("Uniqueness")
@allure.severity(allure.severity_level.CRITICAL)
@pytest.mark.smoke
def test_uniqueness(table_config):
    allure.dynamic.story(table_config.table_name)
    result = validate_uniqueness(table_config)
    if not result.passed:
        pytest.fail("\n".join(result.issues), pytrace=False)
