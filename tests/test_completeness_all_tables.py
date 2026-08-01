import allure
import pytest

from framework.validators.completeness_validator import validate_completeness


@allure.feature("Completeness")
@allure.severity(allure.severity_level.CRITICAL)
@pytest.mark.smoke
def test_completeness(table_config):
    allure.dynamic.story(table_config.table_name)
    result = validate_completeness(table_config)
    if not result.passed:
        pytest.fail("\n".join(result.issues), pytrace=False)
