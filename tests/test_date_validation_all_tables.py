import allure
import pytest

from framework.validators.date_validator import validate_date_consistency


@allure.feature("Date Consistency")
@allure.severity(allure.severity_level.NORMAL)
@pytest.mark.regression
def test_date_validation(table_config):
    allure.dynamic.story(table_config.table_name)
    result = validate_date_consistency(table_config)
    if not result.passed:
        pytest.fail("\n".join(result.issues), pytrace=False)
