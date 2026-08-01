import allure
import pytest

from framework.validators.ge_runner import run_ge_validation


@allure.feature("Great Expectations")
@allure.severity(allure.severity_level.NORMAL)
@pytest.mark.regression
def test_ge_validation(table_config):
    allure.dynamic.story(table_config.table_name)
    result = run_ge_validation(table_config)
    if not result.passed:
        pytest.fail("\n".join(result.issues), pytrace=False)
