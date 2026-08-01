import allure
import pytest

from framework.validators.scd_validator import validate_scd


@allure.feature("SCD Validation")
@allure.severity(allure.severity_level.NORMAL)
@pytest.mark.regression
def test_scd(table_config):
    allure.dynamic.story(table_config.table_name)
    result = validate_scd(table_config)
    if not result.passed:
        pytest.fail("\n".join(result.issues), pytrace=False)
