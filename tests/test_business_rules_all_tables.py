import allure
import pytest

from framework.validators.business_rule_validator import validate_business_rules


@allure.feature("Business Rules")
@allure.severity(allure.severity_level.NORMAL)
@pytest.mark.regression
def test_business_rules(table_config):
    allure.dynamic.story(table_config.table_name)
    result = validate_business_rules(table_config)
    if not result.passed:
        pytest.fail("\n".join(result.issues), pytrace=False)
