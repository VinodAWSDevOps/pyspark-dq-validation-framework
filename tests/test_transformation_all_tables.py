import allure
import pytest

from framework.validators.transformation_validator import validate_transformation


@allure.feature("Transformation")
@allure.severity(allure.severity_level.NORMAL)
@pytest.mark.regression
def test_transformation(table_config):
    allure.dynamic.story(table_config.table_name)
    result = validate_transformation(table_config)
    if not result.passed:
        pytest.fail("\n".join(result.issues), pytrace=False)
