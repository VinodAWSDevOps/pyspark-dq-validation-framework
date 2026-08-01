import allure
import pytest

from framework.validators.file_format_validator import validate_file_format


@allure.feature("File Format")
@allure.severity(allure.severity_level.NORMAL)
@pytest.mark.regression
def test_file_format(table_config):
    allure.dynamic.story(table_config.table_name)
    result = validate_file_format(table_config)
    if not result.passed:
        pytest.fail("\n".join(result.issues), pytrace=False)
