import allure
import pytest

from framework.validators.referential_integrity_validator import validate_referential_integrity


@allure.feature("Referential Integrity")
@allure.severity(allure.severity_level.CRITICAL)
@pytest.mark.smoke
def test_referential_integrity(table_config):
    allure.dynamic.story(table_config.table_name)
    result = validate_referential_integrity(table_config)
    if not result.passed:
        pytest.fail("\n".join(result.issues), pytrace=False)
