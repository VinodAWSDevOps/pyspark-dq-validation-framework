import allure
import pytest

from framework.validators.reconciliation_validator import validate_reconciliation


@allure.feature("Reconciliation")
@allure.severity(allure.severity_level.NORMAL)
@pytest.mark.regression
def test_reconciliation(gold_mapping):
    allure.dynamic.story(gold_mapping.gold_table)
    result = validate_reconciliation(gold_mapping)
    if not result.passed:
        pytest.fail("\n".join(result.issues), pytrace=False)
