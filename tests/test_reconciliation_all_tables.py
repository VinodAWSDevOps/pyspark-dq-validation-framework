import pytest

from framework.validators.reconciliation_validator import validate_reconciliation


@pytest.mark.regression
def test_reconciliation(gold_mapping):
    result = validate_reconciliation(gold_mapping)
    assert result.passed, (
        f"Reconciliation validation failed for '{result.table_name}':\n"
        + "\n".join(f"  - {issue}" for issue in result.issues)
    )
