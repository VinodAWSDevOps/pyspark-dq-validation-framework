"""Shared result type every validator in framework/validators/ returns."""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Dict, List


@dataclass
class ValidationResult:
    table_name: str
    validator_name: str
    passed: bool
    issues: List[str] = field(default_factory=list)
    details: Dict[str, Any] = field(default_factory=dict)

    @property
    def summary(self) -> str:
        status = "PASSED" if self.passed else "FAILED"
        header = f"[{status}] {self.validator_name} on '{self.table_name}'"
        if self.passed or not self.issues:
            return header
        issue_lines = "\n".join(f"  - {issue}" for issue in self.issues)
        return f"{header} ({len(self.issues)} issue{'s' if len(self.issues) != 1 else ''}):\n{issue_lines}"

    def __str__(self) -> str:
        return self.summary
