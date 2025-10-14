from __future__ import annotations

from typing import List, Tuple

from metricflow_semantic_interfaces.validations.validator_helpers import ValidationIssue


def check_error_in_issues(error_substrings: List[str], issues: Tuple[ValidationIssue, ...]) -> None:
    """Check error substrings in build issues."""
    missing_error_strings = set()
    for expected_str in error_substrings:
        if not any(actual_str.as_readable_str().find(expected_str) != -1 for actual_str in issues):
            missing_error_strings.add(expected_str)
    assert len(missing_error_strings) == 0, (
        "Failed to match one or more expected issues: "
        + f"{missing_error_strings} in {set([x.as_readable_str() for x in issues])}"
    )
