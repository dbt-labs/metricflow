from datetime import date

from metricflow.model.validations.validator_helpers import ValidationContext, ValidationFutureError


def test_validaiton_issue_to_json() -> None:  # noqa: D
    issue = ValidationFutureError(
        context=ValidationContext(file_name="foo.yaml", line_number=1337),
        message="A issue was found that will be an error",
        error_date=date(2022, 6, 13),
    )

    assert issue.json(), f"Failed to jsonify issue: {issue}"
