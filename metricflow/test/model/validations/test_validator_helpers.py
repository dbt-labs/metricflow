from datetime import date
import json

import pytest

from metricflow.model.validations.validator_helpers import (
    DataSourceContext,
    DimensionContext,
    IdentifierContext,
    MaterializationContext,
    MeasureContext,
    MetricContext,
    ValidationContext,
    ValidationContextJSON,
    ValidationFutureError,
    ValidationIssue,
    ValidationIssueJSON,
    ValidationIssueLevel,
)


@pytest.fixture
def base_context_dict() -> ValidationContextJSON:  # noqa: D
    return {
        "file_name": "foo.py",
        "line_number": 1337,
    }


@pytest.fixture
def base_issue_dict(base_context_dict: ValidationContextJSON) -> ValidationIssueJSON:  # noqa: D
    return {
        "message": "An issue occured",
        "level": ValidationIssueLevel.ERROR.value,
        "context": base_context_dict,
    }


def test_can_load_issue_from_jsonified_issue() -> None:  # noqa: D
    issue = ValidationFutureError(
        context=ValidationContext(file_name="foo.yaml", line_number=1337),
        message="A issue was found that will be an error",
        error_date=date(2022, 6, 13),
    )

    jsonified_issue = issue.json()
    new_issue = ValidationIssue.from_json(jsonified_issue)

    assert isinstance(new_issue, ValidationFutureError)
    assert new_issue == issue


def test_load_validation_context(base_context_dict: ValidationContextJSON) -> None:  # noqa: D
    context = ValidationContext.from_dict({})
    assert isinstance(context, ValidationContext)
    assert context.line_number is None
    assert context.file_name is None

    context = ValidationContext.from_dict(base_context_dict)
    assert isinstance(context, ValidationContext)
    assert context.line_number == base_context_dict["line_number"]
    assert context.file_name == base_context_dict["file_name"]


def test_load_data_source_context(base_context_dict: ValidationContextJSON) -> None:  # noqa: D
    data_source_name = "data source alpha"
    base_context_dict["data_source_name"] = data_source_name

    context = ValidationContext.from_dict(base_context_dict)

    assert isinstance(context, DataSourceContext)
    assert context.data_source_name == data_source_name


def test_load_dimension_context(base_context_dict: ValidationContextJSON) -> None:  # noqa: D
    data_source_name = "data source alpha"
    dimension_name = "dimension beta"
    base_context_dict["data_source_name"] = data_source_name
    base_context_dict["dimension_name"] = dimension_name

    context = ValidationContext.from_dict(base_context_dict)

    assert isinstance(context, DimensionContext)
    assert context.data_source_name == data_source_name
    assert context.dimension_name == dimension_name


def test_load_identifier_context(base_context_dict: ValidationContextJSON) -> None:  # noqa: D
    data_source_name = "data source alpha"
    identifier_name = "identifer omega"
    base_context_dict["data_source_name"] = data_source_name
    base_context_dict["identifier_name"] = identifier_name

    context = ValidationContext.from_dict(base_context_dict)

    assert isinstance(context, IdentifierContext)
    assert context.data_source_name == data_source_name
    assert context.identifier_name == identifier_name


def test_load_measure_context(base_context_dict: ValidationContextJSON) -> None:  # noqa: D
    data_source_name = "data source alpha"
    measure_name = "measure delta"
    base_context_dict["data_source_name"] = data_source_name
    base_context_dict["measure_name"] = measure_name

    context = ValidationContext.from_dict(base_context_dict)

    assert isinstance(context, MeasureContext)
    assert context.data_source_name == data_source_name
    assert context.measure_name == measure_name


def test_load_materialization_context(base_context_dict: ValidationContextJSON) -> None:  # noqa: D
    materialization_name = "materialization epsilon"
    base_context_dict["materialization_name"] = materialization_name

    context = ValidationContext.from_dict(base_context_dict)

    assert isinstance(context, MaterializationContext)
    assert context.materialization_name == materialization_name


def test_load_metric_context(base_context_dict: ValidationContextJSON) -> None:  # noqa: D
    metric_name = "metric cappa"
    base_context_dict["metric_name"] = metric_name

    context = ValidationContext.from_dict(base_context_dict)

    assert isinstance(context, MetricContext)
    assert context.metric_name == metric_name


def test_load_validation_fatal(base_issue_dict: ValidationIssueJSON) -> None:  # noqa: D
    base_issue_dict["level"] = ValidationIssueLevel.FATAL.value

    issue = ValidationIssue.from_json(json.dumps(base_issue_dict))

    assert isinstance(issue, ValidationIssue)
    assert issue.level == ValidationIssueLevel.FATAL
    assert issue.message == base_issue_dict["message"]
    assert isinstance(issue.context, ValidationContext)


def test_load_validation_error(base_issue_dict: ValidationIssueJSON) -> None:  # noqa: D
    issue = ValidationIssue.from_json(json.dumps(base_issue_dict))

    assert isinstance(issue, ValidationIssue)
    assert issue.level == ValidationIssueLevel.ERROR
    assert issue.message == base_issue_dict["message"]
    assert isinstance(issue.context, ValidationContext)


def test_load_validation_future_error(base_issue_dict: ValidationIssueJSON) -> None:  # noqa: D
    error_date = date(2022, 6, 13)
    base_issue_dict["level"] = ValidationIssueLevel.FUTURE_ERROR.value
    base_issue_dict["error_date"] = error_date.isoformat()

    issue = ValidationIssue.from_json(json.dumps(base_issue_dict))

    assert isinstance(issue, ValidationFutureError)
    assert issue.level == ValidationIssueLevel.FUTURE_ERROR
    assert issue.message == base_issue_dict["message"]
    assert issue.error_date == error_date
    assert isinstance(issue.context, ValidationContext)


def test_load_validation_warning(base_issue_dict: ValidationIssueJSON) -> None:  # noqa: D
    base_issue_dict["level"] = ValidationIssueLevel.WARNING.value

    issue = ValidationIssue.from_json(json.dumps(base_issue_dict))

    assert isinstance(issue, ValidationIssue)
    assert issue.level == ValidationIssueLevel.WARNING
    assert issue.message == base_issue_dict["message"]
    assert isinstance(issue.context, ValidationContext)
