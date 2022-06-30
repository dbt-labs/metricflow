from datetime import date
import json
from typing import Any, Dict, List, Union

import pytest

from metricflow.model.validations.validator_helpers import (
    DataSourceContext,
    DimensionContext,
    FileContext,
    IdentifierContext,
    MaterializationContext,
    MeasureContext,
    MetricContext,
    ModelValidationResults,
    ValidationContextJSON,
    ValidationError,
    ValidationFatal,
    ValidationFutureError,
    ValidationIssue,
    ValidationIssueJSON,
    ValidationIssueLevel,
    ValidationIssueType,
    ValidationWarning,
)


@pytest.fixture
def file_context_dict() -> Dict[str, Union[str, int]]:  # noqa: D
    return {
        "file_name": "foo.py",
        "line_number": 1337,
    }


@pytest.fixture
def data_source_context_dict(file_context_dict: Dict[str, Union[str, int]]) -> Dict[str, Any]:  # noqa: D
    return {
        "file_context": file_context_dict,
        "data_source_name": "data source alpha",
    }


@pytest.fixture
def dimension_context_dict(file_context_dict: Dict[str, Union[str, int]]) -> Dict[str, Any]:  # noqa: D
    return {
        "file_context": file_context_dict,
        "data_source_name": "data source alpha",
        "dimension_name": "dimension beta",
    }


@pytest.fixture
def identifier_context_dict(file_context_dict: Dict[str, Union[str, int]]) -> Dict[str, Any]:  # noqa: D
    return {
        "file_context": file_context_dict,
        "data_source_name": "data source alpha",
        "identifier_name": "identifer omega",
    }


@pytest.fixture
def measure_context_dict(file_context_dict: Dict[str, Union[str, int]]) -> Dict[str, Any]:  # noqa: D
    return {
        "file_context": file_context_dict,
        "data_source_name": "data source alpha",
        "measure_name": "measure delta",
    }


@pytest.fixture
def materialization_context_dict(file_context_dict: Dict[str, Union[str, int]]) -> Dict[str, Any]:  # noqa: D
    return {
        "file_context": file_context_dict,
        "materialization_name": "materialization epsilon",
    }


@pytest.fixture
def metric_context_dict(file_context_dict: Dict[str, Union[str, int]]) -> Dict[str, Any]:  # noqa: D
    return {
        "file_context": file_context_dict,
        "metric_name": "metric cappa",
    }


@pytest.fixture
def base_issue_dict(file_context_dict: ValidationContextJSON) -> ValidationIssueJSON:  # noqa: D
    return {
        "message": "An issue occured",
        "context": file_context_dict,
    }


@pytest.fixture
def list_of_issues(  # noqa: D
    file_context_dict: Dict[str, Union[str, int]],
    data_source_context_dict: Dict[str, Any],
    dimension_context_dict: Dict[str, Any],
    identifier_context_dict: Dict[str, Any],
    measure_context_dict: Dict[str, Any],
    materialization_context_dict: Dict[str, Any],
    metric_context_dict: Dict[str, Any],
) -> List[ValidationIssueType]:
    issues: List[ValidationIssueType] = []
    issues.append(
        ValidationWarning(
            context=DataSourceContext.parse_obj(data_source_context_dict),
            message="Something caused a warning, problem #1",
        )
    )
    issues.append(
        ValidationWarning(
            context=DimensionContext.parse_obj(dimension_context_dict), message="Something caused a warning, problem #2"
        )
    )
    issues.append(
        ValidationFutureError(
            context=IdentifierContext.parse_obj(identifier_context_dict),
            message="Something caused a future error, problem #3",
            error_date=date(2022, 6, 13),
        )
    )
    issues.append(
        ValidationError(
            context=MeasureContext.parse_obj(measure_context_dict), message="Something caused an error, problem #4"
        )
    )
    issues.append(
        ValidationError(
            context=MaterializationContext.parse_obj(materialization_context_dict),
            message="Something caused an error, problem #5",
        )
    )
    issues.append(
        ValidationFatal(
            context=MetricContext.parse_obj(metric_context_dict), message="Something caused a fatal, problem #6"
        )
    )
    issues.append(
        ValidationFatal(
            context=FileContext.parse_obj(file_context_dict), message="Something caused a fatal, probelm #7"
        )
    )
    return issues


def test_load_validation_context(file_context_dict: Dict[str, Union[str, int]]) -> None:  # noqa: D
    context = FileContext.parse_obj({})
    assert isinstance(context, FileContext)
    assert context.line_number is None
    assert context.file_name is None
    assert context.context_str() == ""

    context = FileContext.parse_obj(file_context_dict)
    assert isinstance(context, FileContext)
    assert context.line_number == file_context_dict["line_number"]
    assert context.file_name == file_context_dict["file_name"]
    assert context.context_str()


def test_load_data_source_context(data_source_context_dict: Dict[str, Any]) -> None:  # noqa: D
    context = DataSourceContext.parse_obj(data_source_context_dict)

    assert isinstance(context, DataSourceContext)
    assert context.data_source_name == data_source_context_dict["data_source_name"]
    assert context.context_str()


def test_load_dimension_context(dimension_context_dict: Dict[str, Any]) -> None:  # noqa: D
    context = DimensionContext.parse_obj(dimension_context_dict)

    assert isinstance(context, DimensionContext)
    assert context.data_source_name == dimension_context_dict["data_source_name"]
    assert context.dimension_name == dimension_context_dict["dimension_name"]
    assert context.context_str()


def test_load_identifier_context(identifier_context_dict: Dict[str, Any]) -> None:  # noqa: D
    context = IdentifierContext.parse_obj(identifier_context_dict)

    assert isinstance(context, IdentifierContext)
    assert context.data_source_name == identifier_context_dict["data_source_name"]
    assert context.identifier_name == identifier_context_dict["identifier_name"]
    assert context.context_str()


def test_load_measure_context(measure_context_dict: Dict[str, Any]) -> None:  # noqa: D
    context = MeasureContext.parse_obj(measure_context_dict)

    assert isinstance(context, MeasureContext)
    assert context.data_source_name == measure_context_dict["data_source_name"]
    assert context.measure_name == measure_context_dict["measure_name"]
    assert context.context_str()


def test_load_materialization_context(materialization_context_dict: Dict[str, Any]) -> None:  # noqa: D
    context = MaterializationContext.parse_obj(materialization_context_dict)

    assert isinstance(context, MaterializationContext)
    assert context.materialization_name == materialization_context_dict["materialization_name"]
    assert context.context_str()


def test_load_metric_context(metric_context_dict: ValidationContextJSON) -> None:  # noqa: D
    context = MetricContext.parse_obj(metric_context_dict)

    assert isinstance(context, MetricContext)
    assert context.metric_name == metric_context_dict["metric_name"]
    assert context.context_str()


def test_load_validation_fatal(base_issue_dict: ValidationIssueJSON) -> None:  # noqa: D
    issue = ValidationFatal.parse_raw(json.dumps(base_issue_dict))

    assert isinstance(issue, ValidationIssue)
    assert issue.level == ValidationIssueLevel.FATAL
    assert issue.message == base_issue_dict["message"]
    assert isinstance(issue.context, FileContext)


def test_load_validation_error(base_issue_dict: ValidationIssueJSON) -> None:  # noqa: D
    issue = ValidationError.parse_raw(json.dumps(base_issue_dict))

    assert isinstance(issue, ValidationIssue)
    assert issue.level == ValidationIssueLevel.ERROR
    assert issue.message == base_issue_dict["message"]
    assert isinstance(issue.context, FileContext)


def test_load_validation_future_error(base_issue_dict: ValidationIssueJSON) -> None:  # noqa: D
    error_date = date(2022, 6, 13)
    base_issue_dict["error_date"] = error_date.isoformat()

    issue = ValidationFutureError.parse_raw(json.dumps(base_issue_dict))

    assert isinstance(issue, ValidationFutureError)
    assert issue.level == ValidationIssueLevel.FUTURE_ERROR
    assert issue.message == base_issue_dict["message"]
    assert issue.error_date == error_date
    assert isinstance(issue.context, FileContext)


def test_load_validation_warning(base_issue_dict: ValidationIssueJSON) -> None:  # noqa: D
    issue = ValidationWarning.parse_raw(json.dumps(base_issue_dict))

    assert isinstance(issue, ValidationIssue)
    assert issue.level == ValidationIssueLevel.WARNING
    assert issue.message == base_issue_dict["message"]
    assert isinstance(issue.context, FileContext)


def test_creating_model_validation_results_from_issue_list(  # noqa: D
    list_of_issues: List[ValidationIssueType],
) -> None:
    warnings = [issue for issue in list_of_issues if issue.level == ValidationIssueLevel.WARNING]
    future_errors = [issue for issue in list_of_issues if issue.level == ValidationIssueLevel.FUTURE_ERROR]
    errors = [issue for issue in list_of_issues if issue.level == ValidationIssueLevel.ERROR]
    fatals = [issue for issue in list_of_issues if issue.level == ValidationIssueLevel.FATAL]

    model_validation_issues = ModelValidationResults.from_issues_sequence(list_of_issues)
    assert len(model_validation_issues.warnings) == len(warnings)
    assert len(model_validation_issues.future_errors) == len(future_errors)
    assert len(model_validation_issues.errors) == len(errors)
    assert len(model_validation_issues.fatals) == len(fatals)
    assert model_validation_issues.has_blocking_issues

    model_validation_issues = ModelValidationResults(warnings=warnings, future_errors=future_errors)
    assert not model_validation_issues.has_blocking_issues


def test_jsonifying_and_reloading_model_validation_results_is_equal(  # noqa: D
    list_of_issues: List[ValidationIssueType],
) -> None:
    warnings = [issue for issue in list_of_issues if issue.level == ValidationIssueLevel.WARNING]
    errors = [issue for issue in list_of_issues if issue.level == ValidationIssueLevel.ERROR]
    set_context_types = set([issue.context.__class__ for issue in list_of_issues])

    model_validation_issues = ModelValidationResults.from_issues_sequence(list_of_issues)
    model_validation_issues_new = ModelValidationResults.parse_raw(model_validation_issues.json())
    assert model_validation_issues_new == model_validation_issues
    assert model_validation_issues_new != ModelValidationResults(warnings=warnings, errors=errors)

    # ensure ValidationContexts were properly parsed into the differen subclasses
    new_context_types = [issue.context.__class__ for issue in model_validation_issues_new.warnings]
    new_context_types += [issue.context.__class__ for issue in model_validation_issues_new.future_errors]
    new_context_types += [issue.context.__class__ for issue in model_validation_issues_new.errors]
    new_context_types += [issue.context.__class__ for issue in model_validation_issues_new.fatals]
    assert set_context_types == set(new_context_types)


def test_merge_two_model_validation_results(list_of_issues: List[ValidationIssueType]) -> None:  # noqa: D
    validation_results = ModelValidationResults.from_issues_sequence(list_of_issues)
    validation_results_dup = ModelValidationResults.from_issues_sequence(list_of_issues)
    merged = ModelValidationResults.merge([validation_results, validation_results_dup])

    assert merged.warnings == validation_results.warnings + validation_results_dup.warnings
    assert merged.future_errors == validation_results.future_errors + validation_results_dup.future_errors
    assert merged.errors == validation_results.errors + validation_results_dup.errors
    assert merged.fatals == validation_results.fatals + validation_results_dup.fatals
