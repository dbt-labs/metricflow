from datetime import date
from typing import List

import pytest
from metricflow.instances import (
    EntityElementReference,
    EntityReference,
    MetricModelReference,
)

from metricflow.model.validations.validator_helpers import (
    EntityContext,
    EntityElementContext,
    EntityElementType,
    FileContext,
    MetricContext,
    ModelValidationResults,
    ValidationError,
    ValidationFutureError,
    ValidationIssueLevel,
    ValidationIssueType,
    ValidationWarning,
)


@pytest.fixture
def list_of_issues() -> List[ValidationIssueType]:  # noqa: D
    file_context = FileContext(file_name="foo", line_number=1337)
    entity_name = "My entity"

    issues: List[ValidationIssueType] = []
    issues.append(
        ValidationWarning(
            context=EntityContext(
                file_context=file_context,
                entity=EntityReference(entity_name=entity_name),
            ),
            message="Something caused a warning, problem #1",
        )
    )
    issues.append(
        ValidationWarning(
            context=EntityElementContext(
                file_context=file_context,
                entity_element=EntityElementReference(
                    entity_name=entity_name, element_name="My dimension"
                ),
                element_type=EntityElementType.DIMENSION,
            ),
            message="Something caused a warning, problem #2",
        )
    )
    issues.append(
        ValidationFutureError(
            context=EntityElementContext(
                file_context=file_context,
                entity_element=EntityElementReference(
                    entity_name=entity_name, element_name="My identifier"
                ),
                element_type=EntityElementType.IDENTIFIER,
            ),
            message="Something caused a future error, problem #3",
            error_date=date(2022, 6, 13),
        )
    )
    issues.append(
        ValidationError(
            context=EntityElementContext(
                file_context=file_context,
                entity_name=entity_name,
                entity_element=EntityElementReference(
                    entity_name=entity_name, element_name="My measure"
                ),
                element_type=EntityElementType.MEASURE,
            ),
            message="Something caused an error, problem #4",
        )
    )
    issues.append(
        ValidationError(
            context=MetricContext(
                file_context=file_context,
                metric=MetricModelReference(metric_name="My metric"),
            ),
            message="Something caused a error, problem #6",
        )
    )
    issues.append(ValidationError(context=file_context, message="Something caused a error, probelm #7"))
    return issues


def test_creating_model_validation_results_from_issue_list(  # noqa: D
    list_of_issues: List[ValidationIssueType],
) -> None:
    warnings = [issue for issue in list_of_issues if issue.level == ValidationIssueLevel.WARNING]
    future_errors = [issue for issue in list_of_issues if issue.level == ValidationIssueLevel.FUTURE_ERROR]
    errors = [issue for issue in list_of_issues if issue.level == ValidationIssueLevel.ERROR]

    model_validation_issues = ModelValidationResults.from_issues_sequence(list_of_issues)
    assert len(model_validation_issues.warnings) == len(warnings)
    assert len(model_validation_issues.future_errors) == len(future_errors)
    assert len(model_validation_issues.errors) == len(errors)
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
    assert set_context_types == set(new_context_types)


def test_merge_two_model_validation_results(list_of_issues: List[ValidationIssueType]) -> None:  # noqa: D
    validation_results = ModelValidationResults.from_issues_sequence(list_of_issues)
    validation_results_dup = ModelValidationResults.from_issues_sequence(list_of_issues)
    merged = ModelValidationResults.merge([validation_results, validation_results_dup])

    assert merged.warnings == validation_results.warnings + validation_results_dup.warnings
    assert merged.future_errors == validation_results.future_errors + validation_results_dup.future_errors
    assert merged.errors == validation_results.errors + validation_results_dup.errors
