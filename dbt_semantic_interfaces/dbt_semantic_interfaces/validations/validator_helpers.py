from __future__ import annotations

import functools
import traceback
from abc import ABC, abstractmethod
from dataclasses import dataclass
from datetime import date
from enum import Enum
from typing import Any, Callable, Dict, List, Optional, Sequence, Tuple, Union

import click
from pydantic import BaseModel, Extra

from dbt_semantic_interfaces.enum_extension import assert_values_exhausted
from dbt_semantic_interfaces.objects.base import FrozenBaseModel
from dbt_semantic_interfaces.objects.metadata import Metadata
from dbt_semantic_interfaces.objects.semantic_manifest import SemanticManifest
from dbt_semantic_interfaces.references import (
    MetricModelReference,
    SemanticModelElementReference,
    SemanticModelReference,
)
from dbt_semantic_interfaces.type_enums.dimension_type import DimensionType

VALIDATE_SAFELY_ERROR_STR_TMPLT = ". Issue occurred in method `{method_name}` called with {arguments_str}"
ValidationContextJSON = Dict[str, Union[str, int, None]]
ValidationIssueJSON = Dict[str, Union[str, int, ValidationContextJSON]]


class ValidationIssueLevel(Enum):
    """Categorize the issues found while validating a MQL model."""

    # Issue should be fixed, but model will still work in MQL
    WARNING = 0
    # Issue doesn't prevent model from working in MQL yet, but will eventually be an error
    FUTURE_ERROR = 1
    # Issue will prevent the model from working in MQL
    ERROR = 2

    @property
    def name_plural(self) -> str:
        """Controlled pluralization of ValidationIssueLevel name value."""
        return f"{self.name}S"


ISSUE_COLOR_MAP = {
    ValidationIssueLevel.WARNING: "cyan",
    ValidationIssueLevel.ERROR: "bright_red",
    ValidationIssueLevel.FUTURE_ERROR: "bright_yellow",
}


class SemanticModelElementType(Enum):
    """Maps semantic model element types to a readable string."""

    MEASURE = "measure"
    DIMENSION = "dimension"
    ENTITY = "entity"


class FileContext(BaseModel):
    """The base context class for validation issues."""

    file_name: Optional[str]
    line_number: Optional[int]

    class Config:
        """Pydantic class configuration options."""

        extra = Extra.forbid

    def context_str(self) -> str:
        """Human readable stringified representation of the context."""
        context_string = ""

        if self.file_name:
            context_string += f"in file `{self.file_name}`"
            if self.line_number:
                context_string += f" on line #{self.line_number}"

        return context_string

    @classmethod
    def from_metadata(cls, metadata: Optional[Metadata] = None) -> FileContext:
        """Creates a FileContext instance from a Metadata object."""
        return cls(
            file_name=metadata.file_slice.filename if metadata else None,
            line_number=metadata.file_slice.start_line_number if metadata else None,
        )


class MetricContext(BaseModel):
    """The context class for validation issues involving metrics."""

    file_context: FileContext
    metric: MetricModelReference

    def context_str(self) -> str:
        """Human readable stringified representation of the context."""
        return f"with metric `{self.metric.metric_name}` {self.file_context.context_str()}"


class SemanticModelContext(BaseModel):
    """The context class for validation issues involving semantic models."""

    file_context: FileContext
    semantic_model: SemanticModelReference

    def context_str(self) -> str:
        """Human readable stringified representation of the context."""
        return f"with semantic model `{self.semantic_model.semantic_model_name}` {self.file_context.context_str()}"


class SemanticModelElementContext(BaseModel):
    """The context class for validation issues involving dimensions."""

    file_context: FileContext
    semantic_model_element: SemanticModelElementReference
    element_type: SemanticModelElementType

    def context_str(self) -> str:
        """Human readable stringified representation of the context."""
        return (
            f"with {self.element_type.value} `{self.semantic_model_element.element_name}` in semantic model "
            f"`{self.semantic_model_element.semantic_model_name}` {self.file_context.context_str()}"
        )


ValidationContext = Union[
    FileContext,
    MetricContext,
    SemanticModelContext,
    SemanticModelElementContext,
]


class ValidationIssue(ABC, BaseModel):
    """The abstract base ValidationIsssue class that the specific ValidationIssue classes are built from."""

    message: str
    context: Optional[ValidationContext] = None
    extra_detail: Optional[str]

    @property
    @abstractmethod
    def level(self) -> ValidationIssueLevel:
        """The level of of ValidationIssue."""
        raise NotImplementedError

    def as_readable_str(self, verbose: bool = False, prefix: Optional[str] = None) -> str:
        """Return a easily readable string that can be used to log the issue."""
        prefix = prefix or self.level.name

        # The following is two lines instead of one line because
        # technically self.context.context_str() can return an empty str
        context_str = self.context.context_str() if self.context else ""
        context_str += " - " if context_str != "" else ""

        issue_str = f"{prefix}: {context_str}{self.message}"
        if verbose and self.extra_detail is not None:
            issue_str += f"\n{self.extra_detail}"

        return issue_str

    def as_cli_formatted_str(self, verbose: bool = False) -> str:
        """Returns a color-coded readable string for rendering issues in the CLI."""
        return self.as_readable_str(
            verbose=verbose, prefix=click.style(self.level.name, bold=True, fg=ISSUE_COLOR_MAP[self.level])
        )


class ValidationWarning(ValidationIssue, BaseModel):
    """A warning that was found while validating the model."""

    @property
    def level(self) -> ValidationIssueLevel:  # noqa: D
        return ValidationIssueLevel.WARNING


class ValidationFutureError(ValidationIssue, BaseModel):
    """A future error that was found while validating the model."""

    error_date: date

    @property
    def level(self) -> ValidationIssueLevel:  # noqa: D
        return ValidationIssueLevel.FUTURE_ERROR

    def as_readable_str(self, verbose: bool = False, prefix: Optional[str] = None) -> str:
        """Return a easily readable string that can be used to log the issue."""
        return (
            f"{super().as_readable_str(verbose=verbose, prefix=prefix)}"
            f"IMPORTANT: this error will break your model starting {self.error_date.strftime('%b %d, %Y')}. "
        )


class ValidationError(ValidationIssue, BaseModel):
    """An error that was found while validating the model."""

    @property
    def level(self) -> ValidationIssueLevel:  # noqa: D
        return ValidationIssueLevel.ERROR


class ModelValidationResults(FrozenBaseModel):
    """Class for organizating the results of running validations."""

    warnings: Tuple[ValidationWarning, ...] = tuple()
    future_errors: Tuple[ValidationFutureError, ...] = tuple()
    errors: Tuple[ValidationError, ...] = tuple()

    @property
    def has_blocking_issues(self) -> bool:
        """Does the ModelValidationResults have ERROR issues."""
        return len(self.errors) != 0

    @classmethod
    def from_issues_sequence(cls, issues: Sequence[ValidationIssue]) -> ModelValidationResults:
        """Constructs a ModelValidationResults class from a list of ValidationIssues."""
        warnings: List[ValidationWarning] = []
        future_errors: List[ValidationFutureError] = []
        errors: List[ValidationError] = []

        for issue in issues:
            if issue.level is ValidationIssueLevel.WARNING:
                warnings.append(issue)
            elif issue.level is ValidationIssueLevel.FUTURE_ERROR:
                future_errors.append(issue)
            elif issue.level is ValidationIssueLevel.ERROR:
                errors.append(issue)
            else:
                assert_values_exhausted(issue.level)
        return cls(warnings=tuple(warnings), future_errors=tuple(future_errors), errors=tuple(errors))

    @classmethod
    def merge(cls, results: Sequence[ModelValidationResults]) -> ModelValidationResults:
        """Creates a new ModelValidatorResults instance from multiple instances.

        This is useful when there are multiple validators that are run and the
        combined results are desireable. For instance there is a ModelValidator
        and a DataWarehouseModelValidator. These both return validation issues.
        If it's desireable to combine the results, the following makes it easy.
        """
        if not isinstance(results, List):
            results = list(results)

        # this nested comprehension syntax is a little disorienting
        # basically [element for object in list_of_objects for element in object.list_property]
        # translates to "for each element in an object's list for each object in a list of objects"
        warnings = tuple(issue for result in results for issue in result.warnings)
        future_errors = tuple(issue for result in results for issue in result.future_errors)
        errors = tuple(issue for result in results for issue in result.errors)

        return cls(
            warnings=warnings,
            future_errors=future_errors,
            errors=errors,
        )

    @property
    def all_issues(self) -> Tuple[ValidationIssue, ...]:
        """For when a singular list of issues is needed."""
        return self.errors + self.future_errors + self.warnings

    def summary(self) -> str:
        """Returns a stylized summary string for issues."""
        errors = click.style(
            text=f"{ValidationIssueLevel.ERROR.name_plural}: {len(self.errors)}",
            fg=ISSUE_COLOR_MAP[ValidationIssueLevel.ERROR],
        )
        future_erros = click.style(
            text=f"{ValidationIssueLevel.FUTURE_ERROR.name_plural}: {len(self.future_errors)}",
            fg=ISSUE_COLOR_MAP[ValidationIssueLevel.FUTURE_ERROR],
        )
        warnings = click.style(
            text=f"{ValidationIssueLevel.WARNING.name_plural}: {len(self.warnings)}",
            fg=ISSUE_COLOR_MAP[ValidationIssueLevel.WARNING],
        )
        return f"{errors}, {future_erros}, {warnings}"


def generate_exception_issue(
    what_was_being_done: str, e: Exception, context: Optional[ValidationContext] = None, extras: Dict[str, str] = {}
) -> ValidationIssue:
    """Generates a validation issue for exceptions."""
    if "stacktrace" not in extras:
        extras["stacktrace"] = "".join(traceback.format_tb(e.__traceback__))

    return ValidationError(
        context=context,
        message=f"An error occured while {what_was_being_done} - "
        f"{''.join(traceback.format_exception_only(etype=type(e), value=e))}",
        extra_detail="\n".join([f"{key}: {value}" for key, value in extras.items()]),
    )


def _func_args_to_string(*args: Any, **kwargs: Any) -> str:  # type: ignore
    return f"positional args: {args}, key word args: {kwargs}"


def validate_safely(whats_being_done: str) -> Callable:
    """Decorator to safely run validation checks."""

    def decorator_check_element_safely(func: Callable) -> Callable:  # noqa
        @functools.wraps(func)
        def wrapper(*args: Any, **kwargs: Any) -> List[ValidationIssue]:  # type: ignore
            """Safely run a check on model elements."""
            issues: List[ValidationIssue]
            try:
                issues = func(*args, **kwargs)
            except Exception as e:
                arguments_str = _func_args_to_string(*args, **kwargs)
                issues = [
                    generate_exception_issue(
                        what_was_being_done=whats_being_done,
                        e=e,
                        extras={"method_name": func.__name__, "passed_args": arguments_str},
                    )
                ]
            return issues

        return wrapper

    return decorator_check_element_safely


@dataclass(frozen=True)
class DimensionInvariants:
    """Helper object to ensure consistent dimension attributes across semantic models.

    All dimensions with a given name in all semantic models should have attributes matching these values.
    """

    type: DimensionType
    is_partition: bool


class ModelValidationRule(ABC):
    """Encapsulates logic for checking the values of objects in a model."""

    @classmethod
    @abstractmethod
    def validate_model(cls, model: SemanticManifest) -> List[ValidationIssue]:
        """Check the given model and return a list of validation issues."""
        pass

    @classmethod
    def validate_model_serialized_for_multiprocessing(cls, serialized_model: str) -> str:
        """Validate a model serialized via Pydantic's .json() method, and return a list of JSON serialized issues.

        This method exists because our validations are forked into parallel processes via
        multiprocessing.ProcessPoolExecutor, and passing a model or validation results object can result in
        idiosyncratic behavior and inscrutable errors due to interactions between pickling and pydantic objects.
        """
        return ModelValidationResults.from_issues_sequence(
            cls.validate_model(SemanticManifest.parse_raw(serialized_model))
        ).json()


class ModelValidationException(Exception):
    """Exception raised when validation of a model fails."""

    def __init__(self, issues: Tuple[ValidationIssue, ...]) -> None:  # noqa: D
        issues_str = "\n".join([x.as_readable_str(verbose=True) for x in issues])
        super().__init__(f"Error validating model. Issues:\n{issues_str}")
