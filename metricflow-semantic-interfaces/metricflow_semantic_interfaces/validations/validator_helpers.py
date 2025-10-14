from __future__ import annotations

import functools
import traceback
from abc import ABC, abstractmethod
from dataclasses import dataclass
from datetime import date
from enum import Enum
from typing import (
    Callable,
    Dict,
    Generic,
    Iterable,
    List,
    Optional,
    Sequence,
    Tuple,
    Union,
)

import click
from msi_pydantic_shim import BaseModel, Extra
from typing_extensions import ParamSpec

from metricflow_semantic_interfaces.implementations.base import FrozenBaseModel
from metricflow_semantic_interfaces.protocols import Metadata, SemanticManifestT, SemanticModel
from metricflow_semantic_interfaces.references import (
    MetricModelReference,
    SemanticModelElementReference,
    SemanticModelReference,
)
from metricflow_semantic_interfaces.type_enums import DimensionType

VALIDATE_SAFELY_ERROR_STR_TMPLT = ". Issue occurred in method `{method_name}` called with {arguments_str}"
ValidationContextJSON = Dict[str, Union[str, int, None]]
ValidationIssueJSON = Dict[str, Union[str, int, ValidationContextJSON]]

P = ParamSpec("P")


class ValidationIssueLevel(Enum):
    """Categorize the issues found while validating a semantic manifest."""

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
        """Human-readable stringified representation of the context."""
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


class ValidationIssueContext(BaseModel):
    """Generic validation Context."""

    file_context: FileContext
    object_type: str
    object_name: str

    def context_str(self) -> str:
        """Human-readable stringified representation of the context."""
        return f"with {self.object_type} `{self.object_name}` {self.file_context.context_str()}"


class MetricContext(BaseModel):
    """The context class for validation issues involving metrics."""

    file_context: FileContext
    metric: MetricModelReference

    def context_str(self) -> str:
        """Human-readable stringified representation of the context."""
        return f"with metric `{self.metric.metric_name}` {self.file_context.context_str()}"


class SemanticModelContext(BaseModel):
    """The context class for validation issues involving semantic models."""

    file_context: FileContext
    semantic_model: SemanticModelReference

    def context_str(self) -> str:
        """Human-readable stringified representation of the context."""
        return f"with semantic model `{self.semantic_model.semantic_model_name}` {self.file_context.context_str()}"


class SemanticModelElementContext(BaseModel):
    """The context class for validation issues involving dimensions."""

    file_context: FileContext
    semantic_model_element: SemanticModelElementReference
    element_type: SemanticModelElementType

    def context_str(self) -> str:
        """Human-readable stringified representation of the context."""
        return (
            f"with {self.element_type.value} `{self.semantic_model_element.element_name}` in semantic model "
            f"`{self.semantic_model_element.semantic_model_name}` {self.file_context.context_str()}"
        )


class SavedQueryElementType(Enum):
    """Maps the fields in a saved query to a readable string."""

    METRIC = "metric"
    GROUP_BY = "group by"
    WHERE = "where"
    ORDER_BY = "order by"
    LIMIT = "limit"


class SavedQueryContext(BaseModel):
    """Provides context on where a saved query was defined."""

    file_context: FileContext
    element_type: SavedQueryElementType
    element_value: str

    def context_str(self) -> str:
        """Human-readable stringified representation of the context."""
        return (
            f"with a {self.element_type.value} in saved query `{self.element_type.value}` "
            f"{self.file_context.context_str()}"
        )


ValidationContext = Union[
    FileContext,
    MetricContext,
    SemanticModelContext,
    SemanticModelElementContext,
    SavedQueryContext,
    ValidationIssueContext,
]


class ValidationIssue(ABC, BaseModel):
    """The abstract base ValidationIssue class that the specific ValidationIssue classes are built from."""

    message: str
    context: Optional[ValidationContext] = None
    extra_detail: Optional[str] = None

    @property
    @abstractmethod
    def level(self) -> ValidationIssueLevel:
        """The level of ValidationIssue."""
        raise NotImplementedError

    def as_readable_str(self, verbose: bool = False, prefix: Optional[str] = None) -> str:
        """Return an easily readable string that can be used to log the issue."""
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

    @property
    @abstractmethod
    def as_issue_set(self) -> ValidationIssueSet:  # noqa: D102
        raise NotImplementedError


@dataclass(frozen=True)
class ValidationIssueSet:
    """Groups validation issues by type."""

    warning_issues: Sequence[ValidationWarning] = ()
    future_error_issues: Sequence[ValidationFutureError] = ()
    error_issues: Sequence[ValidationError] = ()

    @staticmethod
    def combine(validation_issue_sets: Iterable[ValidationIssueSet]) -> ValidationIssueSet:
        """Combine the given issues (no de-duping)."""
        combined_warning_issues: List[ValidationWarning] = []
        combined_future_error_issues: List[ValidationFutureError] = []
        combined_error_issues: List[ValidationError] = []

        for validation_issue_set in validation_issue_sets:
            combined_warning_issues.extend(validation_issue_set.warning_issues)
            combined_future_error_issues.extend(validation_issue_set.future_error_issues)
            combined_error_issues.extend(validation_issue_set.error_issues)

        return ValidationIssueSet(
            warning_issues=tuple(combined_warning_issues),
            future_error_issues=tuple(combined_future_error_issues),
            error_issues=tuple(combined_error_issues),
        )


class ValidationWarning(ValidationIssue, BaseModel):
    """A warning that was found while validating the model."""

    @property
    def level(self) -> ValidationIssueLevel:  # noqa: D102
        return ValidationIssueLevel.WARNING

    @property
    def as_issue_set(self) -> ValidationIssueSet:  # noqa: D102
        return ValidationIssueSet(warning_issues=(self,))


class ValidationFutureError(ValidationIssue, BaseModel):
    """A future error that was found while validating the model."""

    error_date: date

    @property
    def level(self) -> ValidationIssueLevel:  # noqa: D102
        return ValidationIssueLevel.FUTURE_ERROR

    def as_readable_str(self, verbose: bool = False, prefix: Optional[str] = None) -> str:
        """Return an easily readable string that can be used to log the issue."""
        return (
            f"{super().as_readable_str(verbose=verbose, prefix=prefix)}"
            f"IMPORTANT: this error will break your model starting {self.error_date.strftime('%b %d, %Y')}. "
        )

    @property
    def as_issue_set(self) -> ValidationIssueSet:  # noqa: D102
        return ValidationIssueSet(future_error_issues=(self,))


class ValidationError(ValidationIssue, BaseModel):
    """An error that was found while validating the model."""

    @property
    def level(self) -> ValidationIssueLevel:  # noqa: D102
        return ValidationIssueLevel.ERROR

    @property
    def as_issue_set(self) -> ValidationIssueSet:  # noqa: D102
        return ValidationIssueSet(error_issues=(self,))


class SemanticManifestValidationResults(FrozenBaseModel):
    """Class for organizing the results of running validations."""

    warnings: Tuple[ValidationWarning, ...] = tuple()
    future_errors: Tuple[ValidationFutureError, ...] = tuple()
    errors: Tuple[ValidationError, ...] = tuple()

    @property
    def has_blocking_issues(self) -> bool:
        """Does the SemanticManifestValidationResults have ERROR issues."""
        return len(self.errors) != 0

    @staticmethod
    def from_issues_sequence(issues: Sequence[ValidationIssue]) -> SemanticManifestValidationResults:
        """Constructs a SemanticManifestValidationResults class from a list of ValidationIssues."""
        combined_issue_set = ValidationIssueSet.combine(tuple(issue.as_issue_set for issue in issues))
        return SemanticManifestValidationResults(
            warnings=tuple(combined_issue_set.warning_issues),
            future_errors=tuple(combined_issue_set.future_error_issues),
            errors=tuple(combined_issue_set.error_issues),
        )

    @classmethod
    def merge(cls, results: Sequence[SemanticManifestValidationResults]) -> SemanticManifestValidationResults:
        """Creates a new ModelValidatorResults instance from multiple instances.

        This is useful when there are multiple validators that are run and the
        combined results are desirable. For instance there is a SemanticManifestValidator
        and a DataWarehouseModelValidator. These both return validation issues.
        If it's desirable to combine the results, the following makes it easy.
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
        future_errors = click.style(
            text=f"{ValidationIssueLevel.FUTURE_ERROR.name_plural}: {len(self.future_errors)}",
            fg=ISSUE_COLOR_MAP[ValidationIssueLevel.FUTURE_ERROR],
        )
        warnings = click.style(
            text=f"{ValidationIssueLevel.WARNING.name_plural}: {len(self.warnings)}",
            fg=ISSUE_COLOR_MAP[ValidationIssueLevel.WARNING],
        )
        return f"{errors}, {future_errors}, {warnings}"


def generate_exception_issue(
    what_was_being_done: str,
    e: Exception,
    context: Optional[ValidationContext] = None,
    extras: Optional[Dict[str, str]] = None,
) -> ValidationIssue:
    """Generates a validation issue for exceptions."""
    if extras is None:
        extras = {}

    if "stacktrace" not in extras:
        extras["stacktrace"] = "".join(traceback.format_tb(e.__traceback__))

    return ValidationError(
        context=context,
        message=f"An error occurred while {what_was_being_done} - "
        f"{''.join(traceback.format_exception_only(type(e), value=e))}",
        extra_detail="\n".join([f"{key}: {value}" for key, value in extras.items()]),
    )


def _func_args_to_string(*args: P.args, **kwargs: P.kwargs) -> str:  # type: ignore
    return f"positional args: {args}, key word args: {kwargs}"


def validate_safely(
    whats_being_done: str,
) -> Callable[[Callable[P, Sequence[ValidationIssue]]], Callable[P, Sequence[ValidationIssue]]]:
    """Decorator to safely run validation checks."""

    def decorator_check_element_safely(
        func: Callable[P, Sequence[ValidationIssue]],
    ) -> Callable[P, Sequence[ValidationIssue]]:
        @functools.wraps(func)
        def wrapper(*args: P.args, **kwargs: P.kwargs) -> Sequence[ValidationIssue]:  # type: ignore
            """Safely run a check on model elements."""
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


class SemanticManifestValidationRule(ABC, Generic[SemanticManifestT]):
    """Encapsulates logic for checking the values of objects in a manifest."""

    @classmethod
    @abstractmethod
    def validate_manifest(cls, semantic_manifest: SemanticManifestT) -> Sequence[ValidationIssue]:
        """Check the given manifest and return a list of validation issues."""
        pass


class SemanticManifestValidationException(Exception):
    """Exception raised when validation of a model fails."""

    def __init__(self, issues: Tuple[ValidationIssue, ...]) -> None:  # noqa: D107
        issues_str = "\n".join([x.as_readable_str(verbose=True) for x in issues])
        super().__init__(f"Error validating model. Issues:\n{issues_str}")


class SemanticModelValidationHelpers:
    """Class containing all the helpers related to semantic model validations."""

    @staticmethod
    def time_dimension_in_model(time_dimension_name: str, semantic_model: SemanticModel) -> bool:  # noqa: D102
        for dimension in semantic_model.dimensions:
            if dimension.type == DimensionType.TIME and dimension.name == time_dimension_name:
                return True
        return False
