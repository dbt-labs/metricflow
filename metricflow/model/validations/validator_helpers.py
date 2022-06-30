from __future__ import annotations

import functools
import traceback
from abc import ABC, abstractmethod
from dataclasses import dataclass
from datetime import date
from enum import Enum
from pydantic import BaseModel, Extra
from typing import Any, Callable, Dict, List, Optional, Sequence, Tuple, Union

from metricflow.model.objects.elements.dimension import DimensionType
from metricflow.model.objects.user_configured_model import UserConfiguredModel
from metricflow.model.objects.utils import FrozenBaseModel
from metricflow.object_utils import assert_values_exhausted

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
    # Issue is blocking and further validation should be stopped.
    FATAL = 3


class ModelObjectType(Enum):
    """Maps object types in the models to a readable string."""

    DATA_SOURCE = "data_source"
    MATERIALIZATION = "materialization"
    MEASURE = "measure"
    DIMENSION = "dimension"
    IDENTIFIER = "identifier"
    METRIC = "metric"


class FileContext(BaseModel):
    """The base context class for validation issues"""

    file_name: Optional[str]
    line_number: Optional[int]

    class Config:
        """Pydantic class configuration options"""

        extra = Extra.forbid

    def context_str(self) -> str:
        """Human readable stringified representation of the context"""

        context_string = ""

        if self.file_name:
            context_string += f"in file `{self.file_name}`"
            if self.line_number:
                context_string += f" on line #{self.line_number}"

        return context_string


class MaterializationContext(BaseModel):
    """The context class for validation issues involving materializations"""

    file_context: FileContext
    materialization_name: str

    def context_str(self) -> str:
        """Human readable stringified representation of the context"""
        return f"with materialization `{self.materialization_name}` {self.file_context.context_str()}"


class MetricContext(BaseModel):
    """The context class for validation issues involving metrics"""

    file_context: FileContext
    metric_name: str

    def context_str(self) -> str:
        """Human readable stringified representation of the context"""
        return f"with metric `{self.metric_name}` {self.file_context.context_str()}"


class DataSourceContext(BaseModel):
    """The context class for validation issues involving data sources"""

    file_context: FileContext
    data_source_name: str

    def context_str(self) -> str:
        """Human readable stringified representation of the context"""
        return f"with data source `{self.data_source_name}` {self.file_context.context_str()}"


class DimensionContext(BaseModel):
    """The context class for validation issues involving dimensions"""

    file_context: FileContext
    data_source_name: str
    dimension_name: str

    def context_str(self) -> str:
        """Human readable stringified representation of the context"""
        return f"with dimension `{self.dimension_name}` in data source `{self.data_source_name}` {DataSourceContext.context_str(self)}"


class IdentifierContext(BaseModel):
    """The context class for validation issues involving indentifiers"""

    file_context: FileContext
    data_source_name: str
    identifier_name: str

    def context_str(self) -> str:
        """Human readable stringified representation of the context"""
        return f"with identifier `{self.identifier_name}` in data source `{self.data_source_name}` {DataSourceContext.context_str(self)}"


class MeasureContext(BaseModel):
    """The context class for validation issues involving measures"""

    file_context: FileContext
    data_source_name: str
    measure_name: str

    def context_str(self) -> str:
        """Human readable stringified representation of the context"""
        return f"with measure `{self.measure_name}` in data source `{self.data_source_name}` {DataSourceContext.context_str(self)}"


ValidationContext = Union[
    FileContext,
    MaterializationContext,
    MetricContext,
    DimensionContext,
    MeasureContext,
    IdentifierContext,
    DataSourceContext,
]


class ValidationIssue(ABC, BaseModel):
    """The abstract base ValidationIsssue class that the specific ValidationIssue classes are built from"""

    message: str
    context: Optional[ValidationContext] = None

    @property
    @abstractmethod
    def level(self) -> ValidationIssueLevel:
        """The level of of ValidationIssue"""

        raise NotImplementedError

    def as_readable_str(self, with_level: bool = True) -> str:
        """Return a easily readable string that can be used to log the issue."""
        msg_base = f"{self.level.name} " if with_level else ""
        msg_base += self.context.context_str() if self.context else ""
        if msg_base:
            msg_base += " - "
        return msg_base + self.message


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

    def as_readable_str(self, with_level: bool = True) -> str:
        """Return a easily readable string that can be used to log the issue."""
        return (
            f"{ValidationIssue.as_readable_str(self, with_level=with_level)}"
            f"IMPORTANT: this error will break your model starting {self.error_date.strftime('%b %d, %Y')}. "
        )


class ValidationError(ValidationIssue, BaseModel):
    """An error that was found while validating the model."""

    @property
    def level(self) -> ValidationIssueLevel:  # noqa: D
        return ValidationIssueLevel.ERROR


class ValidationFatal(ValidationIssue, BaseModel):
    """A fatal issue that was found while validating the model."""

    @property
    def level(self) -> ValidationIssueLevel:  # noqa: D
        return ValidationIssueLevel.FATAL


ValidationIssueType = Union[ValidationWarning, ValidationFutureError, ValidationError, ValidationFatal]


class ModelValidationResults(FrozenBaseModel):
    """Class for organizating the results of running validations"""

    warnings: Tuple[ValidationWarning, ...] = tuple()
    future_errors: Tuple[ValidationFutureError, ...] = tuple()
    errors: Tuple[ValidationError, ...] = tuple()
    fatals: Tuple[ValidationFatal, ...] = tuple()

    @property
    def has_blocking_issues(self) -> bool:
        """Does the ModelValidationResults have ERROR or FATAL issues"""
        return len(self.errors) != 0 or len(self.fatals) != 0

    @classmethod
    def from_issues_sequence(cls, issues: Sequence[ValidationIssueType]) -> ModelValidationResults:
        """Constructs a ModelValidationResults class from a list of ValidationIssues"""

        warnings: List[ValidationWarning] = []
        future_errors: List[ValidationFutureError] = []
        errors: List[ValidationError] = []
        fatals: List[ValidationFatal] = []

        for issue in issues:
            if issue.level == ValidationIssueLevel.WARNING:
                warnings.append(issue)
            elif issue.level == ValidationIssueLevel.FUTURE_ERROR:
                future_errors.append(issue)
            elif issue.level == ValidationIssueLevel.ERROR:
                errors.append(issue)
            elif issue.level == ValidationIssueLevel.FATAL:
                fatals.append(issue)
            else:
                assert_values_exhausted(issue.level)
        return cls(
            warnings=tuple(warnings), future_errors=tuple(future_errors), errors=tuple(errors), fatals=tuple(fatals)
        )

    @classmethod
    def merge(cls, results: Sequence[ModelValidationResults]) -> ModelValidationResults:
        """Creates a new ModelValidatorResults instance from multiple instances

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
        fatals = tuple(issue for result in results for issue in result.fatals)

        return cls(
            warnings=warnings,
            future_errors=future_errors,
            errors=errors,
            fatals=fatals,
        )

    @property
    def all_issues(self) -> Tuple[ValidationIssueType, ...]:
        """For when a singular list of issues is needed"""
        return self.fatals + self.errors + self.future_errors + self.warnings


def generate_exception_issue(
    what_was_being_done: str,
    e: Exception,
    context: Optional[ValidationContext] = None,
) -> ValidationIssue:
    """Generates a validation issue for exceptions"""
    return ValidationError(
        context=context,
        message=f"An error occured while {what_was_being_done} - {''.join(traceback.format_exception(etype=type(e), value=e, tb=e.__traceback__))}",
    )


def _func_args_to_string(*args: Any, **kwargs: Any) -> str:  # type: ignore
    return f"positional args: {args}, key word args: {kwargs}"


def validate_safely(whats_being_done: str) -> Callable:
    """Decorator to safely run validation checks"""

    def decorator_check_element_safely(func: Callable) -> Callable:  # noqa
        @functools.wraps(func)
        def wrapper(*args: Any, **kwargs: Any) -> List[ValidationIssueType]:  # type: ignore
            """Safely run a check on model elements"""
            issues: List[ValidationIssueType]
            try:
                issues = func(*args, **kwargs)
            except Exception as e:
                arguments_str = _func_args_to_string(*args, **kwargs)
                issues = [
                    generate_exception_issue(
                        what_was_being_done=whats_being_done
                        + VALIDATE_SAFELY_ERROR_STR_TMPLT.format(
                            method_name=func.__name__, arguments_str=arguments_str
                        ),
                        e=e,
                    )
                ]
            return issues

        return wrapper

    return decorator_check_element_safely


@dataclass(frozen=True)
class DimensionInvariants:
    """Helper object to ensure consistent dimension attributes across data sources.

    All dimensions with a given name in all data sources should have attributes matching these values.
    """

    type: DimensionType
    is_partition: bool


class ModelValidationRule(ABC):
    """Encapsulates logic for checking the values of objects in a model."""

    @staticmethod
    @abstractmethod
    def validate_model(model: UserConfiguredModel) -> List[ValidationIssueType]:
        """Check the given model and return a list of validation issues"""
        pass


class ModelValidationException(Exception):
    """Exception raised when validation of a model fails."""

    def __init__(self, issues: Tuple[ValidationIssueType, ...]) -> None:  # noqa: D
        issues_str = "\n".join([x.as_readable_str() for x in issues])
        super().__init__(f"Error validating model. Issues:\n{issues_str}")
