from __future__ import annotations

import functools
import traceback
from abc import ABC, abstractmethod
from collections import OrderedDict
from dataclasses import dataclass
from datetime import date
from enum import Enum
from typing import Any, Callable, List, Optional, Tuple, Union

from metricflow.model.objects.elements.dimension import DimensionType
from metricflow.model.objects.user_configured_model import UserConfiguredModel

VALIDATE_SAFELY_ERROR_STR_TMPLT = ". Issue occurred in method `{method_name}` called with {arguments_str}"


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


@dataclass(unsafe_hash=True)
class ValidationIssue:
    """An issue that was found while validating the MetricFlow model."""

    level: ValidationIssueLevel
    # Need a better way to represent the object, but right now this is a dict to the type of object to the name,
    # inserted in hierarchical order.
    # e.g. {"data_source": "example_data_source", "measure": "example_measure"}
    model_object_reference: OrderedDict[ModelObjectType, str]
    message: str
    # Consider adding a enum here that categories the type of validation issue and standardize the error messages.

    def as_readable_str(self) -> str:
        """Return a easily readable string that can be used to log the issue."""
        object_str = "{" + ", ".join([f"{k.value}: {v}" for k, v in self.model_object_reference.items()]) + "}"
        return f"[{self.level.name}] Object: {object_str} - {self.message}"

    @staticmethod
    def make_object_reference(
        data_source_name: Optional[str] = None,
        materialization_name: Optional[str] = None,
        dimension_name: Optional[str] = None,
        identifier_name: Optional[str] = None,
        measure_name: Optional[str] = None,
        metric_name: Optional[str] = None,
        object_type: Optional[ModelObjectType] = None,
        object_name: Optional[str] = None,
    ) -> OrderedDict[ModelObjectType, str]:
        """Convenience function for making the object reference dict."""

        model_object: OrderedDict[ModelObjectType, str] = OrderedDict()
        if data_source_name:
            if object_type == ModelObjectType.DATA_SOURCE:
                raise RuntimeError("Conflicting object types - multiple data sources")
            model_object[ModelObjectType.DATA_SOURCE] = data_source_name
        if materialization_name:
            if object_type == ModelObjectType.MATERIALIZATION:
                raise RuntimeError("Conflicting object types - multiple materializations")
            model_object[ModelObjectType.MATERIALIZATION] = materialization_name
        if metric_name:
            if object_type == ModelObjectType.METRIC:
                raise RuntimeError("Conflicting object types - multiple metrics")
            model_object[ModelObjectType.METRIC] = metric_name
        if dimension_name:
            if object_type == ModelObjectType.DIMENSION:
                raise RuntimeError("Conflicting object types - multiple dimensions")
            model_object[ModelObjectType.DIMENSION] = dimension_name
        if identifier_name:
            if object_type == ModelObjectType.IDENTIFIER:
                raise RuntimeError("Conflicting object types - multiple dimensions")
            model_object[ModelObjectType.IDENTIFIER] = identifier_name
        if measure_name:
            if object_type == ModelObjectType.MEASURE:
                raise RuntimeError("Conflicting object types - multiple dimensions")
            model_object[ModelObjectType.MEASURE] = measure_name

        if object_type:
            if not object_name:
                raise RuntimeError("Object name must be specified with object type.")
            model_object[object_type] = object_name

        if len(model_object) == 0:
            raise RuntimeError("No object parameters were passed in.")

        return model_object


@dataclass(unsafe_hash=True)
class ValidationWarning(ValidationIssue):
    """A warning that was found while validation the model."""

    def __init__(self, model_object_reference: OrderedDict[ModelObjectType, str], message: str):
        """Initializes with super (ValidationIssue) with hardcoded level of WARNING"""
        super().__init__(
            level=ValidationIssueLevel.WARNING, model_object_reference=model_object_reference, message=message
        )


@dataclass(unsafe_hash=True)
class ValidationFutureError(ValidationIssue):
    """A future error that was found while validation the model."""

    error_date: date

    def __init__(self, model_object_reference: OrderedDict[ModelObjectType, str], message: str, error_date: date):
        """Calls super (ValidationIssue) with hardcoded level of FUTURE_ERROR"""
        # Special way to set error_date because we're in a frozen dataclass
        object.__setattr__(self, "error_date", error_date)
        super().__init__(
            level=ValidationIssueLevel.FUTURE_ERROR,
            model_object_reference=model_object_reference,
            message=message,
        )

    def as_readable_str(self) -> str:
        """Return a easily readable string that can be used to log the issue."""
        object_str = "{" + ", ".join([f"{k.value}: {v}" for k, v in self.model_object_reference.items()]) + "}"
        return (
            f"[{self.level.name}] Object: {object_str} - {self.message}. "
            f"IMPORTANT: this error will break your model starting {self.error_date.strftime('%b %d, %Y')}. "
        )


@dataclass(unsafe_hash=True)
class ValidationError(ValidationIssue):
    """An error that was found while validating the model."""

    def __init__(self, model_object_reference: OrderedDict[ModelObjectType, str], message: str):
        """Calls super (ValidationIssue) with hardcoded level of ERROR"""
        super().__init__(
            level=ValidationIssueLevel.ERROR, model_object_reference=model_object_reference, message=message
        )


@dataclass(unsafe_hash=True)
class ValidationFatal(ValidationIssue):
    """A fatal issue that was found while validation the model."""

    def __init__(self, model_object_reference: OrderedDict[ModelObjectType, str], message: str):
        """Calls super (ValidationIssue) with hardcoded level of FATAL"""
        super().__init__(
            level=ValidationIssueLevel.FATAL, model_object_reference=model_object_reference, message=message
        )


ValidationIssueType = Union[ValidationIssue, ValidationWarning, ValidationFutureError, ValidationError, ValidationFatal]


def generate_exception_issue(
    obj_ref: OrderedDict[ModelObjectType, str],
    what_was_being_done: str,
    e: Exception,
) -> ValidationIssue:
    """Generates a validation issue for exceptions"""
    return ValidationError(
        model_object_reference=obj_ref,
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
                        obj_ref=OrderedDict(),
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
