from __future__ import annotations

import textwrap
from collections.abc import Sequence
from typing import Dict, Optional


class MetricFlowException(Exception):
    """Base class for custom exceptions in MF."""

    pass


class MetricFlowInternalError(MetricFlowException):
    """A non-recoverable error due to an issue within MF and not caused by the user.."""

    pass


class InvalidManifestException(MetricFlowException):
    """Raised when an invalid manifest is detected.

    Generally, a semantic manifest is validated before it is passed into the engine. This is useful to raise in
    sanity checks done outside of validations.
    """

    pass


class GraphvizException(MetricFlowException):
    """Raised when there is an error when calling `graphviz` methods."""


class InformativeUserError(MetricFlowException):
    """Raised for user errors.

    The error is actionable by the user or provides user userful information
    as to why it's unactionable (eg., Feature isn't supported)
    """

    pass


class CustomerFacingSemanticException(InformativeUserError):
    """Class of Exceptions that make it to the customer's eyeballs."""

    pass


class UnableToSatisfyQueryError(CustomerFacingSemanticException):  # noqa: D101
    def __init__(self, error_name: str, context: Optional[Dict[str, str]] = None) -> None:
        """Context will be printed as list of items when this is converted to a string."""
        self.error_name = error_name
        self._context = context

    def __str__(self) -> str:  # noqa: D105
        error_lines = ["Unable To Satisfy Query Error: " + self.error_name]
        if self._context:
            for key, value in self._context.items():
                error_lines.append(f"\n{key}:")
                error_lines.append(f"{textwrap.indent(value, prefix='    ')}")
        return "\n".join(error_lines)


class SemanticException(MetricFlowException):  # noqa: D101
    pass


class DuplicateMetricError(SemanticException):  # noqa: D101
    pass


class MetricNotFoundError(SemanticException, KeyError):  # noqa: D101
    pass


class InvalidSemanticModelError(SemanticException):  # noqa: D101
    pass


class ExecutionException(MetricFlowException):
    """Raised if there are any errors while executing the execution plan."""

    pass


class UnsupportedEngineFeatureError(InformativeUserError, RuntimeError):
    """Raised when the user attempts to use a feature that isn't supported by the data platform."""


class SqlBindParametersNotSupportedError(MetricFlowException):
    """Raised when a SqlClient that does not have support for bind parameters receives a non-empty set of params."""


class UnknownMetricError(InformativeUserError):
    """Raised when user input contains metric names that are not known."""

    def __init__(self, metric_names: Sequence[str]) -> None:  # noqa: D107
        name_count = len(metric_names)
        if name_count == 0:
            raise RuntimeError(f"Can't create an {self.__class__.__name__} without metric names")
        elif name_count == 1:
            super().__init__(f"Unknown metric: {repr(metric_names[0])}")
        else:
            super().__init__(f"Unknown metrics: {list(metric_names)}")


class InvalidQuerySyntax(InformativeUserError):
    """Raised when query syntax is invalid. Primarily used in the where clause."""

    def __init__(self, msg: str) -> None:  # noqa: D107
        super().__init__(msg)


class InvalidQueryException(InformativeUserError):
    """Exception thrown when there is an error with the parameters to a MF query."""

    pass


class RenderSqlTemplateException(InformativeUserError):
    """Exception thrown when there is an error rendering a SQL template."""

    pass


class FeatureNotSupportedError(InformativeUserError):
    """Exception thrown when a feature is not implemented."""

    pass


class SemanticManifestConfigurationError(InformativeUserError):
    """Exception thrown when the semantic manifest is not configured correctly."""

    pass
