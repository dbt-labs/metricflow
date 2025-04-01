from __future__ import annotations

import textwrap
from typing import Dict, Optional


class CustomerFacingSemanticException(Exception):
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


class SemanticException(Exception):  # noqa: D101
    pass


class DuplicateMetricError(SemanticException):  # noqa: D101
    pass


class MetricNotFoundError(SemanticException, KeyError):  # noqa: D101
    pass


class NonExistentMeasureError(SemanticException):  # noqa: D101
    pass


class InvalidSemanticModelError(SemanticException):  # noqa: D101
    pass


class ExecutionException(Exception):
    """Raised if there are any errors while executing the execution plan."""

    pass


class InferenceError(Exception):
    """Exception to represent errors related to inference."""


class UnsupportedEngineFeatureError(RuntimeError):
    """Raised when the user attempts to use a feature that isn't supported by the data platform."""


class SqlBindParametersNotSupportedError(Exception):
    """Raised when a SqlClient that does not have support for bind parameters receives a non-empty set of params."""


class UnknownMetricLinkingError(ValueError):
    """Raised during linking when a user attempts to use a metric that isn't specified."""


class InvalidQuerySyntax(Exception):
    """Raised when query syntax is invalid. Primarily used in the where clause."""

    def __init__(self, msg: str) -> None:  # noqa: D107
        super().__init__(msg)
