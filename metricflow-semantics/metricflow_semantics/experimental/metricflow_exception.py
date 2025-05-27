from __future__ import annotations


class MetricflowException(Exception):
    """Base class for custom exceptions in MF."""

    pass


class MetricflowAssertionError(MetricflowException):
    """Raised when a code assumption is violated."""

    pass


class InvalidManifestException(MetricflowException):
    """Raised when an invalid manifest is provided."""

    pass
