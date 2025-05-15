from __future__ import annotations


class MetricflowException(Exception):
    """Base class for all exceptions in MF."""

    pass


class MetricflowAssertionError(MetricflowException):
    """Raised when a code assumption is violated."""

    pass
