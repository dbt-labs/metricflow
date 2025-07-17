from __future__ import annotations


class MetricflowException(Exception):
    """Base class for custom exceptions in MF."""

    pass


class MetricflowInternalError(MetricflowException):
    """A non-recoverable error due to an issue within MF and not caused by the user.."""

    pass


class InvalidManifestException(MetricflowException):
    """Raised when an invalid manifest is detected.

    Generally, a semantic manifest is validated before it is passed into the engine. This is useful to raise in
    sanity checks done outside of validations.
    """

    pass


class GraphvizException(MetricflowException):
    """Raised when there is an error when calling `graphviz` methods."""
