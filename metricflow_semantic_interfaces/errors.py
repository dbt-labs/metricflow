from __future__ import annotations

from typing import Optional

from metricflow_semantic_interfaces.parsing.yaml_loader import ParsingContext


class ConstraintParseException(Exception):  # noqa: D101
    pass


class ParsingException(Exception):  # noqa: D101
    def __init__(  # noqa: D107
        self, message: str, ctx: Optional[ParsingContext] = None, config_filepath: Optional[str] = None
    ) -> None:
        if config_filepath:
            message = f"Failed to parse YAML file '{config_filepath}' - {message}"
        if ctx:
            message = f"{message}\nContext: {str(ctx)}"
        super().__init__(message)


class ModelTransformError(Exception):
    """Exception to represent errors related to model transformations."""

    pass


class InvalidQuerySyntax(Exception):
    """Raised when query syntax is invalid."""

    def __init__(self, msg: str) -> None:  # noqa: D107
        super().__init__(msg)
