from typing import Optional

from dbt_semantic_interfaces.parsing.yaml_loader import ParsingContext


class ConstraintParseException(Exception):  # noqa: D
    pass


class ParsingException(Exception):  # noqa: D
    def __init__(  # noqa: D
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
