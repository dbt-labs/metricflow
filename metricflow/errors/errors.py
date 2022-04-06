import textwrap
from typing import Dict, Optional

from metricflow.model.parsing.yaml_file import YamlFile
from metricflow.model.parsing.yaml_loader import ParsingContext


class CustomerFacingSemanticException(Exception):
    """Class of Exceptions that make it to the customer's eyeballs"""

    pass


class UnableToSatisfyQueryError(CustomerFacingSemanticException):  # noqa:D
    def __init__(self, error_name: str, context: Dict[str, str] = None) -> None:  # noqa:D
        """Context will be printed as list of items when this is converted to a string."""
        self.error_name = error_name
        self._context = context

    def __str__(self) -> str:  # noqa:D
        error_lines = ["Unable To Satisfy Query Error: " + self.error_name]
        if self._context:
            for key, value in self._context.items():
                error_lines.append(f"\n{key}:")
                error_lines.append(f"{textwrap.indent(value, prefix='    ')}")
        return "\n".join(error_lines)


class SemanticException(Exception):  # noqa:D
    pass


class DuplicateMetricError(SemanticException):  # noqa:D
    pass


class MaterializationNotFoundError(SemanticException, KeyError):  # noqa:D
    pass


class MetricNotFoundError(SemanticException, KeyError):  # noqa:D
    pass


class NonExistentMeasureError(SemanticException):  # noqa:D
    pass


class InvalidDataSourceError(SemanticException):  # noqa:D
    pass


class ConstraintParseException(Exception):  # noqa: D
    pass


class ParsingException(Exception):  # noqa: D
    def __init__(  # noqa: D
        self, message: str, ctx: Optional[ParsingContext] = None, config_yaml: Optional[YamlFile] = None
    ) -> None:
        if config_yaml:
            message = f"Failed to parse YAML file '{config_yaml.file_path}' - {message}"
        if ctx:
            message = f"{message}\nContext: {str(ctx)}"
        super().__init__(message)


class ExecutionException(Exception):
    """Raised if there are any errors while executing the execution plan"""

    pass
