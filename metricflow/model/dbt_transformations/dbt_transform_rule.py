from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from typing import Any, Dict, Tuple

from dbt_metadata_client.dbt_metadata_api_schema import MetricNode, ModelNode
from metricflow.model.validations.validator_helpers import ModelValidationResults


@dataclass
class DbtTransformedObjects:  # type: ignore[misc]
    """Model elements, and sub elements, mapped by element name path"""

    data_sources: Dict[str, Dict[str, Any]] = field(default_factory=dict)  # type: ignore[misc]
    metrics: Dict[str, Dict[str, Any]] = field(default_factory=dict)  # type: ignore[misc]
    materializations: Dict[str, Dict[str, Any]] = field(default_factory=dict)  # type: ignore[misc]
    # access path is ["data_source_name"]["dimension_name"] -> dict dimension representation
    dimensions: Dict[str, Dict[str, Dict[str, Any]]] = field(default_factory=dict)  # type: ignore[misc]
    # access path is ["data_source_name"]["identifier_name"] -> dict identifier representation
    identifiers: Dict[str, Dict[str, Dict[str, Any]]] = field(default_factory=dict)  # type: ignore[misc]
    # access path is ["data_source_name"]["measure_name"] -> dict measure representation
    measures: Dict[str, Dict[str, Dict[str, Any]]] = field(default_factory=dict)  # type: ignore[misc]

    def add_data_source_object_if_not_exists(self, name: str) -> None:
        """Checks if a mapping exists for the given data source name, and adds one if not found"""
        if self.data_sources.get(name) is None:
            self.data_sources[name] = {}


@dataclass
class DbtTransformationResult:  # noqa: D
    transformed_objects: DbtTransformedObjects
    validation_results: ModelValidationResults


def assert_metric_model_name(metric: MetricNode) -> None:
    """Asserts that a metric has a model and that model has a name

    We abstracted this into a function, because it is a common pattern
    in DbtTransformRules.
    """
    assert isinstance(
        metric.model, ModelNode
    ), f"Expected `ModelNode` for `{metric.name}` metric's `model`, got `{type(metric.model)}`"
    assert metric.model.name, f"Expected a `name` for `{metric.name}` metric's `model`, got `None`"


class DbtTransformRule(ABC):
    """Encapsulates logic for transforming a dbt manifest. e.g. add metrics based on measures."""

    @staticmethod
    @abstractmethod
    def run(dbt_metrics: Tuple[MetricNode, ...], objects: DbtTransformedObjects) -> ModelValidationResults:
        """Take in a DbtTransformedObjects object, transform it in place given the metrics and the rule, return any issues"""
        pass
