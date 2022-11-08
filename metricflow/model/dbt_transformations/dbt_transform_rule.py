from abc import ABC, abstractmethod
from collections import defaultdict
from dataclasses import dataclass, field
from typing import Any, Tuple, DefaultDict, Dict
from typing_extensions import TypeAlias

from dbt_metadata_client.dbt_metadata_api_schema import MetricNode, ModelNode
from metricflow.model.validations.validator_helpers import ModelValidationResults


TransformedObjectsValueType: TypeAlias = Any  # type: ignore[misc]


@dataclass
class DbtTransformedObjects:  # type: ignore[misc]
    """Model elements, and sub elements, mapped by element name path"""

    data_sources: DefaultDict[str, Dict[str, TransformedObjectsValueType]] = field(
        default_factory=lambda: defaultdict(dict)
    )
    metrics: DefaultDict[str, Dict[str, TransformedObjectsValueType]] = field(default_factory=lambda: defaultdict(dict))
    materializations: DefaultDict[str, Dict[str, TransformedObjectsValueType]] = field(
        default_factory=lambda: defaultdict(dict)
    )
    # access path is ["data_source_name"]["dimension_name"] -> dict dimension representation
    dimensions: DefaultDict[str, DefaultDict[str, Dict[str, TransformedObjectsValueType]]] = field(
        default_factory=lambda: defaultdict(lambda: defaultdict(dict))
    )
    # access path is ["data_source_name"]["identifier_name"] -> dict identifier representation
    identifiers: DefaultDict[str, DefaultDict[str, Dict[str, TransformedObjectsValueType]]] = field(
        default_factory=lambda: defaultdict(lambda: defaultdict(dict))
    )
    # access path is ["data_source_name"]["measure_name"] -> dict measure representation
    measures: DefaultDict[str, DefaultDict[str, Dict[str, TransformedObjectsValueType]]] = field(
        default_factory=lambda: defaultdict(lambda: defaultdict(dict))
    )


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


def assert_essential_metric_properties(metric: MetricNode) -> None:
    """Asserts that a dbt metric has the essential properties commonly needed when building metrics

    We abstracted this into a function, because it is a common pattern
    in DbtTransformRules.
    """
    assert metric.name, f"Expected a `name` for `{metric.name}` metric, got `None`"
    assert metric.calculation_method, f"Expected a `calculation_method` for `{metric.name}` metric, got `None`"


class DbtTransformRule(ABC):
    """Encapsulates logic for transforming a dbt manifest. e.g. add metrics based on measures."""

    @staticmethod
    @abstractmethod
    def run(dbt_metrics: Tuple[MetricNode, ...], objects: DbtTransformedObjects) -> ModelValidationResults:
        """Take in a DbtTransformedObjects object, transform it in place given the metrics and the rule, return any issues"""
        pass
