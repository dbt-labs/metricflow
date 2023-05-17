from abc import ABC, abstractmethod
from collections import defaultdict
from dataclasses import dataclass, field
from typing import Any, Tuple, DefaultDict, Dict
from typing_extensions import TypeAlias

from dbt_metadata_client.dbt_metadata_api_schema import MetricNode, ModelNode
from dbt_semantic_interfaces.objects.metric import MetricType
from dbt_semantic_interfaces.validations.validator_helpers import ModelValidationResults


TransformedObjectsValueType: TypeAlias = Any  # type: ignore[misc]


@dataclass
class MappedObjects:
    """Model elements, and sub elements, mapped by element name path"""

    semantic_models: DefaultDict[str, Dict[str, TransformedObjectsValueType]] = field(
        default_factory=lambda: defaultdict(dict)
    )
    metrics: DefaultDict[str, Dict[str, TransformedObjectsValueType]] = field(default_factory=lambda: defaultdict(dict))
    # access path is ["semantic_model_name"]["dimension_name"] -> dict dimension representation
    dimensions: DefaultDict[str, DefaultDict[str, Dict[str, TransformedObjectsValueType]]] = field(
        default_factory=lambda: defaultdict(lambda: defaultdict(dict))
    )
    # access path is ["semantic_model_name"]["entity_name"] -> dict entity representation
    entities: DefaultDict[str, DefaultDict[str, Dict[str, TransformedObjectsValueType]]] = field(
        default_factory=lambda: defaultdict(lambda: defaultdict(dict))
    )
    # access path is ["semantic_model_name"]["measure_name"] -> dict measure representation
    measures: DefaultDict[str, DefaultDict[str, Dict[str, TransformedObjectsValueType]]] = field(
        default_factory=lambda: defaultdict(lambda: defaultdict(dict))
    )


@dataclass
class DbtMappingResults:  # noqa: D
    mapped_objects: MappedObjects
    validation_results: ModelValidationResults


def assert_metric_model_name(metric: MetricNode) -> None:
    """Asserts that a metric has a model and that model has a name

    We abstracted this into a function, because it is a common pattern
    in DbtMappingRules.
    """
    assert isinstance(
        metric.model, ModelNode
    ), f"Expected `ModelNode` for `{metric.name}` metric's `model`, got `{type(metric.model)}`"
    assert metric.model.name, f"Expected a `name` for `{metric.name}` metric's `model`, got `None`"


def assert_essential_metric_properties(metric: MetricNode) -> None:
    """Asserts that a dbt metric has the essential properties commonly needed when building metrics

    We abstracted this into a function, because it is a common pattern
    in DbtMappingRules.
    """
    assert metric.name, f"Expected a `name` for `{metric.name}` metric, got `None`"
    assert metric.calculation_method, f"Expected a `calculation_method` for `{metric.name}` metric, got `None`"


CALC_METHOD_TO_METRIC_TYPE: Dict[str, MetricType] = {
    "count": MetricType.MEASURE_PROXY,
    "count_distinct": MetricType.MEASURE_PROXY,
    "sum": MetricType.MEASURE_PROXY,
    "average": MetricType.MEASURE_PROXY,
    "min": MetricType.MEASURE_PROXY,
    "max": MetricType.MEASURE_PROXY,
    "derived": MetricType.DERIVED,
}


def get_and_assert_calc_method_mapping(dbt_metric: MetricNode) -> MetricType:
    """Helper method for getting the metric type for a dbt metric's calcuation_method

    Anytime we want to map from a dbt metric's calculation_method to a MetricFlow
    metric type, we need to assert that the mapping exists. This is because dbt
    will likely add more calculation_methods over time. In which case this will
    appropriately break and we'll have to extend the CALC_METHOD_TO_METRIC_TYPE.
    """
    metric_type = CALC_METHOD_TO_METRIC_TYPE.get(dbt_metric.calculation_method)
    assert (
        metric_type is not None
    ), f"Unknown dbt calculation method `{dbt_metric.calculation_method}` on dbt metric {dbt_metric.name}, unable to map to MetricFlow metric type"
    return metric_type


class DbtMappingRule(ABC):
    """Encapsulates logic for mapping a dbt manifest attributes to MetricFlow Model element attributes.

    A given mapping rule should be irrespective of any and all other
    DbtMappingRules. That is a rule should not depend on information in the
    passedMappedObjects of `run` to run properly and should not depend on
    another rule to clean things up.
    """

    @staticmethod
    @abstractmethod
    def run(dbt_metrics: Tuple[MetricNode, ...], objects: MappedObjects) -> ModelValidationResults:
        """Take in a MappedObjects object, update it in place given the metrics as determined by the rule. Return any issues"""
        pass
