from __future__ import annotations

from abc import ABC
from dataclasses import dataclass
from enum import Enum
from typing import Optional

from dbt_semantic_interfaces.naming.keywords import METRIC_TIME_ELEMENT_NAME
from dbt_semantic_interfaces.references import (
    DimensionReference,
    EntityReference,
    MeasureReference,
    MetricReference,
)
from dbt_semantic_interfaces.type_enums import TimeGranularity
from typing_extensions import override

from metricflow_semantics.dag.id_prefix import IdPrefix, StaticIdPrefix
from metricflow_semantics.experimental.comparison import Comparable, ComparisonTuple
from metricflow_semantics.experimental.mf_graph.graph_node import DisplayableGraphNode
from metricflow_semantics.mf_logging.pretty_formattable import MetricFlowPrettyFormattable

#
# @dataclass(frozen=True, order=True)
# class SemanticGraphNodeComparisonKey:
#     node_type: str
#     comparison_str: Optional[str]


class SemanticGraphNode(DisplayableGraphNode["SemanticGraphNode"], Comparable, ABC):
    """A node in the semantic graph."""

    pass


@dataclass(frozen=True)
class MetricAttributeNode(SemanticGraphNode):
    """An entity attribute that is labeled as a metric."""

    @classmethod
    @override
    def id_prefix(cls) -> IdPrefix:
        return StaticIdPrefix.METRIC_ATTRIBUTE_NODE

    metric_reference: MetricReference

    @property
    @override
    def dot_label(self) -> str:
        return self.metric_reference.element_name

    @property
    @override
    def comparison_tuple(self) -> ComparisonTuple:
        return (self.metric_reference,)


@dataclass(frozen=True)
class MeasureAttributeNode(SemanticGraphNode):
    """An entity attribute that is labeled as a measure."""

    measure_reference: MeasureReference

    @classmethod
    @override
    def id_prefix(cls) -> IdPrefix:
        return StaticIdPrefix.MEASURE_ATTRIBUTE_NODE

    @property
    @override
    def dot_label(self) -> str:
        return f"MeasureAttribute({self.measure_reference.element_name!r})"

    @property
    @override
    def comparison_tuple(self) -> ComparisonTuple:
        return (self.measure_reference,)


@dataclass(frozen=True)
class DimensionAttributeNode(SemanticGraphNode):
    """An entity attribute that is labeled as a dimension."""

    dimension_reference: DimensionReference

    @classmethod
    @override
    def id_prefix(cls) -> IdPrefix:
        return StaticIdPrefix.DIMENSION_ATTRIBUTE_NODE

    @property
    @override
    def dot_label(self) -> str:
        return f"DimensionAttribute({self.dimension_reference.element_name!r})"

    @property
    @override
    def comparison_tuple(self) -> ComparisonTuple:
        return (self.dimension_reference,)


@dataclass(frozen=True)
class AssociativeEntityNode(SemanticGraphNode):
    dimension_reference: DimensionReference

    @classmethod
    @override
    def id_prefix(cls) -> IdPrefix:
        return StaticIdPrefix.ASSOCIATIVE_ENTITY_NODE

    @property
    @override
    def dot_label(self) -> str:
        return f"AssociativeEntity({self.dimension_reference.element_name!r})"

    @property
    @override
    def comparison_tuple(self) -> ComparisonTuple:
        return (self.dimension_reference,)


@dataclass(frozen=True)
class EntityNode(MetricFlowPrettyFormattable, SemanticGraphNode):
    """An entity in the entity-relationship diagram / semantic graph."""

    entity_reference: EntityReference

    @classmethod
    @override
    def id_prefix(cls) -> IdPrefix:
        return StaticIdPrefix.ENTITY_NODE

    @property
    @override
    def dot_label(self) -> str:
        return f"Entity({repr(self.entity_reference.element_name)})"

    @property
    @override
    def comparison_tuple(self) -> ComparisonTuple:
        return (self.entity_reference,)

    @property
    @override
    def pretty_format(self) -> Optional[str]:
        return f"{self.__class__.__name__}({repr(self.entity_reference.element_name)})"


class SpecialEntityEnum(Enum):
    METRIC_TIME = EntityNode(EntityReference(METRIC_TIME_ELEMENT_NAME))
    NANOSECOND = EntityNode(EntityReference(TimeGranularity.NANOSECOND.value))
    MICROSECOND = EntityNode(EntityReference(TimeGranularity.MICROSECOND.value))
    MILLISECOND = EntityNode(EntityReference(TimeGranularity.MILLISECOND.value))
    SECOND = EntityNode(EntityReference(TimeGranularity.SECOND.value))
    MINUTE = EntityNode(EntityReference(TimeGranularity.MINUTE.value))
    HOUR = EntityNode(EntityReference(TimeGranularity.HOUR.value))
    DAY = EntityNode(EntityReference(TimeGranularity.DAY.value))
    WEEK = EntityNode(EntityReference(TimeGranularity.WEEK.value))
    MONTH = EntityNode(EntityReference(TimeGranularity.MONTH.value))
    QUARTER = EntityNode(EntityReference(TimeGranularity.QUARTER.value))
    YEAR = EntityNode(EntityReference(TimeGranularity.YEAR.value))
