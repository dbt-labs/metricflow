from __future__ import annotations

import logging
from functools import cached_property
from typing import Optional, override

from dbt_semantic_interfaces.naming.keywords import METRIC_TIME_ELEMENT_NAME

from metricflow_semantics.experimental.mf_graph.comparable import Comparable, ComparisonKey
from metricflow_semantics.experimental.mf_graph.displayable_graph_element import MetricflowGraphLabel
from metricflow_semantics.experimental.mf_graph.formatting.graphviz_attributes import (
    DotNodeAttributeSet,
    GraphvizColor,
)
from metricflow_semantics.experimental.mf_graph.node_descriptor import MetricflowGraphNodeDescriptor
from metricflow_semantics.experimental.ordered_set import FrozenOrderedSet
from metricflow_semantics.experimental.semantic_graph.nodes.node_label import (
    AggregationLabel,
    DsiEntityLabel,
    DunderNameElementLabel,
    GroupByAttributeRootLabel,
    JoinFromLabel,
    MetricTimeLabel,
    TimeDimensionLabel,
)
from metricflow_semantics.experimental.semantic_graph.nodes.semantic_graph_node import (
    SemanticGraphNode,
    SemanticGraphNodeVisitor,
)
from metricflow_semantics.experimental.singleton_decorator import singleton_dataclass
from metricflow_semantics.mf_logging.pretty_formattable import MetricFlowPrettyFormattable
from metricflow_semantics.mf_logging.pretty_formatter import PrettyFormatContext
from metricflow_semantics.visitor import VisitorOutputT

logger = logging.getLogger(__name__)


@singleton_dataclass(order=False)
class DsiEntityNode(SemanticGraphNode):
    entity_name: str

    @staticmethod
    def get_instance(entity_name: str) -> DsiEntityNode:
        element_label = DunderNameElementLabel(element_name=entity_name)
        return DsiEntityNode(
            entity_name=entity_name,
        )

    @override
    def accept(self, visitor: SemanticGraphNodeVisitor[VisitorOutputT]) -> VisitorOutputT:
        return visitor.visit_entity_node(self)

    @property
    @override
    def comparison_key(self) -> ComparisonKey:
        return (self.entity_name,)

    @override
    @property
    def dot_attributes(self) -> DotNodeAttributeSet:
        return super(DsiEntityNode, self).dot_attributes.merge(
            DotNodeAttributeSet.create(
                color=GraphvizColor.CORNFLOWER_BLUE,
            )
        )

    @override
    @property
    def node_descriptor(self) -> MetricflowGraphNodeDescriptor:
        return MetricflowGraphNodeDescriptor.get_instance(
            node_name=f"Entity({self.entity_name})", cluster_name="model_entity"
        )

    @override
    @cached_property
    def dunder_name_element_label(self) -> DunderNameElementLabel:
        return DunderNameElementLabel(element_name=self.entity_name)

    @cached_property
    def labels(self) -> FrozenOrderedSet[MetricflowGraphLabel]:
        return FrozenOrderedSet((self.dunder_name_element_label, DsiEntityLabel()))


@singleton_dataclass(order=False)
class SemanticModelId(MetricFlowPrettyFormattable, Comparable):
    model_name: str

    @property
    def cluster_name(self) -> str:
        return self.model_name

    @override
    @property
    def comparison_key(self) -> ComparisonKey:
        return (self.model_name,)

    @override
    def __str__(self) -> str:
        return self.model_name

    @override
    def pretty_format(self, format_context: PrettyFormatContext) -> Optional[str]:
        return self.model_name


@singleton_dataclass(order=False)
class TimeBaseNode(SemanticGraphNode):
    time_grain_name: str

    def accept(self, visitor: SemanticGraphNodeVisitor[VisitorOutputT]) -> VisitorOutputT:
        raise NotImplementedError

    @override
    @cached_property
    def node_descriptor(self) -> MetricflowGraphNodeDescriptor:
        return MetricflowGraphNodeDescriptor.get_instance(
            node_name=f"TimeBase({self.time_grain_name})", cluster_name="time_base"
        )

    @property
    def comparison_key(self) -> ComparisonKey:
        return (self.time_grain_name,)


@singleton_dataclass(order=False)
class AggregationNode(SemanticGraphNode):
    model_id: SemanticModelId
    aggregation_time_dimension_name: str

    def accept(self, visitor: SemanticGraphNodeVisitor[VisitorOutputT]) -> VisitorOutputT:
        raise NotImplementedError

    @override
    @cached_property
    def node_descriptor(self) -> MetricflowGraphNodeDescriptor:
        return MetricflowGraphNodeDescriptor.get_instance(
            node_name=f"Agg({self.model_id.model_name}, {self.aggregation_time_dimension_name})",
            cluster_name=self.model_id.cluster_name,
        )

    @property
    def comparison_key(self) -> ComparisonKey:
        return (self.model_id.comparison_key, self.aggregation_time_dimension_name)

    @override
    @cached_property
    def labels(self) -> FrozenOrderedSet[MetricflowGraphLabel]:
        return super(AggregationNode, self).labels.union((AggregationLabel(),))


@singleton_dataclass(order=False)
class JoinToModelNode(SemanticGraphNode):
    model_id: SemanticModelId

    def accept(self, visitor: SemanticGraphNodeVisitor[VisitorOutputT]) -> VisitorOutputT:
        raise NotImplementedError

    @override
    @cached_property
    def node_descriptor(self) -> MetricflowGraphNodeDescriptor:
        return MetricflowGraphNodeDescriptor.get_instance(
            node_name=f"JoinTo({self.model_id.model_name})",
            cluster_name=self.model_id.cluster_name,
        )

    @property
    def comparison_key(self) -> ComparisonKey:
        return self.model_id.comparison_key


@singleton_dataclass(order=False)
class JoinFromModelNode(SemanticGraphNode):
    model_id: SemanticModelId

    def accept(self, visitor: SemanticGraphNodeVisitor[VisitorOutputT]) -> VisitorOutputT:
        raise NotImplementedError

    @override
    @cached_property
    def node_descriptor(self) -> MetricflowGraphNodeDescriptor:
        return MetricflowGraphNodeDescriptor.get_instance(
            node_name=f"JoinFrom({self.model_id})",
            cluster_name=self.model_id.cluster_name,
        )

    @override
    @property
    def comparison_key(self) -> ComparisonKey:
        return self.model_id.comparison_key

    @override
    @cached_property
    def labels(self) -> FrozenOrderedSet[MetricflowGraphLabel]:
        return super(SemanticGraphNode, self).labels.union((JoinFromLabel(),))


@singleton_dataclass(order=False)
class TimeDimensionNode(SemanticGraphNode):
    dimension_name: str
    time_grain_name: str

    def accept(self, visitor: SemanticGraphNodeVisitor[VisitorOutputT]) -> VisitorOutputT:
        raise NotImplementedError

    @override
    @cached_property
    def node_descriptor(self) -> MetricflowGraphNodeDescriptor:
        return MetricflowGraphNodeDescriptor.get_instance(
            node_name=f"TimeDim({self.dimension_name})",
            cluster_name=self.dimension_name,
        )

    @property
    def comparison_key(self) -> ComparisonKey:
        return (self.dimension_name,)

    @override
    @cached_property
    def labels(self) -> FrozenOrderedSet[MetricflowGraphLabel]:
        return super(TimeDimensionNode, self).labels.union((TimeDimensionLabel(),))

    @override
    @cached_property
    def dunder_name_element_label(self) -> Optional[DunderNameElementLabel]:
        return DunderNameElementLabel(element_name=self.dimension_name)


@singleton_dataclass(order=False)
class MetricTimeDimensionNode(TimeDimensionNode):
    @staticmethod
    def get_instance(time_grain_name: str) -> MetricTimeDimensionNode:
        return MetricTimeDimensionNode(
            dimension_name=METRIC_TIME_ELEMENT_NAME,
            time_grain_name=time_grain_name,
        )

    def accept(self, visitor: SemanticGraphNodeVisitor[VisitorOutputT]) -> VisitorOutputT:
        raise NotImplementedError

    @override
    @cached_property
    def node_descriptor(self) -> MetricflowGraphNodeDescriptor:
        return MetricflowGraphNodeDescriptor.get_instance(
            node_name=f"MetricTimeDim({self.time_grain_name})", cluster_name="metric_time_base"
        )

    @property
    def comparison_key(self) -> ComparisonKey:
        return (self.time_grain_name,)

    @override
    @cached_property
    def labels(self) -> FrozenOrderedSet[MetricflowGraphLabel]:
        return super(MetricTimeDimensionNode, self).labels.union((MetricTimeLabel(),))


@singleton_dataclass(order=False)
class GroupByAttributeRootNode(SemanticGraphNode):
    @override
    def accept(self, visitor: SemanticGraphNodeVisitor[VisitorOutputT]) -> VisitorOutputT:
        raise NotImplementedError

    @override
    @property
    def node_descriptor(self) -> MetricflowGraphNodeDescriptor:
        return MetricflowGraphNodeDescriptor.get_instance(
            node_name="GroupByAttributeRoot",
            cluster_name=None,
        )

    @override
    @property
    def comparison_key(self) -> ComparisonKey:
        return ()

    @override
    @cached_property
    def labels(self) -> FrozenOrderedSet[MetricflowGraphLabel]:
        return super(GroupByAttributeRootNode, self).labels.union((GroupByAttributeRootLabel(),))

    @override
    @property
    def dot_attributes(self) -> DotNodeAttributeSet:
        return super(GroupByAttributeRootNode, self).dot_attributes.merge(
            DotNodeAttributeSet.create(
                color=GraphvizColor.GOLD,
            )
        )
