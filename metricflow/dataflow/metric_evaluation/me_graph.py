from __future__ import annotations

import logging
import pathlib
import typing
from abc import ABC, abstractmethod
from collections import defaultdict
from collections.abc import Mapping, Sequence, Set
from dataclasses import dataclass
from enum import Enum
from functools import cached_property
from pathlib import Path
from typing import Callable, Generic, Optional, TypeVar, override

from typing_extensions import Self, override

from metricflow.dataflow.metric_evaluation.me_elements import MetricDescriptor, MetricDescriptorSet
from metricflow_semantics.semantic_graph.model_id import SemanticModelId
from metricflow_semantics.toolkit.collections.ordered_set import FrozenOrderedSet, MutableOrderedSet
from metricflow_semantics.toolkit.dataclass_helpers import fast_frozen_dataclass
from metricflow_semantics.toolkit.mf_graph.comparable import ComparisonKey
from metricflow_semantics.toolkit.mf_graph.graph_id import MetricFlowGraphId, SequentialGraphId
from metricflow_semantics.toolkit.mf_graph.mf_graph import MetricFlowGraphNode, MetricFlowGraphEdge, MetricFlowGraph
from metricflow_semantics.toolkit.mf_graph.mutable_graph import MutableGraph
from metricflow_semantics.toolkit.mf_graph.node_descriptor import MetricFlowGraphNodeDescriptor

from metricflow_semantics.toolkit.mf_logging.lazy_formattable import LazyFormat
from metricflow_semantics.toolkit.string_helpers import mf_dedent

logger = logging.getLogger(__name__)


@fast_frozen_dataclass()
class MetricQueryNode(MetricFlowGraphNode, ABC):
    output_metric_descriptors: MetricDescriptorSet


class BaseMetricQueryNode(MetricQueryNode):
    model_id: SemanticModelId

    @cached_property
    def node_descriptor(self) -> MetricFlowGraphNodeDescriptor:
        return MetricFlowGraphNodeDescriptor(node_name=self.model_id.model_name, cluster_name=None)

    @property
    def comparison_key(self) -> ComparisonKey:
        return (self.node_descriptor,)


class RecursiveMetricQueryNode(MetricQueryNode):

    @cached_property
    def node_descriptor(self) -> MetricFlowGraphNodeDescriptor:
        return MetricFlowGraphNodeDescriptor(node_name=self.node_name, cluster_name=None)

    @property
    def comparison_key(self) -> ComparisonKey:
        return (self.node_name, self.output_metric_descriptors)


@fast_frozen_dataclass()
class MetricQueryEdge(MetricFlowGraphEdge):
    metric_descriptor: MetricDescriptor

    @cached_property
    def inverse(self) -> MetricQueryEdge:
        raise NotImplementedError("Inverse metric query graph not yet supported")

    @cached_property
    def comparison_key(self) -> ComparisonKey:
        return (self.head_node, self.tail_node, self.metric_descriptor)


@fast_frozen_dataclass()
class MetricQueryGraph(MetricFlowGraph[MetricQueryNode, MetricQueryEdge], ABC):
    pass


@dataclass
class MutableMetricQueryGraph(MutableGraph[MetricQueryNode, MetricQueryEdge]):

    @classmethod
    def create(cls, graph_id: Optional[MetricFlowGraphId] = None) -> MutableMetricQueryGraph:  # noqa: D102
        return MutableMetricQueryGraph(
            _graph_id=graph_id or SequentialGraphId.create(),
            _nodes=MutableOrderedSet(),
            _edges=MutableOrderedSet(),
            _tail_node_to_edges=defaultdict(MutableOrderedSet),
            _head_node_to_edges=defaultdict(MutableOrderedSet),
            _label_to_nodes=defaultdict(MutableOrderedSet),
            _label_to_edges=defaultdict(MutableOrderedSet),
            _node_to_predecessor_nodes=defaultdict(MutableOrderedSet),
            _node_to_successor_nodes=defaultdict(MutableOrderedSet),
        )

    @override
    def intersection(self, other: MetricFlowGraph[MetricQueryNode, MetricQueryEdge]) -> MutableMetricQueryGraph:
        intersection_graph = MutableMetricQueryGraph.create(graph_id=self.graph_id)
        self.add_edges(self._intersect_edges(other))
        return intersection_graph

    @override
    def inverse(self) -> MutableMetricQueryGraph:
        raise NotImplementedError

    @override
    def as_sorted(self) -> MutableMetricQueryGraph:
        updated_graph = MutableMetricQueryGraph.create(graph_id=self.graph_id)
        for node in sorted(self._nodes):
            updated_graph.add_node(node)

        for edge in sorted(self._edges):
            updated_graph.add_edge(edge)

        return updated_graph
