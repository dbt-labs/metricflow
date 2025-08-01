# from __future__ import annotations
#
# import logging
# from abc import ABC, abstractmethod
# from collections import defaultdict
# from collections.abc import Mapping
# from dataclasses import dataclass
# from functools import cached_property
#
# from typing_extensions import Optional, override
#
# from metricflow_semantics.collection_helpers.syntactic_sugar import mf_flatten
# from metricflow_semantics.experimental.dataclass_helpers import fast_frozen_dataclass
# from metricflow_semantics.experimental.mf_graph.comparable import ComparisonKey
# from metricflow_semantics.experimental.mf_graph.formatting.dot_attributes import DotGraphAttributeSet
# from metricflow_semantics.experimental.mf_graph.graph_id import MetricflowGraphId, SequentialGraphId
# from metricflow_semantics.experimental.mf_graph.graph_labeling import MetricflowGraphLabel
# from metricflow_semantics.experimental.mf_graph.mf_graph import (
#     MetricflowGraph,
#     MetricflowGraphEdge,
#     MetricflowGraphNode,
# )
# from metricflow_semantics.experimental.mf_graph.mutable_graph import MutableGraph
# from metricflow_semantics.experimental.mf_graph.node_descriptor import MetricflowGraphNodeDescriptor
# from metricflow_semantics.experimental.ordered_set import FrozenOrderedSet, MutableOrderedSet, OrderedSet
# from metricflow_semantics.experimental.singleton import Singleton
# from metricflow_semantics.mf_logging.pretty_formattable import MetricFlowPrettyFormattable
# from metricflow_semantics.mf_logging.pretty_formatter import PrettyFormatContext
# from metricflow_semantics.model.semantics.linkable_element_set_base import AnnotatedSpec
#
# logger = logging.getLogger(__name__)
#
#
# @fast_frozen_dataclass(order=False)
# class DunderNameNode(MetricflowGraphNode, MetricFlowPrettyFormattable, ABC):
#     pass
#
#
# @fast_frozen_dataclass(order=False)
# class NameElementNode(DunderNameNode, ABC):
#     name_element: str
#
#     @cached_property
#     @override
#     def node_descriptor(self) -> MetricflowGraphNodeDescriptor:
#         return MetricflowGraphNodeDescriptor(
#             node_name=self.name_element,
#             cluster_name=None,
#         )
#
#     @cached_property
#     @override
#     def comparison_key(self) -> ComparisonKey:
#         return (self.name_element,)
#
#     @override
#     def pretty_format(self, format_context: PrettyFormatContext) -> Optional[str]:
#         return self.node_descriptor.node_name
#
#
# @fast_frozen_dataclass(order=False)
# class IntermediateNameElementNode(NameElementNode, Singleton):
#     name_element_index: int
#
#     @classmethod
#     def get_instance(cls, name_element: str, name_element_index: int) -> IntermediateNameElementNode:
#         return cls._get_instance(name_element=name_element, name_element_index=name_element_index)
#
#
# @fast_frozen_dataclass()
# class TerminalNameNodeLabel(MetricflowGraphLabel, Singleton):
#     name_element: Optional[str]
#
#     @classmethod
#     def get_instance(cls, name_element: Optional[str] = None) -> TerminalNameNodeLabel:
#         return cls._get_instance(name_element=name_element)
#
#
# @fast_frozen_dataclass(order=False)
# class TerminalNameNode(NameElementNode, Singleton):
#     @classmethod
#     def get_instance(cls, name_element: Optional[str] = None) -> TerminalNameNode:
#         return cls._get_instance(name_element=name_element)
#
#     @cached_property
#     def labels(self) -> OrderedSet[MetricflowGraphLabel]:
#         return FrozenOrderedSet(
#             (TerminalNameNodeLabel.get_instance(), TerminalNameNodeLabel.get_instance(self.name_element))
#         )
#
#
# @fast_frozen_dataclass(order=False)
# class NameSourceNode(DunderNameNode, Singleton):
#     @cached_property
#     @override
#     def node_descriptor(self) -> MetricflowGraphNodeDescriptor:
#         return MetricflowGraphNodeDescriptor(node_name="Source", cluster_name=None)
#
#     @cached_property
#     @override
#     def comparison_key(self) -> ComparisonKey:
#         return ()
#
#     @classmethod
#     def get_instance(
#         cls,
#     ) -> NameSourceNode:
#         return cls._get_instance()
#
#
# @fast_frozen_dataclass(order=False)
# class DunderNameEdge(MetricflowGraphEdge[DunderNameNode]):
#     @cached_property
#     def inverse(self) -> DunderNameEdge:
#         return DunderNameEdge(tail_node=self.head_node, head_node=self.tail_node)
#
#     @cached_property
#     def comparison_key(self) -> ComparisonKey:
#         return ()
#
#     @override
#     def pretty_format(self, format_context: PrettyFormatContext) -> Optional[str]:
#         formatter = format_context.formatter
#         return formatter.pretty_format_object_by_parts(
#             class_name=self.__class__.__name__,
#             field_mapping={
#                 "tail_node": self.tail_node,
#                 "head_node": self.head_node,
#             },
#         )
#
#
# class DunderNameGraph(MetricflowGraph[TerminalNameNode, DunderNameEdge], ABC):
#     @override
#     def as_dot_graph(self, include_graphical_attributes: bool) -> DotGraphAttributeSet:
#         return (
#             super()
#             .as_dot_graph(include_graphical_attributes=include_graphical_attributes)
#             .with_attributes(
#                 dot_kwargs={
#                     "rankdir": "LR",
#                 }
#                 if include_graphical_attributes
#                 else {}
#             )
#         )
#
#     def nodes_with_labels(self, *labels: MetricflowGraphLabel) -> MutableOrderedSet[TerminalNameNode]:
#         """Return nodes in the graph with any of the given labels."""
#         return MutableOrderedSet(mf_flatten(self.nodes_with_label(label) for label in labels))
#
#     @property
#     @abstractmethod
#     def terminal_node_to_annotated_spec_mapping(self) -> Mapping[DunderNameNode, AnnotatedSpec]:
#         raise NotImplementedError
#
#
# @dataclass
# class MutableDunderNameGraph(MutableGraph[TerminalNameNode, DunderNameEdge], DunderNameGraph):
#     """A mutable implementation of `SemanticGraph` for building graphs."""
#
#     _terminal_node_to_annotated_spec: dict[DunderNameNode, AnnotatedSpec]
#
#     @classmethod
#     def create(
#         cls,
#         graph_id: Optional[MetricflowGraphId] = None,
#         _terminal_node_to_annotated_spec: Optional[dict[DunderNameNode, AnnotatedSpec]] = None,
#     ) -> MutableDunderNameGraph:  # noqa: D102
#         return MutableDunderNameGraph(
#             _graph_id=graph_id or SequentialGraphId.create(),
#             _nodes=MutableOrderedSet(),
#             _edges=MutableOrderedSet(),
#             _tail_node_to_edges=defaultdict(MutableOrderedSet),
#             _head_node_to_edges=defaultdict(MutableOrderedSet),
#             _label_to_nodes=defaultdict(MutableOrderedSet),
#             _label_to_edges=defaultdict(MutableOrderedSet),
#             _node_to_predecessor_nodes=defaultdict(MutableOrderedSet),
#             _node_to_successor_nodes=defaultdict(MutableOrderedSet),
#             _terminal_node_to_annotated_spec=_terminal_node_to_annotated_spec
#             if _terminal_node_to_annotated_spec is not None
#             else {},
#         )
#
#     def add_annotated_spec(self, terminal_node: DunderNameNode, annotated_spec: AnnotatedSpec) -> None:
#         current_spec = self._terminal_node_to_annotated_spec.get(terminal_node)
#         if current_spec is None:
#             self._terminal_node_to_annotated_spec[terminal_node] = annotated_spec
#             return
#
#         new_spec = current_spec.merge(annotated_spec)
#         self._terminal_node_to_annotated_spec[terminal_node] = new_spec
#
#     @override
#     def intersection(self, other: MetricflowGraph[TerminalNameNode, DunderNameEdge]) -> MutableDunderNameGraph:
#         # intersection_graph = MutableSemanticGraph.create(graph_id=self.graph_id)
#         # self.add_edges(self._intersect_edges(other))
#         # return intersection_graph
#         raise NotImplementedError
#
#     @override
#     def inverse(self) -> MutableDunderNameGraph:
#         raise NotImplementedError
#
#     @override
#     def as_sorted(self) -> MutableDunderNameGraph:
#         updated_graph = MutableDunderNameGraph.create(graph_id=self.graph_id)
#         for node in sorted(self._nodes):
#             updated_graph.add_node(node)
#
#         for edge in sorted(self._edges):
#             updated_graph.add_edge(edge)
#
#         return updated_graph
#
#     @cached_property
#     def terminal_node_to_annotated_spec_mapping(self) -> Mapping[DunderNameNode, AnnotatedSpec]:
#         return self._terminal_node_to_annotated_spec
