from __future__ import annotations

import logging
from collections.abc import Sequence
from dataclasses import dataclass
from typing import Optional, override

from metricflow_semantics.experimental.metricflow_exception import MetricflowAssertionError
from metricflow_semantics.experimental.ordered_set import MutableOrderedSet
from metricflow_semantics.experimental.semantic_graph.attribute_computation import (
    AttributeComputationUpdate,
    MutableAttributeComputation,
    SpecResult,
)
from metricflow_semantics.experimental.semantic_graph.manifest_object_lookup import ManifestObjectLookup
from metricflow_semantics.experimental.semantic_graph.nodes.node_label import (
    GroupByAttributeLabel,
    MeasureAttributeLabel,
)
from metricflow_semantics.experimental.semantic_graph.nodes.semantic_graph_node import (
    SemanticGraphEdge,
    SemanticGraphNode,
)
from metricflow_semantics.experimental.semantic_graph.path_finding.graph_path import (
    MetricflowGraphPath,
    MutableMetricflowGraphPath,
)
from metricflow_semantics.experimental.semantic_graph.path_finding.path_finder import (
    MetricflowGraphPathFinder,
    WeightFunction,
)
from metricflow_semantics.experimental.semantic_graph.semantic_graph import SemanticGraph
from metricflow_semantics.mf_logging.lazy_formattable import LazyFormat

logger = logging.getLogger(__name__)


@dataclass
class AttributeResolutionPath(MutableMetricflowGraphPath[SemanticGraphNode, SemanticGraphEdge]):
    attribute_computation: MutableAttributeComputation

    @classmethod
    def create(cls) -> AttributeResolutionPath:
        return AttributeResolutionPath(
            _nodes=[],
            _edges=[],
            _weight_addition_order=[],
            _current_weight=0,
            _current_node_set=MutableOrderedSet(),
            _node_set_addition_order=[],
            attribute_computation=MutableAttributeComputation(),
        )

    def _append_update(self, update: AttributeComputationUpdate) -> None:
        self.attribute_computation.append_update(update)

    @override
    def append_edge(self, edge: SemanticGraphEdge, weight: int) -> None:
        if len(self._edges) == 0 and len(self._nodes) == 0:
            self._append_update(edge.tail_node.attribute_computation_update)

        self._append_update(edge.attribute_computation_update)
        self._append_update(edge.head_node.attribute_computation_update)
        super().append_edge(edge, weight)

    @override
    def pop(self) -> None:
        if len(self._nodes) == 1 and len(self._edges) == 0:
            self.attribute_computation.pop_update()

        if len(self._edges) != 0:
            self.attribute_computation.pop_update()
            self.attribute_computation.pop_update()
        super().pop()


class SemanticGraphGroupByItemSpecResolver:
    def __init__(self, manifest_object_lookup: ManifestObjectLookup, semantic_graph: SemanticGraph) -> None:
        self._manifest_object_lookup = manifest_object_lookup
        self._semantic_graph = semantic_graph
        self._path_finder = MetricflowGraphPathFinder(semantic_graph)

    def get_specs_for_measure(self, measure_name: str) -> Sequence[SpecResult]:
        matching_nodes = self._semantic_graph.nodes_with_label(MeasureAttributeLabel(measure_name=measure_name))

        measure_node = matching_nodes.first_element(
            lambda: MetricflowAssertionError(
                LazyFormat(
                    "Did not find exactly 1 node in the semantic graph with the given measure name",
                    measure_name=measure_name,
                    matching_nodes=matching_nodes,
                )
            )
        )
        group_by_attribute_nodes = self._semantic_graph.nodes_with_label(GroupByAttributeLabel())

        path = AttributeResolutionPath.create()
        spec_results = []
        for _ in self._path_finder.traverse_dfs(
            source_node=measure_node,
            target_nodes=group_by_attribute_nodes,
            weight_function=DunderNameWeightFunction(),
            max_path_weight=3,
            allow_node_revisits=True,
            mutable_path=path,
        ):
            spec = path.attribute_computation.spec
            logger.debug(LazyFormat("Got path", path_nodes=path.nodes, spec=spec))
            if spec is not None:
                spec_results.append(spec)

        return spec_results


class DunderNameWeightFunction(WeightFunction[SemanticGraphNode, SemanticGraphEdge, MutableMetricflowGraphPath]):
    def weight(
        self, path: MetricflowGraphPath[SemanticGraphNode, SemanticGraphEdge], edge: SemanticGraphEdge
    ) -> Optional[int]:
        raise NotImplementedError()
