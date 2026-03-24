from __future__ import annotations

import logging
from collections.abc import Sequence
from typing import Type

from typing_extensions import override

from metricflow_semantics.semantic_graph.builder.categorical_dimension_subgraph import (
    CategoricalDimensionSubgraphGenerator,
)
from metricflow_semantics.semantic_graph.builder.entity_join_subgraph import EntityJoinSubgraphGenerator
from metricflow_semantics.semantic_graph.builder.entity_key_subgraph import (
    EntityKeySubgraphGenerator,
)
from metricflow_semantics.semantic_graph.builder.metric_subgraph import ComplexMetricSubgraphGenerator
from metricflow_semantics.semantic_graph.builder.simple_metric_subgraph import (
    SimpleMetricSubgraphGenerator,
)
from metricflow_semantics.semantic_graph.builder.subgraph_generator import SemanticSubgraphGenerator
from metricflow_semantics.semantic_graph.builder.time_dimension_subgraph import (
    TimeDimensionSubgraphGenerator,
)
from metricflow_semantics.semantic_graph.builder.time_entity_subgraph import TimeEntitySubgraphGenerator
from metricflow_semantics.semantic_graph.lookups.manifest_object_lookup import ManifestObjectLookup
from metricflow_semantics.semantic_graph.sg_interfaces import MutableSemanticGraph, SemanticGraph
from metricflow_semantics.toolkit.mf_logging.lazy_formattable import LazyFormat
from metricflow_semantics.toolkit.mf_type_aliases import AnyLengthTuple
from metricflow_semantics.toolkit.performance_helpers import ExecutionTimer

logger = logging.getLogger(__name__)


class SemanticGraphBuilder:
    """Convenience class that builds a semantic graph using all subgraph generators.

    This will replace `PartialSemanticGraphBuilder` once the PRs that depend on it are merged.
    """

    _ALL_SUBGRAPH_GENERATORS: AnyLengthTuple[Type[SemanticSubgraphGenerator]] = (
        CategoricalDimensionSubgraphGenerator,
        EntityKeySubgraphGenerator,
        EntityJoinSubgraphGenerator,
        SimpleMetricSubgraphGenerator,
        TimeDimensionSubgraphGenerator,
        TimeEntitySubgraphGenerator,
        ComplexMetricSubgraphGenerator,
    )

    @override
    def __init__(self, manifest_object_lookup: ManifestObjectLookup) -> None:
        self._manifest_object_lookup = manifest_object_lookup
        self._verbose_debug_logs = True

    def _build(self, subgraph_generators: Sequence[Type[SemanticSubgraphGenerator]]) -> SemanticGraph:
        current_graph = MutableSemanticGraph.create()
        for generator in subgraph_generators:
            generation_timer = ExecutionTimer()
            with generation_timer:
                generator_instance = generator(self._manifest_object_lookup)
                start_node_count = len(current_graph.nodes)
                start_edge_count = len(current_graph.edges)
                generated_edges = generator_instance.generate_edges()
                generated_edge_count = len(generated_edges)
                current_graph.add_edges(generated_edges)
                added_node_count = len(current_graph.nodes) - start_node_count
                added_edge_count = len(current_graph.edges) - start_edge_count

            if self._verbose_debug_logs:
                logger.debug(
                    LazyFormat(
                        "Generated subgraph",
                        generator=generator.__name__,
                        duration=generation_timer.total_duration,
                        added_node_count=added_node_count,
                        added_edge_count=added_edge_count,
                        generated_edge_count=generated_edge_count,
                    )
                )

        return current_graph

    def build(  # noqa: D102
        self, subgraph_generators: Sequence[Type[SemanticSubgraphGenerator]] = _ALL_SUBGRAPH_GENERATORS
    ) -> SemanticGraph:
        with ExecutionTimer() as build_timer:
            result_graph = self._build(subgraph_generators)
        logger.info(
            LazyFormat(
                "Generated semantic graph.",
                node_count=len(result_graph.nodes),
                edge_count=len(result_graph.edges),
                duration=build_timer.total_duration,
            )
        )
        return result_graph
