from __future__ import annotations

import logging
import time
from typing import Type

from typing_extensions import override

from metricflow_semantics.collection_helpers.mf_type_aliases import AnyLengthTuple
from metricflow_semantics.experimental.dsi.manifest_object_lookup import ManifestObjectLookup
from metricflow_semantics.experimental.semantic_graph.builder.categorical_dimension_subgraph import (
    CategoricalDimensionSubgraphGenerator,
)
from metricflow_semantics.experimental.semantic_graph.builder.entity_join_subgraph import EntityJoinSubgraphGenerator
from metricflow_semantics.experimental.semantic_graph.builder.entity_key_subgraph import (
    EntityKeySubgraphGenerator,
)
from metricflow_semantics.experimental.semantic_graph.builder.measure_subgraph import (
    MeasureSubgraphGenerator,
)
from metricflow_semantics.experimental.semantic_graph.builder.metric_subgraph import MetricSubgraphGenerator
from metricflow_semantics.experimental.semantic_graph.builder.subgraph_generator import SemanticSubgraphGenerator
from metricflow_semantics.experimental.semantic_graph.builder.time_dimension_subgraph import (
    TimeDimensionSubgraphGenerator,
)
from metricflow_semantics.experimental.semantic_graph.builder.time_entity_subgraph import TimeEntitySubgraphGenerator
from metricflow_semantics.experimental.semantic_graph.sg_interfaces import MutableSemanticGraph, SemanticGraph
from metricflow_semantics.mf_logging.lazy_formattable import LazyFormat

logger = logging.getLogger(__name__)


class SemanticGraphBuilder:
    """Convenience class that builds a semantic graph using all subgraph generators.

    This will replace `PartialSemanticGraphBuilder` once the PRs that depend on it are merged.
    """

    _ALL_SUBGRAPH_GENERATORS: AnyLengthTuple[Type[SemanticSubgraphGenerator]] = (
        CategoricalDimensionSubgraphGenerator,
        EntityKeySubgraphGenerator,
        EntityJoinSubgraphGenerator,
        MeasureSubgraphGenerator,
        TimeDimensionSubgraphGenerator,
        TimeEntitySubgraphGenerator,
        MetricSubgraphGenerator,
    )

    @override
    def __init__(self, manifest_object_lookup: ManifestObjectLookup) -> None:
        self._manifest_object_lookup = manifest_object_lookup
        self._verbose_debug_logs = True

    def build(self) -> SemanticGraph:  # noqa: D102
        current_graph = MutableSemanticGraph.create()
        for generator in self._ALL_SUBGRAPH_GENERATORS:
            start_time = time.perf_counter()
            generator_instance = generator(self._manifest_object_lookup)

            start_node_count = len(current_graph.nodes)
            start_edge_count = len(current_graph.edges)
            generated_edges = generator_instance.generate_edges()
            generated_edge_count = len(generated_edges)
            current_graph.add_edges(generated_edges)
            added_node_count = len(current_graph.nodes) - start_node_count
            added_edge_count = len(current_graph.edges) - start_edge_count
            runtime = time.perf_counter() - start_time
            if self._verbose_debug_logs:
                logger.debug(
                    LazyFormat(
                        "Generated subgraph",
                        generator=generator.__name__,
                        runtime=f"{runtime:.2f}s",
                        added_node_count=added_node_count,
                        added_edge_count=added_edge_count,
                        generated_edge_count=generated_edge_count,
                    )
                )

        return current_graph
