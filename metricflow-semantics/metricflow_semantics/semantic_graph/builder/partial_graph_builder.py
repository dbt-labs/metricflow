from __future__ import annotations

import logging
import time
from typing import Iterable, Type

from typing_extensions import override

from metricflow_semantics.semantic_graph.builder.subgraph_generator import (
    SemanticSubgraphGenerator,
)
from metricflow_semantics.semantic_graph.lookups.manifest_object_lookup import ManifestObjectLookup
from metricflow_semantics.semantic_graph.sg_interfaces import MutableSemanticGraph, SemanticGraph
from metricflow_semantics.toolkit.mf_logging.lazy_formattable import LazyFormat

logger = logging.getLogger(__name__)


class PartialSemanticGraphBuilder:
    """Builds a partial semantic graph using a subset of generators."""

    @override
    def __init__(self, manifest_object_lookup: ManifestObjectLookup) -> None:
        self._manifest_object_lookup = manifest_object_lookup
        self._verbose_debug_logs = True

    def build(
        self,
        subgraph_generators: Iterable[Type[SemanticSubgraphGenerator]],
    ) -> SemanticGraph:
        """Return a semantic graph created by combining the output of the given generators."""
        current_graph = MutableSemanticGraph.create()
        for generator in subgraph_generators:
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
