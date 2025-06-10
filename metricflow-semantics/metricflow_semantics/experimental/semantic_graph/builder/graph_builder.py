from __future__ import annotations

import logging
from typing import Iterable, Optional, Type

from metricflow_semantics.experimental.semantic_graph.attribute_resolution.attribute_computation_path import (
    AttributeComputationPath,
)
from metricflow_semantics.experimental.semantic_graph.builder.categorical_dimension_attribute_subgraph import (
    CategoricalDimensionAttributeSubgraphGenerator,
)
from metricflow_semantics.experimental.semantic_graph.builder.entity_attribute_subgraph import (
    EntityAttributeSubgraphGenerator,
)
from metricflow_semantics.experimental.semantic_graph.builder.entity_join_subgraph import EntityJoinSubgraphGenerator
from metricflow_semantics.experimental.semantic_graph.builder.graph_change_rule import (
    SemanticSubgraphGenerator,
    SubgraphGeneratorArgumentSet,
)
from metricflow_semantics.experimental.semantic_graph.builder.group_by_metric_subgraph import GroupByMetricSubgraph
from metricflow_semantics.experimental.semantic_graph.builder.measure_attribute_subgraph import (
    MeasureAttributeSubgraphGenerator,
)
from metricflow_semantics.experimental.semantic_graph.builder.metric_subgraph import MetricAttributeSubgraphGenerator
from metricflow_semantics.experimental.semantic_graph.builder.time_dimension_subgraph import (
    TimeDimensionSubgraphGenerator,
)
from metricflow_semantics.experimental.semantic_graph.manifest_object_lookup import ManifestObjectLookup
from metricflow_semantics.experimental.semantic_graph.nodes.semantic_graph_node import (
    SemanticGraphEdge,
    SemanticGraphNode,
)
from metricflow_semantics.experimental.semantic_graph.path_finding.path_finder import MetricflowGraphPathFinder
from metricflow_semantics.experimental.semantic_graph.semantic_graph import MutableSemanticGraph, SemanticGraph
from metricflow_semantics.mf_logging.lazy_formattable import LazyFormat

logger = logging.getLogger(__name__)


class SemanticGraphBuilder:
    _ALL_SUBGRAPH_GENERATORS = (
        CategoricalDimensionAttributeSubgraphGenerator,
        EntityAttributeSubgraphGenerator,
        EntityJoinSubgraphGenerator,
        MeasureAttributeSubgraphGenerator,
        TimeDimensionSubgraphGenerator,
        MetricAttributeSubgraphGenerator,
        GroupByMetricSubgraph,
    )

    def __init__(
        self,
        manifest_object_lookup: ManifestObjectLookup,
        path_finder: MetricflowGraphPathFinder[SemanticGraphNode, SemanticGraphEdge, AttributeComputationPath],
    ) -> None:
        self._path_finder = path_finder
        self._generator_argument_set = SubgraphGeneratorArgumentSet(
            manifest_object_lookup=manifest_object_lookup,
            path_finder=self._path_finder,
        )
        self._verbose_debug_logs = False

    def build(
        self,
        subgraph_generators: Optional[Iterable[Type[SemanticSubgraphGenerator]]] = None,
    ) -> SemanticGraph:
        if subgraph_generators is None:
            subgraph_generators = self._ALL_SUBGRAPH_GENERATORS

        current_graph = MutableSemanticGraph.create()
        for generator in subgraph_generators:
            generator_instance = generator(self._generator_argument_set)

            subgraph = generator_instance.generate_subgraph(current_graph)
            if self._verbose_debug_logs:
                logger.debug(
                    LazyFormat(
                        "Generated subgraph from a generator",
                        generator=generator.__name__,
                        subgraph=subgraph,
                    )
                )
            current_graph.add_nodes(subgraph.nodes)
            current_graph.add_edges(subgraph.edges)

        return current_graph
