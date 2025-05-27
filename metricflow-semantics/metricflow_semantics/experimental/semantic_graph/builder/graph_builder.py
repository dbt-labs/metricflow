from __future__ import annotations

import logging
from typing import Iterable, Optional, Type

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
from metricflow_semantics.experimental.semantic_graph.builder.measure_attribute_subgraph import (
    MeasureAttributeSubgraphGenerator,
)
from metricflow_semantics.experimental.semantic_graph.builder.time_dimension_subgraph import (
    TimeDimensionSubgraphGenerator,
)
from metricflow_semantics.experimental.semantic_graph.manifest_object_lookup import ManifestObjectLookup
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
        # TimeSubgraphGenerator,
    )

    def build(
        self,
        manifest_object_lookup: ManifestObjectLookup,
        subgraph_generators: Optional[Iterable[Type[SemanticSubgraphGenerator]]] = None,
    ) -> SemanticGraph:
        if subgraph_generators is None:
            subgraph_generators = self._ALL_SUBGRAPH_GENERATORS

        semantic_graph = MutableSemanticGraph.create()
        argument_set = SubgraphGeneratorArgumentSet(manifest_object_lookup=manifest_object_lookup)
        for Generator in subgraph_generators:
            generator = Generator(argument_set)

            subgraph = generator.generate_subgraph()
            logger.debug(
                LazyFormat(
                    "Generated Subgraph",
                    generator=Generator.__name__,
                    subgraph=subgraph,
                )
            )
            semantic_graph.add_nodes(subgraph.nodes)
            semantic_graph.add_edges(subgraph.edges)

        return semantic_graph
