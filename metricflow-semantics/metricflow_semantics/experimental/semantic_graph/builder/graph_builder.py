from __future__ import annotations

import logging
import time
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
from metricflow_semantics.experimental.semantic_graph.builder.group_by_metric_subgraph import (
    GroupByMetricSubgraphGenerator,
)
from metricflow_semantics.experimental.semantic_graph.builder.measure_attribute_subgraph import (
    MeasureAttributeSubgraphGenerator,
)
from metricflow_semantics.experimental.semantic_graph.builder.metric_subgraph import MetricSubgraphGenerator
from metricflow_semantics.experimental.semantic_graph.builder.time_dimension_subgraph import (
    TimeDimensionSubgraphGenerator,
)
from metricflow_semantics.experimental.semantic_graph.builder.time_entity_subgraph import TimeEntitySubgraphGenerator
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
        TimeEntitySubgraphGenerator,
        MetricSubgraphGenerator,
        GroupByMetricSubgraphGenerator,
    )

    def __init__(
        self,
        generator_argument_set: SubgraphGeneratorArgumentSet,
    ) -> None:
        self._generator_argument_set = generator_argument_set
        self._verbose_debug_logs = False

    def build(
        self,
        subgraph_generators: Optional[Iterable[Type[SemanticSubgraphGenerator]]] = None,
    ) -> SemanticGraph:
        if subgraph_generators is None:
            subgraph_generators = self._ALL_SUBGRAPH_GENERATORS

        current_graph = MutableSemanticGraph.create()
        for generator in subgraph_generators:
            start_time = time.perf_counter()
            generator_instance = generator(self._generator_argument_set)

            subgraph = generator_instance.generate_subgraph(current_graph)
            runtime = time.perf_counter() - start_time
            if self._verbose_debug_logs:
                logger.debug(
                    LazyFormat(
                        "Generated subgraph from a generator",
                        generator=generator.__name__,
                        subgraph=subgraph,
                        runtime=f"{runtime:.2f}s",
                    )
                )
            current_graph.add_nodes(subgraph.nodes)
            current_graph.add_edges(subgraph.edges)

        return current_graph
