from __future__ import annotations

import logging
from typing import Sequence

from dbt_semantic_interfaces.type_enums import TimeGranularity

from metricflow_semantics.experimental.semantic_graph.builder.in_progress_semantic_graph import InProgressSemanticGraph
from metricflow_semantics.experimental.semantic_graph.builder.rules.time_helpers import TimeHelper
from metricflow_semantics.experimental.semantic_graph.builder.semantic_graph_transform_rule import (
    SemanticGraphRecipe,
)
from metricflow_semantics.experimental.semantic_graph.computation_method import (
    DateTruncComputationMethod,
    MetricTimeComputationMethod,
)
from metricflow_semantics.experimental.semantic_graph.graph_edges import (
    ProvidedEdgeTagSet,
    RequiredTagSet,
    SemanticGraphEdgeType,
)
from metricflow_semantics.experimental.semantic_graph.time_nodes import TimeAttributeNodeEnum, TimeEntityNodeEnum

logger = logging.getLogger(__name__)


class AddTimeNodes(SemanticGraphRecipe):

    def execute_recipe(self, semantic_graph: InProgressSemanticGraph) -> None:
        time_entity_node = TimeEntityNodeEnum.TIME_ENTITY_NODE.value
        metric_time_entity_node = TimeEntityNodeEnum.METRIC_TIME_ENTITY_NODE.value

        semantic_graph.nodes.add(time_entity_node)
        semantic_graph.nodes.add(metric_time_entity_node)

        semantic_graph.add_edge(
            tail_node=metric_time_entity_node,
            edge_type=SemanticGraphEdgeType.ONE_TO_ONE,
            head_node=time_entity_node,
            computation_method=MetricTimeComputationMethod(),
            required_tags=RequiredTagSet.empty_set(),
            provided_tags=ProvidedEdgeTagSet.empty_set(),
        )

        for time_grain in TimeHelper.ALLOWED_TIME_GRAINS:
            semantic_graph.nodes.add(TimeAttributeNodeEnum.get_for_time_grain(time_grain))

            more_fine_time_grains = TimeHelper.more_fine_time_grains(time_grain)
            time_grain_attribute_node = TimeAttributeNodeEnum.get_for_time_grain(time_grain)

            allowed_time_grains = (time_grain,) + tuple(more_fine_time_grains)
            semantic_graph.add_edge(
                tail_node=time_entity_node,
                edge_type=SemanticGraphEdgeType.ONE_TO_ONE,
                head_node=time_grain_attribute_node,
                computation_method=DateTruncComputationMethod(time_grain),
                required_tags=RequiredTagSet.create(
                    allowed_metric_time_grains=allowed_time_grains,
                    allowed_attribute_time_grains=allowed_time_grains,
                ),
            )
