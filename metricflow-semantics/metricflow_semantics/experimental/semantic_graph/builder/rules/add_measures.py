from __future__ import annotations

import logging

from dbt_semantic_interfaces.protocols import SemanticModel
from dbt_semantic_interfaces.references import EntityReference, MeasureReference
from dbt_semantic_interfaces.type_enums import EntityType, TimeGranularity

from metricflow_semantics.experimental.semantic_graph.builder.in_progress_semantic_graph import InProgressSemanticGraph
from metricflow_semantics.experimental.semantic_graph.builder.semantic_graph_transform_rule import (
    SemanticGraphRecipe,
)
from metricflow_semantics.experimental.semantic_graph.computation_method import (
    CoLocatedComputationMethod,
)
from metricflow_semantics.experimental.semantic_graph.graph_edges import (
    ProvidedEdgeTagSet,
    RequiredTagSet,
    SemanticGraphEdgeType,
)
from metricflow_semantics.experimental.semantic_graph.graph_nodes import (
    EntityNode,
    MeasureAttributeNode,
)
from metricflow_semantics.experimental.semantic_graph.time_nodes import TimeEntityNodeEnum

logger = logging.getLogger(__name__)


class AddMeasureAttributeNodes(SemanticGraphRecipe):
    def _get_primary_entity_reference(self, semantic_model: SemanticModel) -> EntityReference:
        if semantic_model.primary_entity_reference is not None:
            return semantic_model.primary_entity_reference

        for entity in semantic_model.entities:
            if entity.type is EntityType.PRIMARY:
                return entity.reference

        raise RuntimeError(
            f"There is no primary entity in {semantic_model.reference}. This should have been caught "
            f"during semantic-manifest validation."
        )

    def _get_aggregation_time_grain_for_measure(
        self, semantic_model: SemanticModel, measure_reference: MeasureReference
    ) -> TimeGranularity:
        measure_aggregation_time_dimension = semantic_model.checked_agg_time_dimension_for_measure(measure_reference)

        dimension_reference = measure_aggregation_time_dimension.dimension_reference
        for dimension in semantic_model.dimensions:
            if dimension.reference == dimension_reference:
                dimension_type_params = dimension.type_params
                if dimension_type_params is not None:
                    return dimension_type_params.time_granularity

        raise RuntimeError(
            f"Did not find the aggregation time grain for {measure_reference} "
            f"in {semantic_model.reference}. This should have been caught during semantic-manifest validation."
        )

    def execute_recipe(self, semantic_graph: InProgressSemanticGraph) -> None:
        for semantic_model in self._semantic_manifest.semantic_models:
            if len(semantic_model.measures) == 0:
                continue

            primary_entity_node = EntityNode(self._get_primary_entity_reference(semantic_model))

            for measure in semantic_model.measures:
                aggregation_time_grain = self._get_aggregation_time_grain_for_measure(semantic_model, measure.reference)
                measure_node = MeasureAttributeNode(measure.reference)
                computation_method = CoLocatedComputationMethod(semantic_model.reference)

                # Add edge to primary entity.
                semantic_graph.add_edge(
                    tail_node=measure_node,
                    edge_type=SemanticGraphEdgeType.ATTRIBUTE_SOURCE,
                    head_node=primary_entity_node,
                    computation_method=computation_method,
                    required_tags=RequiredTagSet.empty_set(),
                    provided_tags=ProvidedEdgeTagSet.empty_set(),
                )

                # Add edge to metric_time.
                semantic_graph.add_edge(
                    tail_node=measure_node,
                    edge_type=SemanticGraphEdgeType.ATTRIBUTE_SOURCE,
                    head_node=TimeEntityNodeEnum.METRIC_TIME_ENTITY_NODE.value,
                    computation_method=computation_method,
                    required_tags=RequiredTagSet.empty_set(),
                    provided_tags=ProvidedEdgeTagSet.create(
                        metric_time_grains=[
                            time_grain
                            for time_grain in TimeGranularity
                            if time_grain.to_int() >= aggregation_time_grain.to_int()
                        ]
                    ),
                )
