from __future__ import annotations

import logging
from typing import Sequence

from dbt_semantic_interfaces.enum_extension import assert_values_exhausted
from dbt_semantic_interfaces.protocols import SemanticModel, Dimension
from dbt_semantic_interfaces.references import EntityReference
from dbt_semantic_interfaces.type_enums import DimensionType

from metricflow_semantics.experimental.semantic_graph.builder.in_progress_semantic_graph import InProgressSemanticGraph
from metricflow_semantics.experimental.semantic_graph.builder.rules.time_helpers import TimeHelper
from metricflow_semantics.experimental.semantic_graph.builder.semantic_graph_transform_rule import (
    SemanticGraphRecipe,
)
from metricflow_semantics.experimental.semantic_graph.computation_method import (
    CoLocatedComputationMethod,
)
from metricflow_semantics.experimental.semantic_graph.graph_edges import (
    ProvidedEdgeTagSet,
    RequiredTagSet,
    SemanticGraphEdgeType, Cardinality,
)
from metricflow_semantics.experimental.semantic_graph.graph_nodes import (
    EntityNode,
    DimensionAttributeNode, AssociativeEntityNode,
)

logger = logging.getLogger(__name__)


class AddNodesForDimensionsRecipe(SemanticGraphRecipe):

    def _get_cardinality_one_entity_references(self, semantic_model: SemanticModel) -> Sequence[EntityReference]:
        cardinality_one_entity_references = []
        for entity in semantic_model.entities:
            if Cardinality.get_for_entity_type(entity.type) is Cardinality.ONE:
                cardinality_one_entity_references.append(entity.reference)

        primary_entity_reference = semantic_model.primary_entity_reference
        if primary_entity_reference is not None:
            cardinality_one_entity_references.append(primary_entity_reference)
        return cardinality_one_entity_references

    def _add_graph_elements_for_time_dimension(
        self,
        semantic_graph: InProgressSemanticGraph,
        semantic_model: SemanticModel,
        dimension: Dimension,
        cardinality_one_entity_references: Sequence[EntityReference],
    ) -> None:
        if dimension.type_params is None:
            raise RuntimeError(
                f"{dimension} is of type {DimensionType.TIME}, but {dimension.type_params=}. This should have been"
                f" caught during validation."
            )
        associative_entity_node = AssociativeEntityNode(dimension.reference)
        semantic_graph.nodes.add(associative_entity_node)
        semantic_model_reference = semantic_model.reference
        for entity_reference in cardinality_one_entity_references:
            semantic_graph.add_edge(
                tail_node=EntityNode(entity_reference),
                edge_type=SemanticGraphEdgeType.MANY_TO_ONE,
                head_node=associative_entity_node,
                computation_method=CoLocatedComputationMethod(semantic_model_reference),
                required_tags=RequiredTagSet.create(
                    allowed_attribute_time_grains=TimeHelper.more_coarse_time_grains(
                        dimension.type_params.time_granularity),
                    allowed_metric_time_grains=TimeHelper.ALL_TIME_GRAINS,
                ),
                provided_tags=ProvidedEdgeTagSet.empty_set(),
            )

    def _add_graph_elements_for_categorical_dimension(
        self,
        semantic_graph: InProgressSemanticGraph,
        semantic_model: SemanticModel,
        dimension: Dimension,
        cardinality_one_entity_references: Sequence[EntityReference],
    ) -> None:
        dimension_node = DimensionAttributeNode(dimension.reference)
        semantic_graph.nodes.add(dimension_node)
        semantic_model_reference = semantic_model.reference
        for entity_reference in cardinality_one_entity_references:
            semantic_graph.add_edge(
                tail_node=EntityNode(entity_reference),
                edge_type=SemanticGraphEdgeType.ATTRIBUTE,
                head_node=dimension_node,
                computation_method=CoLocatedComputationMethod(semantic_model_reference),
                required_tags=RequiredTagSet.empty_set(),
                provided_tags=ProvidedEdgeTagSet.empty_set(),
            )

    def execute_recipe(self, semantic_graph: InProgressSemanticGraph) -> None:
        for semantic_model in self._semantic_manifest.semantic_models:
            if len(semantic_model.dimensions) == 0:
                continue

            cardinality_one_entity_references = self._get_cardinality_one_entity_references(semantic_model)

            for dimension in semantic_model.dimensions:
                if dimension.type is DimensionType.CATEGORICAL:
                    self._add_graph_elements_for_categorical_dimension(
                        semantic_graph=semantic_graph,
                        semantic_model=semantic_model,
                        dimension=dimension,
                        cardinality_one_entity_references=cardinality_one_entity_references,
                    )
                elif dimension.type is DimensionType.TIME:
                    self._add_graph_elements_for_time_dimension(
                        semantic_graph=semantic_graph,
                        semantic_model=semantic_model,
                        dimension=dimension,
                        cardinality_one_entity_references=cardinality_one_entity_references,
                    )
                else:
                    assert_values_exhausted(dimension.type)

