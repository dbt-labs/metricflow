from __future__ import annotations

from dbt_semantic_interfaces.references import MeasureReference

from metricflow_semantics.experimental.semantic_graph_old.builder.in_progress_semantic_graph import (
    InProgressSemanticGraph,
)
from metricflow_semantics.experimental.semantic_graph_old.builder.recipies.graph_recipe import SemanticGraphRecipe
from metricflow_semantics.experimental.semantic_graph_old.graph_nodes import AssociativeEntityNode, SemanticEntityType
from metricflow_semantics.experimental.semantic_graph_old.ids.entity_ids import ElementEntityId


class AddMeasureRecipe(SemanticGraphRecipe):
    def add_node_for_measure(
        self, semantic_graph: InProgressSemanticGraph, measure_reference: MeasureReference
    ) -> None:
        semantic_model = self.semantic_model_lookup.get_semantic_model_for_measure(measure_reference)

        semantic_graph.add_node(
            AssociativeEntityNode.get_instance(
                element_entity_id=ElementEntityId.get_instance(
                    measure_reference.element_name, SemanticEntityType.MEASURE
                ),
                via_semantic_model=semantic_model.reference,
            )
        )
