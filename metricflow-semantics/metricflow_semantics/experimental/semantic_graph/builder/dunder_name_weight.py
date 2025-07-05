from __future__ import annotations

import logging
from typing import Optional

from dbt_semantic_interfaces.enum_extension import assert_values_exhausted
from typing_extensions import override

from metricflow_semantics.experimental.semantic_graph.edges.edge_labels import MetricDefinitionLabel
from metricflow_semantics.experimental.semantic_graph.nodes.semantic_graph_node import (
    SemanticGraphEdge,
    SemanticGraphNode,
)
from metricflow_semantics.experimental.semantic_graph.path_finding.weight_function import WeightFunction
from metricflow_semantics.mf_logging.lazy_formattable import LazyFormat
from metricflow_semantics.model.linkable_element_property import LinkableElementProperty
from metricflow_semantics.model.semantics.linkable_element import LinkableElementType
from metricflow_semantics.model.semantics.semantic_model_join_evaluator import MAX_JOIN_HOPS

logger = logging.getLogger(__name__)

from metricflow_semantics.experimental.semantic_graph.attribute_resolution.attribute_computation_path import (
    AttributeRecipeWriterPath,
)


class DunderNameWeightFunction(WeightFunction[SemanticGraphNode, SemanticGraphEdge, AttributeRecipeWriterPath]):
    MAX_ENTITY_LINKS = MAX_JOIN_HOPS

    _METRIC_DEFINITION_LABEL = MetricDefinitionLabel()

    def __init__(self) -> None:  # noqa: D107
        self._verbose_debug_logs = True

    @override
    def incremental_weight(
        self, path_to_node: AttributeRecipeWriterPath, next_edge: SemanticGraphEdge
    ) -> Optional[int]:
        # Don't allow traversal of the metric definition edges unless the previous edge in the path was also a metric
        # definition edge. This prevents unnecessary traversal when searching for group-by items as it prevents
        # traversal from the metric node (which is a successor of the `JoinToModelNode` and represents a group-by
        # metric).
        path_edges = path_to_node.edges
        if len(path_edges) > 0 and DunderNameWeightFunction._METRIC_DEFINITION_LABEL in next_edge.labels:
            last_edge = path_edges[-1]
            if DunderNameWeightFunction._METRIC_DEFINITION_LABEL not in last_edge.labels:
                return None

        recipe_writer = path_to_node.recipe_writer
        current_recipe = recipe_writer.latest_recipe
        next_edge_update = next_edge.attribute_recipe_update
        next_node_update = next_edge.head_node.attribute_recipe_update
        next_attribute_recipe = current_recipe.with_update(next_edge_update)
        next_attribute_recipe = next_attribute_recipe.with_update(next_node_update)

        # dundered_name_elements = (
        #     current_attribute_descriptor.dundered_name_elements
        #     + mf_tuple_from_optional(next_edge.attribute_computation_update.dundered_name_element_addition)
        #     + mf_tuple_from_optional(
        #         next_edge.head_node.attribute_computation_update.dundered_name_element_addition
        #     )
        # )
        dundered_name_elements = next_attribute_recipe.dunder_name_elements
        # We do not allow repeated element names in the dundered name (e.g. `listing__listing`),
        # so return `None` to indicate a blocked edge.
        unique_dunder_name_element_count = len(set(dundered_name_elements))
        if unique_dunder_name_element_count != len(dundered_name_elements) and len(dundered_name_elements) > 1:
            logger.debug(
                LazyFormat(
                    "Blocking edge due to dunder name uniqueness.",
                    next_edge=next_edge,
                    dundered_name_elements=dundered_name_elements,
                )
            )
            return None

        # Don't allow joining a semantic model multiple times.
        # element_type = next_edge.head_node.attribute_computation_update.element_type_addition
        element_type = next_attribute_recipe.element_type

        model_ids_in_current_path = current_recipe.models_in_join
        if element_type is not LinkableElementType.METRIC:
            if len(next_attribute_recipe.models_in_join) != len(set(next_attribute_recipe.models_in_join)):
                if self._verbose_debug_logs:
                    logger.debug(
                        LazyFormat(
                            "Blocking edge due to a repeated model.",
                            next_edge=next_edge,
                            model_id_changes=next_attribute_recipe.models_in_join,
                        )
                    )
                return None

        # Require entity links for dimensions / time dimensions, except for metric time

        if element_type is not None:
            # Doing a max for the join count to allow this weight function to be used for paths that haven't yet
            # included a semantic model.
            join_count = max(0, len(next_attribute_recipe.models_in_join) - 1)
            min_entity_link_length = 1
            if join_count == 0:
                max_entity_link_length = 1
            else:
                max_entity_link_length = join_count

            if element_type is LinkableElementType.ENTITY:
                min_entity_link_length = 0
            elif element_type is LinkableElementType.TIME_DIMENSION or element_type is LinkableElementType.DIMENSION:
                if LinkableElementProperty.METRIC_TIME in next_attribute_recipe.properties:
                    min_entity_link_length = 0

            elif element_type is LinkableElementType.METRIC:
                # max_entity_link_length = join_count + 1
                pass
                # if len(next_attribute_descriptor.dsi_entity_names) > 2:
                #     if self._verbose_debug_logs:
                #         logger.debug(
                #             LazyFormat(
                #                 "Blocking edge entity link limit for metrics.",
                #                 next_edge=next_edge,
                #                 model_ids_in_current_path=model_ids_in_current_path,
                #                 dsi_entity_names=current_attribute_descriptor.dsi_entity_names,
                #             )
                #         )
                #     return None
                #
                #
                # if (
                #     # 1 < len(model_ids_in_current_path) <= len(current_attribute_descriptor.dsi_entity_names)
                #     len(model_ids_in_current_path) > 1
                #     and len(next_attribute_descriptor.dsi_entity_names) - 1 >= len(next_attribute_descriptor.model_ids) - 1
                #     # and next_attribute_descriptor.dundered_name_elements[-1] != METRIC_TIME_ELEMENT_NAME
                # ):
                #     if self._verbose_debug_logs:
                #         logger.debug(
                #             LazyFormat(
                #                 "Blocking edge due to non-shortest entity links.",
                #                 next_edge=next_edge,
                #                 model_ids_in_current_path=model_ids_in_current_path,
                #                 dsi_entity_names=current_attribute_descriptor.dsi_entity_names,
                #             )
                #         )
                #     return None
            else:
                assert_values_exhausted(element_type)

            next_entity_link_length = len(next_attribute_recipe.entity_link_names)
            if not (min_entity_link_length <= next_entity_link_length <= max_entity_link_length):
                if self._verbose_debug_logs:
                    logger.debug(
                        LazyFormat(
                            "Blocking edge due to entity links being outside of range.",
                            min_entity_link_length=min_entity_link_length,
                            next_entity_link_length=next_entity_link_length,
                            max_entity_link_length=max_entity_link_length,
                            next_edge=next_edge,
                            dsi_entity_names=current_recipe.entity_link_names,
                            model_ids_in_current_path=model_ids_in_current_path,
                        )
                    )
                return None

        # if next_attribute_descriptor.element_type is not None:
        #     if (
        #         # 1 < len(model_ids_in_current_path) <= len(current_attribute_descriptor.dsi_entity_names)
        #         len(next_attribute_descriptor.dsi_entity_names) > len(next_attribute_descriptor.model_ids)
        #         and current_attribute_descriptor.dundered_name_elements[-1] != METRIC_TIME_ELEMENT_NAME
        #     ):
        #         if self._verbose_debug_logs:
        #             logger.debug(
        #                 LazyFormat(
        #                     "Blocking edge due to non-shortest entity links.",
        #                     next_edge=next_edge,
        #                     model_ids_in_current_path=model_ids_in_current_path,
        #                     dsi_entity_names=current_attribute_descriptor.dsi_entity_names,
        #                 )
        #             )
        #         return None

        # if len(dsi_entity_names) > 1 and len(model_ids_in_current_path) == len(dsi_entity_names):
        # # if 1 < len(model_ids_in_current_path) <= len(dsi_entity_names):
        #     if self._verbose_debug_logs:
        #         logger.debug(
        #             LazyFormat(
        #                 "Blocking edge due to non-shortest entity links.",
        #                 next_edge=next_edge,
        #                 dsi_entity_names=current_attribute_descriptor.dsi_entity_names,
        #                 model_ids=model_ids_in_current_path,
        #             )
        #         )
        #     return None

        # if (
        #     GroupByAttributeLabel.get_instance() in next_edge.head_node.labels
        #     and len(path_to_node.attribute_computation.attribute_descriptor.dsi_entity_names) == 0
        #     and len(dundered_name_elements) > 1
        #     and dundered_name_elements[-2] != METRIC_TIME_ELEMENT_NAME
        # ):
        #     logger.debug(
        #         LazyFormat(
        #             "Blocking edge due to missing entity links.",
        #             next_edge=next_edge,
        #             dundered_name_elements=dundered_name_elements,
        #         )
        #     )
        #     return None

        # dsi_entity_name_additions = mf_tuple_from_optional(
        #     next_edge.attribute_computation_update.dsi_entity_addition
        # ) + mf_tuple_from_optional(next_edge.head_node.attribute_computation_update.dsi_entity_addition)

        weight_added_by_taking_edge = 0
        if next_edge_update.add_entity_link is not None:
            weight_added_by_taking_edge += 1
        if next_edge.head_node.attribute_recipe_update.add_entity_link is not None:
            weight_added_by_taking_edge += 1

        return weight_added_by_taking_edge
