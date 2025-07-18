from __future__ import annotations

import logging
from collections import defaultdict

from metricflow_semantics.collection_helpers.mf_type_aliases import AnyLengthTuple
from metricflow_semantics.experimental.dataclass_helpers import fast_frozen_dataclass
from metricflow_semantics.experimental.ordered_set import MutableOrderedSet
from metricflow_semantics.experimental.semantic_graph.attribute_resolution.attribute_recipe import AttributeQueryRecipe
from metricflow_semantics.experimental.semantic_graph.attribute_resolution.key_query_set import (
    KeyQueryGroup,
    KeyQueryGrouper,
)
from metricflow_semantics.experimental.semantic_graph.model_id import SemanticModelId
from metricflow_semantics.experimental.semantic_graph.nodes.node_labels import (
    ConfiguredEntityLabel,
    JoinedModelLabel,
    KeyAttributeLabel,
    KeyEntityLabel,
    LocalModelLabel,
)
from metricflow_semantics.experimental.semantic_graph.sg_interfaces import SemanticGraph, SemanticGraphNode
from metricflow_semantics.experimental.semantic_graph.weight.dunder_name_weight import DunderNameWeightFunction
from metricflow_semantics.mf_logging.lazy_formattable import LazyFormat

logger = logging.getLogger(__name__)


DunderNameTuple = AnyLengthTuple[str]


class EntityKeyQueryResolver:
    _MAX_ENTITY_LINKS = 1

    def __init__(self) -> None:  # noqa: D
        self._verbose_debug_logs = False

    def find_key_query_groups(
        self,
        semantic_graph: SemanticGraph,
    ) -> dict[SemanticGraphNode, KeyQueryGroup]:
        local_model_nodes = semantic_graph.nodes_with_label(LocalModelLabel.get_instance())
        key_attribute_nodes = semantic_graph.nodes_with_label(KeyAttributeLabel.get_instance())

        allowed_nodes = MutableOrderedSet(local_model_nodes)
        allowed_nodes.update(key_attribute_nodes)
        for label in (
            JoinedModelLabel.get_instance(),
            KeyEntityLabel.get_instance(),
            ConfiguredEntityLabel.get_instance(),
        ):
            allowed_nodes.update(semantic_graph.nodes_with_label(label))

        source_nodes = local_model_nodes
        target_nodes = key_attribute_nodes
        max_entity_links = EntityKeyQueryResolver._MAX_ENTITY_LINKS

        if self._verbose_debug_logs:
            logger.debug(
                LazyFormat(
                    "Starting path-finding for key queries",
                    source_nodes=source_nodes,
                    target_nodes=target_nodes,
                    allowed_nodes=allowed_nodes,
                )
            )

        notifications_to_process: dict[SemanticGraphNode, list[_FoundPathToNodeNotification]] = defaultdict(list)

        initial_recipe = AttributeQueryRecipe()
        for node in target_nodes:
            recipe = initial_recipe.push_step(node.recipe_step)
            for edge_from_predecessor in semantic_graph.edges_with_head_node(node):
                predecessor_node = edge_from_predecessor.tail_node
                recipe = recipe.push_steps(
                    edge_from_predecessor.recipe_step,
                    edge_from_predecessor.tail_node.recipe_step,
                )
                notifications_to_process[predecessor_node].append(
                    _FoundPathToNodeNotification(next_node=node, recipe=recipe)
                )

        found_source_to_target_path_notifications: dict[
            SemanticGraphNode, list[_FoundPathToNodeNotification]
        ] = defaultdict(list)

        step_index = 0
        while len(notifications_to_process) > 0:
            if self._verbose_debug_logs:
                logger.debug(
                    LazyFormat(
                        "Processing step", step_index=step_index, notifications_to_process=notifications_to_process
                    )
                )

            next_notifications_to_process: dict[SemanticGraphNode, list[_FoundPathToNodeNotification]] = defaultdict(
                list
            )

            for current_node, found_new_path_notifications in notifications_to_process.items():
                if current_node in source_nodes:
                    found_source_to_target_path_notifications[current_node].extend(found_new_path_notifications)
                    continue

                for new_path_notification in found_new_path_notifications:
                    recipe_at_current_node = new_path_notification.recipe

                    for edge_from_predecessor in semantic_graph.edges_with_head_node(current_node):
                        predecessor_node = edge_from_predecessor.tail_node
                        recipe_at_predecessor = recipe_at_current_node.push_steps(
                            edge_from_predecessor.recipe_step,
                            edge_from_predecessor.tail_node.recipe_step,
                        )
                        # dunder_name_elements = recipe_at_predecessor.dunder_name_elements

                        weight = DunderNameWeightFunction.recipe_weight(recipe_at_predecessor)

                        if weight is not None and weight <= max_entity_links:
                            next_notifications_to_process[predecessor_node].append(
                                _FoundPathToNodeNotification(next_node=current_node, recipe=recipe_at_predecessor)
                            )
            notifications_to_process = next_notifications_to_process
            step_index += 1

        source_node_to_query_grouper: dict[SemanticGraphNode, KeyQueryGrouper] = defaultdict(KeyQueryGrouper)
        for source_node, notifications in found_source_to_target_path_notifications.items():
            found_entity_key_queries: MutableOrderedSet[DunderNameTuple] = MutableOrderedSet()
            ambiguous_entity_key_queries: MutableOrderedSet[DunderNameTuple] = MutableOrderedSet()

            key_query_to_queried_model_ids: dict[DunderNameTuple, AnyLengthTuple[SemanticModelId]] = {}
            for notification in notifications:
                key_query = notification.recipe.dunder_name_elements
                if key_query in ambiguous_entity_key_queries:
                    continue
                if key_query in found_entity_key_queries:
                    ambiguous_entity_key_queries.add(key_query)
                    found_entity_key_queries.discard(key_query)
                found_entity_key_queries.add(key_query)
                key_query_to_queried_model_ids[key_query] = notification.recipe.models_in_join

            for key_query in found_entity_key_queries:
                queried_model_ids = key_query_to_queried_model_ids[key_query]
                source_node_to_query_grouper[source_node].add(key_query, queried_model_ids)

        return {source_node: source_node_to_query_grouper[source_node].group() for source_node in source_nodes}


@fast_frozen_dataclass()
class _FoundPathToNodeNotification:
    next_node: SemanticGraphNode
    recipe: AttributeQueryRecipe
