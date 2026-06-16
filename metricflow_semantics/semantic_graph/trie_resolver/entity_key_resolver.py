from __future__ import annotations

import logging
from collections import defaultdict
from collections.abc import Mapping
from typing import Set

from metricflow_semantics.model.semantics.linkable_element import LinkableElementType
from metricflow_semantics.model.semantics.semantic_model_join_evaluator import MAX_JOIN_HOPS
from metricflow_semantics.semantic_graph.attribute_resolution.attribute_recipe import (
    AttributeRecipe,
    IndexedDunderName,
)
from metricflow_semantics.semantic_graph.attribute_resolution.recipe_writer_weight import (
    AttributeRecipeWriterWeightFunction,
)
from metricflow_semantics.semantic_graph.nodes.node_labels import (
    ConfiguredEntityLabel,
    JoinedModelLabel,
    KeyAttributeLabel,
    LocalModelLabel,
)
from metricflow_semantics.semantic_graph.sg_interfaces import SemanticGraph, SemanticGraphNode
from metricflow_semantics.semantic_graph.trie_resolver.dunder_name_descriptor import DunderNameDescriptor
from metricflow_semantics.semantic_graph.trie_resolver.dunder_name_trie import (
    DunderNameTrie,
    MutableDunderNameTrie,
)
from metricflow_semantics.toolkit.dataclass_helpers import fast_frozen_dataclass
from metricflow_semantics.toolkit.mf_logging.lazy_formattable import LazyFormat
from metricflow_semantics.toolkit.mf_type_aliases import AnyLengthTuple
from metricflow_semantics.toolkit.performance_helpers import ExecutionTimer

logger = logging.getLogger(__name__)


DunderNameTuple = AnyLengthTuple[str]


class EntityKeyTrieResolver:
    """A resolver to figure out the possible group-by items that are entity-keys.

     i.e. the available items when the user wants to `query for an entity`.

    For example, the `bookings` metric could be queried by the following entity keys (if you include entity links):

        booking
        host
        guest
        booking__listing
        listing__user
        ...

    The entity-key trie represents all possible group-by items that represent an entity key.

    The entity-key trie is needed to resolve the available group-by metrics for a query. Group-by metrics
    are modeled as a join using a subquery where the group-by item is the entity key. e.g. for the query (`bookings` by
    `listing__views` ), the query (`bookings` by `listing`) is joined with the subquery (`views` by `listing`) on
    the entity key.
    """

    def __init__(self, semantic_graph: SemanticGraph) -> None:  # noqa: D107
        self._verbose_debug_logs = False
        self._semantic_graph = semantic_graph

    def resolve_entity_key_trie_mapping(self) -> Mapping[SemanticGraphNode, DunderNameTrie]:
        """Returns a mapping from the local-model to the associated entity-key trie.

        This is returned as single mapping because to resolve the group-by metrics for any measure, you need them all
        anyway.
        """
        with ExecutionTimer() as resolution_timer:
            result = self._resolve_entity_key_trie_mapping()
        logger.info(LazyFormat("Resolved entity-key trie mapping", duration=resolution_timer.total_duration))
        return result

    def _resolve_entity_key_trie_mapping(self) -> Mapping[SemanticGraphNode, DunderNameTrie]:
        local_model_nodes = self._semantic_graph.nodes_with_labels(LocalModelLabel.get_instance())
        key_attribute_nodes = self._semantic_graph.nodes_with_labels(KeyAttributeLabel.get_instance())
        joined_model_nodes = self._semantic_graph.nodes_with_labels(JoinedModelLabel.get_instance())
        configured_entity_nodes = self._semantic_graph.nodes_with_labels(ConfiguredEntityLabel.get_instance())

        allowed_nodes: Set[SemanticGraphNode] = set()
        allowed_nodes.update(local_model_nodes, key_attribute_nodes, joined_model_nodes, configured_entity_nodes)

        source_nodes = local_model_nodes
        target_nodes = key_attribute_nodes
        max_entity_links = MAX_JOIN_HOPS

        if self._verbose_debug_logs:
            logger.debug(
                LazyFormat(
                    "Starting path-finding for key queries",
                    source_nodes=source_nodes,
                    target_nodes=target_nodes,
                    allowed_nodes=allowed_nodes,
                )
            )

        # We're looking to find all possible paths from the local-model nodes (the source nodes) to the entity-key
        # nodes (the target nodes).
        # To do this efficiently, this uses a method similar to Dijkstra's algorithm. For each node, we maintain
        # a lookup that records the next possible node on paths that lead to entity-key nodes. By starting from the
        # target nodes (instead of the source nodes), we can ensure that by the end of the exploration, the lookup
        # only contains valid paths from source nodes to target nodes.
        semantic_graph = self._semantic_graph
        notifications_to_process: dict[SemanticGraphNode, list[_FoundPathToNodeNotification]] = defaultdict(list)
        initial_recipe = AttributeRecipe()

        # Since we're starting at the target node and going backwards, initially populate the lookup for the target
        # nodes.
        for node in target_nodes:
            recipe_at_node = initial_recipe.push_step(node.recipe_step_to_append)
            for edge_from_predecessor in semantic_graph.edges_with_head_node(node):
                predecessor_node = edge_from_predecessor.tail_node
                recipe_at_predecessor_node = recipe_at_node.push_steps(
                    edge_from_predecessor.recipe_step_to_append,
                    edge_from_predecessor.tail_node.recipe_step_to_append,
                )
                notification = _FoundPathToNodeNotification(next_node=node, recipe=recipe_at_predecessor_node)

                if self._verbose_debug_logs:
                    logger.debug(
                        LazyFormat(
                            "Adding initial notification",
                            predecessor_node=predecessor_node,
                            notification=notification,
                        )
                    )
                notifications_to_process[predecessor_node].append(notification)

        found_path_to_node_notifications: dict[SemanticGraphNode, list[_FoundPathToNodeNotification]] = defaultdict(
            list
        )

        # At each step, explore the predecessors of the nodes that were discovered in the previous step. Repeat until.
        # there are no more new nodes / new paths discovered.
        logger.info(
            LazyFormat(
                "Starting exploration for entity-key paths",
                source_node_count=len(source_nodes),
                target_node_count=len(target_nodes),
            )
        )
        step_index = 0
        processed_notification_count = 0
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
                processed_notification_count += len(found_new_path_notifications)
                if current_node in source_nodes:
                    found_path_to_node_notifications[current_node].extend(found_new_path_notifications)
                    continue

                for new_path_notification in found_new_path_notifications:
                    recipe_at_current_node = new_path_notification.recipe

                    for edge_from_predecessor in semantic_graph.edges_with_head_node(current_node):
                        predecessor_node = edge_from_predecessor.tail_node

                        tail_node_step = edge_from_predecessor.tail_node.recipe_step_to_append
                        edge_step = edge_from_predecessor.recipe_step_to_append

                        if AttributeRecipeWriterWeightFunction.repeated_dunder_name_elements(
                            recipe=recipe_at_current_node, step=tail_node_step, other_step=edge_step
                        ) or AttributeRecipeWriterWeightFunction.repeated_model_join(
                            recipe=recipe_at_current_node, step=tail_node_step, other_step=edge_step
                        ):
                            continue

                        next_entity_link_count = (
                            len(recipe_at_current_node.entity_link_names)
                            + (1 if tail_node_step.add_entity_link else 0)
                            + (1 if edge_step.add_entity_link else 0)
                        )

                        if next_entity_link_count > max_entity_links:
                            continue

                        # Handle a special case where entities can be queried by an entity link prefix in the same
                        # semantic model, but not when a part of a join.
                        next_model_count = (
                            len(recipe_at_current_node.joined_model_ids)
                            + (1 if tail_node_step.add_model_join else 0)
                            + (1 if edge_step.add_model_join else 0)
                        )
                        if next_model_count > 1 and next_entity_link_count > (next_model_count - 1):
                            continue

                        recipe_at_predecessor = recipe_at_current_node.push_steps(
                            edge_from_predecessor.recipe_step_to_append,
                            edge_from_predecessor.tail_node.recipe_step_to_append,
                        )

                        notification = _FoundPathToNodeNotification(
                            next_node=current_node, recipe=recipe_at_predecessor
                        )
                        if self._verbose_debug_logs:
                            logger.debug(
                                LazyFormat(
                                    "Adding notification",
                                    predecessor_node=predecessor_node,
                                    notification=notification,
                                )
                            )
                        next_notifications_to_process[predecessor_node].append(notification)
            notifications_to_process = next_notifications_to_process
            step_index += 1
        logger.info(
            LazyFormat(
                "Finished exploration for entity-key paths",
                step_index=step_index,
                processed_notification_count=processed_notification_count,
            )
        )

        # Once the above loop has finished, the lookup can be used to figure out the possible path (encoded in the
        # recipe).
        source_node_to_trie: dict[SemanticGraphNode, MutableDunderNameTrie] = defaultdict(MutableDunderNameTrie)
        added_name_item_count = 0
        for source_node, notifications in found_path_to_node_notifications.items():
            items_to_add: list[tuple[IndexedDunderName, DunderNameDescriptor]] = []
            for notification in notifications:
                recipe = notification.recipe
                indexed_dunder_name = recipe.indexed_dunder_name
                last_model_id = recipe.last_model_id
                items_to_add.append(
                    (
                        indexed_dunder_name,
                        DunderNameDescriptor(
                            element_type=LinkableElementType.ENTITY,
                            time_grain=None,
                            date_part=None,
                            element_properties=(),
                            origin_model_ids=(last_model_id,) if last_model_id else (),
                            derived_from_model_ids=notification.recipe.joined_model_ids,
                            entity_key_queries_for_group_by_metric=(),
                        ),
                    )
                )

            source_node_to_trie[source_node].add_name_items(items_to_add)
            added_name_item_count += len(items_to_add)
        logger.info(LazyFormat("Finished mapping source node to entity-key trie", source_node_count=len(source_nodes)))
        return {source_node: source_node_to_trie[source_node] for source_node in source_nodes}


@fast_frozen_dataclass()
class _FoundPathToNodeNotification:
    """The value for the keys in the dict that maps a node to the next node in the path (to a target node)."""

    next_node: SemanticGraphNode
    recipe: AttributeRecipe
