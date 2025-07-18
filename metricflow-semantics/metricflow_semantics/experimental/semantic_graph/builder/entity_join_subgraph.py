from __future__ import annotations

import logging

from typing_extensions import override

from metricflow_semantics.experimental.dsi.manifest_object_lookup import SemanticModelJoinLookup
from metricflow_semantics.experimental.dsi.model_object_lookup import (
    SemanticModelObjectLookup,
)
from metricflow_semantics.experimental.semantic_graph.builder.subgraph_generator import (
    SemanticSubgraphGenerator,
    SubgraphGeneratorArgumentSet,
)
from metricflow_semantics.experimental.semantic_graph.edges.sg_edges import JoinFromModelEdge, JoinToModelEdge
from metricflow_semantics.experimental.semantic_graph.model_id import SemanticModelId
from metricflow_semantics.experimental.semantic_graph.nodes.entity_nodes import (
    ConfiguredEntityNode,
    JoinedModelNode,
    LocalModelNode,
)
from metricflow_semantics.experimental.semantic_graph.sg_interfaces import MutableSemanticGraph, SemanticGraph

logger = logging.getLogger(__name__)


class EntityJoinSubgraphGenerator(SemanticSubgraphGenerator):
    def __init__(self, argument_set: SubgraphGeneratorArgumentSet) -> None:
        super().__init__(argument_set)
        self._verbose_debug_logs = True
        self._join_lookup = SemanticModelJoinLookup(self._manifest_object_lookup.model_object_lookups)

        self._valid_entity_join_types = SemanticModelJoinLookup.valid_join_to_entity_types()

    def _get_subgraph_for_model(self, lookup: SemanticModelObjectLookup) -> MutableSemanticGraph:
        current_subgraph = MutableSemanticGraph.create()

        left_model_id = SemanticModelId.get_instance(model_name=lookup.semantic_model.name)
        left_model_node = JoinedModelNode.get_instance(left_model_id)
        left_local_model_node = LocalModelNode.get_instance(left_model_id)
        left_model = lookup.semantic_model

        for right_model_id, join_descriptors in self._join_lookup.get_join_model_on_right_descriptors(
            left_model_id=left_model_id
        ).items():
            for join_descriptor in join_descriptors:
                right_entity_node = ConfiguredEntityNode.get_instance(
                    entity_name=join_descriptor.entity_name,
                    model_id=right_model_id,
                )
                current_subgraph.add_edge(
                    JoinFromModelEdge.get_instance(tail_node=left_model_node, head_node=right_entity_node)
                )
                current_subgraph.add_edge(
                    JoinFromModelEdge.get_instance(
                        tail_node=left_local_model_node,
                        head_node=right_entity_node,
                    )
                )

        for entity in left_model.entities:
            if entity.type in self._valid_entity_join_types:
                current_subgraph.add_edge(
                    JoinToModelEdge.get_instance(
                        tail_node=ConfiguredEntityNode.get_instance(
                            entity_name=entity.name,
                            model_id=left_model_id,
                        ),
                        head_node=left_model_node,
                    )
                )

        primary_entity_name = lookup.primary_entity_name
        if primary_entity_name is not None:
            primary_entity_node = ConfiguredEntityNode.get_instance(
                entity_name=primary_entity_name,
                model_id=left_model_id,
            )
            current_subgraph.add_edge(
                JoinToModelEdge.get_instance(
                    tail_node=primary_entity_node,
                    head_node=left_model_node,
                )
            )
            current_subgraph.add_edge(
                JoinFromModelEdge.get_instance(
                    tail_node=left_local_model_node,
                    head_node=primary_entity_node,
                )
            )
        return current_subgraph

    @override
    def generate_subgraph(self, predecessor_graph: SemanticGraph) -> MutableSemanticGraph:
        current_subgraph = MutableSemanticGraph.create()

        for lookup in self._manifest_object_lookup.model_object_lookups:
            subgraph_from_model = self._get_subgraph_for_model(lookup)
            current_subgraph.update(subgraph_from_model)

        return current_subgraph
