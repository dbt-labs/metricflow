from __future__ import annotations

import logging

from _pytest.fixtures import FixtureRequest
from dbt_semantic_interfaces.implementations.semantic_manifest import PydanticSemanticManifest
from metricflow_semantics.experimental.mf_graph.graph_formatter import GraphTextFormatter
from metricflow_semantics.experimental.semantic_graph.builder.in_progress_semantic_graph import InProgressSemanticGraph
from metricflow_semantics.experimental.semantic_graph.graph_nodes import (
    ElementEntityNode,
    EntityNode,
    SemanticEntityType,
)
from metricflow_semantics.mf_logging.lazy_formattable import LazyFormat
from metricflow_semantics.mf_logging.pretty_print import mf_pformat
from metricflow_semantics.model.semantic_manifest_lookup import SemanticManifestLookup
from metricflow_semantics.test_helpers.config_helpers import MetricFlowTestConfiguration

from tests_metricflow_semantics.experimental.svg_graph import display_graph_if_requested

logger = logging.getLogger(__name__)


# def test_semantic_graph(  # noqa: D103
#     mf_test_configuration: MetricFlowTestConfiguration,
#     request: FixtureRequest,
# ) -> None:
#     listing_reference = EntityReference("listing")
#     node_0 = EntityNode(EntityReference("booking"))
#     node_1 = EntityNode(listing_reference)
#     bookings_source = SemanticModelReference("bookings_source")
#     listings_source = SemanticModelReference("listings_source")
#     semantic_graph = SemanticGraph.create(
#         nodes=[node_0, node_1],
#         edges=[
#             SemanticGraphEdge(
#                 tail_node=node_0,
#                 edge_type=SemanticGraphEdgeType.ONE_TO_ONE,
#                 head_node=node_1,
#                 computation_method=(
#                     JoinedComputationMethod(
#                         left_semantic_model_reference=bookings_source,
#                         right_semantic_model_reference=listings_source,
#                         join_on_entity=listing_reference,
#                     )
#                 ),
#                 provided_tags=ProvidedEdgeTagSet.empty_set(),
#                 required_tags=RequiredTagSet.empty_set(),
#             ),
#             SemanticGraphEdge(
#                 tail_node=node_1,
#                 edge_type=SemanticGraphEdgeType.ONE_TO_MANY,
#                 head_node=node_0,
#                 computation_method=(
#                     JoinedComputationMethod(
#                         left_semantic_model_reference=listings_source,
#                         right_semantic_model_reference=bookings_source,
#                         join_on_entity=listing_reference,
#                     )
#                 ),
#                 provided_tags=ProvidedEdgeTagSet.empty_set(),
#                 required_tags=RequiredTagSet.empty_set(),
#             ),
#         ],
#     )
#
#     formatting = DotNotationFormatter()
#     logger.info(formatting.dot_format(semantic_graph))
#     display_graph_if_requested(mf_test_configuration, request, semantic_graph)


def test_semantic_graph_from_manifest(  # noqa: D103
    mf_test_configuration: MetricFlowTestConfiguration,
    request: FixtureRequest,
    semantic_graph_manifest: PydanticSemanticManifest,
    semantic_graph_manifest_lookup: SemanticManifestLookup,
) -> None:
    semantic_graph = InProgressSemanticGraph.create()
    semantic_graph_model_lookup = semantic_graph_manifest_lookup.semantic_model_lookup

    semantic_graph.add_node(ElementEntityNode.get_instance())
    recipes = (
        # AddEntitiesRule(semantic_graph_manifest, semantic_graph_model_lookup),
        # AddTimeNodesRecipe(semantic_graph_manifest, semantic_graph_model_lookup),
        # AddCoLocatedEntityEdgesRule(semantic_graph_manifest, semantic_graph_model_lookup),
        # AddJoinedEntityEdgesRule(semantic_graph_manifest, semantic_graph_model_lookup),
        # AddMeasureAttributeNodes(semantic_graph_manifest, semantic_graph_model_lookup),
        # AddNodesForDimensionsRecipe(semantic_graph_manifest, semantic_graph_model_lookup),
        # AddMetricNodesRecipe(semantic_graph_manifest, semantic_graph_model_lookup)
    )

    for recipe in recipes:
        recipe.execute_recipe(semantic_graph)

    semantic_graph = semantic_graph.as_semantic_graph()

    formatter = GraphTextFormatter()
    logger.info(formatter.dot_format(semantic_graph))
    display_graph_if_requested(mf_test_configuration, request, semantic_graph)

    # start_node = MetricAttributeNode(MetricReference("bookings"))
    # end_node = EntityValueAttributeNode(EntityReference("user"))
    # path_finder = SemanticGraphPathFinder(semantic_graph)
    #
    # with log_block_runtime("Find paths"):
    #     found_paths = path_finder.find_possible_paths(
    #         start_node=start_node,
    #         end_node=end_node,
    #         allowed_edge_types=SemanticGraphEdgeTypeSet.ENTITY_RELATIONSHIP.union(
    #             {SemanticGraphEdgeType.ATTRIBUTE_SOURCE, SemanticGraphEdgeType.ATTRIBUTE}
    #         )
    #     )
    # logger.info(LazyFormat("Found paths", found_paths=found_paths))


def test_format_entity_node() -> None:
    entity_node = EntityNode.get_instance("foo", SemanticEntityType.ENTITY)
    logger.debug(mf_pformat(entity_node))
    logger.debug(LazyFormat("Entity node.", entity_node=entity_node))
