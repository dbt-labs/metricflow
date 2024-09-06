from __future__ import annotations

import logging

from _pytest.fixtures import FixtureRequest
from dbt_semantic_interfaces.implementations.semantic_manifest import PydanticSemanticManifest
from dbt_semantic_interfaces.references import EntityReference, SemanticModelReference
from metricflow_semantics.experimental.semantic_graph.builder.in_progress_semantic_graph import InProgressSemanticGraph
from metricflow_semantics.experimental.semantic_graph.builder.rules.add_co_located_entity_edges import (
    AddCoLocatedEntityEdgesRule,
)
from metricflow_semantics.experimental.semantic_graph.builder.rules.add_dimension_nodes import AddNodesForDimensionsRecipe
from metricflow_semantics.experimental.semantic_graph.builder.rules.add_entities import AddEntitiesRule
from metricflow_semantics.experimental.semantic_graph.builder.rules.add_joined_entities import AddJoinedEntityEdgesRule
from metricflow_semantics.experimental.semantic_graph.builder.rules.add_measures import AddMeasureAttributeNodes
from metricflow_semantics.experimental.semantic_graph.builder.rules.add_time_nodes import AddTimeNodes
from metricflow_semantics.experimental.semantic_graph.computation_method import (
    JoinedComputationMethod,
)
from metricflow_semantics.experimental.semantic_graph.dot_notation_formatter import DotNotationFormatter
from metricflow_semantics.experimental.semantic_graph.graph_edges import (
    ProvidedEdgeTagSet,
    RequiredTagSet,
    SemanticGraphEdge,
    SemanticGraphEdgeType,
)
from metricflow_semantics.experimental.semantic_graph.graph_nodes import EntityNode
from metricflow_semantics.experimental.semantic_graph.semantic_graph import SemanticGraph
from metricflow_semantics.mf_logging.lazy_formattable import LazyFormat
from metricflow_semantics.mf_logging.pretty_print import mf_pformat
from metricflow_semantics.model.semantic_manifest_lookup import SemanticManifestLookup
from metricflow_semantics.test_helpers.config_helpers import MetricFlowTestConfiguration

from tests_metricflow_semantics.experimental.svg_graph import display_graph_if_requested

logger = logging.getLogger(__name__)


def test_semantic_graph(  # noqa: D103
    mf_test_configuration: MetricFlowTestConfiguration,
    request: FixtureRequest,
) -> None:
    listing_reference = EntityReference("listing")
    node_0 = EntityNode(EntityReference("booking"))
    node_1 = EntityNode(listing_reference)
    bookings_source = SemanticModelReference("bookings_source")
    listings_source = SemanticModelReference("listings_source")
    semantic_graph = SemanticGraph.create(
        nodes=[node_0, node_1],
        edges=[
            SemanticGraphEdge(
                tail_node=node_0,
                edge_type=SemanticGraphEdgeType.ONE_TO_ONE,
                head_node=node_1,
                computation_method=(
                    JoinedComputationMethod(
                        left_semantic_model_reference=bookings_source,
                        right_semantic_model_reference=listings_source,
                        on_entity_reference=listing_reference,
                    )
                ),
                provided_tags=ProvidedEdgeTagSet.empty_set(),
                required_tags=RequiredTagSet.empty_set(),
            ),
            SemanticGraphEdge(
                tail_node=node_1,
                edge_type=SemanticGraphEdgeType.ONE_TO_MANY,
                head_node=node_0,
                computation_method=(
                    JoinedComputationMethod(
                        left_semantic_model_reference=listings_source,
                        right_semantic_model_reference=bookings_source,
                        on_entity_reference=listing_reference,
                    )
                ),
                provided_tags=ProvidedEdgeTagSet.empty_set(),
                required_tags=RequiredTagSet.empty_set(),
            ),
        ],
    )

    formatter = DotNotationFormatter()
    logger.info(formatter.dot_format(semantic_graph))
    display_graph_if_requested(mf_test_configuration, request, semantic_graph)


def test_semantic_graph_from_manifest(  # noqa: D103
    mf_test_configuration: MetricFlowTestConfiguration,
    request: FixtureRequest,
    semantic_graph_manifest: PydanticSemanticManifest,
    semantic_graph_manifest_lookup: SemanticManifestLookup,
) -> None:
    in_progress_semantic_graph = InProgressSemanticGraph.create()
    semantic_graph_model_lookup = semantic_graph_manifest_lookup.semantic_model_lookup
    transform_rules = (
        AddEntitiesRule(semantic_graph_manifest, semantic_graph_model_lookup),
        AddTimeNodes(semantic_graph_manifest, semantic_graph_model_lookup),
        AddCoLocatedEntityEdgesRule(semantic_graph_manifest, semantic_graph_model_lookup),
        AddJoinedEntityEdgesRule(semantic_graph_manifest, semantic_graph_model_lookup),
        AddMeasureAttributeNodes(semantic_graph_manifest, semantic_graph_model_lookup),
        AddNodesForDimensionsRecipe(semantic_graph_manifest, semantic_graph_model_lookup),
    )

    for transform_rule in transform_rules:
        transform_rule.execute_recipe(in_progress_semantic_graph)

    semantic_graph = in_progress_semantic_graph.as_semantic_graph()

    formatter = DotNotationFormatter()
    logger.info(formatter.dot_format(semantic_graph))
    display_graph_if_requested(mf_test_configuration, request, semantic_graph)


def test_format_entity_node() -> None:
    entity_node = EntityNode(EntityReference("foo"))
    logger.debug(mf_pformat(entity_node))
    logger.debug(LazyFormat("Entity node.", entity_node=entity_node))
