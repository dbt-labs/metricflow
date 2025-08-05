from __future__ import annotations

import logging

from _pytest.fixtures import FixtureRequest
from dbt_semantic_interfaces.protocols import SemanticManifest
from metricflow_semantics.experimental.dsi.manifest_object_lookup import ManifestObjectLookup
from metricflow_semantics.experimental.mf_graph.path_finding.pathfinder import MetricflowPathfinder
from metricflow_semantics.experimental.ordered_set import FrozenOrderedSet
from metricflow_semantics.experimental.semantic_graph.attribute_resolution.recipe_writer_path import (
    RecipeWriterPathfinder,
)
from metricflow_semantics.experimental.semantic_graph.builder.graph_builder import SemanticGraphBuilder
from metricflow_semantics.experimental.semantic_graph.nodes.node_labels import MeasureLabel, MetricLabel
from metricflow_semantics.experimental.semantic_graph.trie_resolver.simple_resolver import SimpleTrieResolver
from metricflow_semantics.test_helpers.config_helpers import MetricFlowTestConfiguration
from metricflow_semantics.test_helpers.snapshot_helpers import assert_object_snapshot_equal

logger = logging.getLogger(__name__)


def test_measure(  # noqa: D103
    request: FixtureRequest,
    mf_test_configuration: MetricFlowTestConfiguration,
    sg_02_single_join_manifest: SemanticManifest,
) -> None:
    semantic_graph = SemanticGraphBuilder(ManifestObjectLookup(sg_02_single_join_manifest)).build()
    pathfinder: RecipeWriterPathfinder = MetricflowPathfinder()
    resolver = SimpleTrieResolver(semantic_graph=semantic_graph, path_finder=pathfinder)
    measure_node = semantic_graph.node_with_label(MeasureLabel.get_instance("booking_count"))
    result = resolver.resolve_trie(source_nodes=FrozenOrderedSet((measure_node,)), element_filter=None)

    assert_object_snapshot_equal(
        request=request,
        snapshot_configuration=mf_test_configuration,
        obj=sorted(result.dunder_name_trie.dunder_names()),
    )


def test_metrics(  # noqa: D103
    request: FixtureRequest,
    mf_test_configuration: MetricFlowTestConfiguration,
    sg_05_derived_metric_manifest: SemanticManifest,
) -> None:
    semantic_graph = SemanticGraphBuilder(ManifestObjectLookup(sg_05_derived_metric_manifest)).build()
    pathfinder: RecipeWriterPathfinder = MetricflowPathfinder()
    resolver = SimpleTrieResolver(semantic_graph=semantic_graph, path_finder=pathfinder)
    resolver._verbose_debug_logs = True

    bookings_metric = semantic_graph.nodes_with_labels(MetricLabel.get_instance("bookings"))
    views_metric = semantic_graph.nodes_with_labels(MetricLabel.get_instance("views"))
    views_per_booking_metric = semantic_graph.nodes_with_labels(MetricLabel.get_instance("bookings_per_view"))
    multiple_metric_nodes = semantic_graph.nodes_with_labels(
        MetricLabel.get_instance("bookings"), MetricLabel.get_instance("views")
    )

    obj = {
        source_nodes: sorted(resolver.resolve_trie(source_nodes, element_filter=None).dunder_name_trie.dunder_names())
        for source_nodes in (bookings_metric, views_metric, multiple_metric_nodes, views_per_booking_metric)
    }
    assert_object_snapshot_equal(request=request, snapshot_configuration=mf_test_configuration, obj=obj)
