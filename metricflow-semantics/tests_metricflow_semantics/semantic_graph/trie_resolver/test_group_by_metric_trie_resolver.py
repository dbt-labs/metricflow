from __future__ import annotations

import logging

from _pytest.fixtures import FixtureRequest
from dbt_semantic_interfaces.protocols import SemanticManifest
from metricflow_semantics.semantic_graph.attribute_resolution.group_by_item_set import (
    GroupByItemSet,
)
from metricflow_semantics.semantic_graph.attribute_resolution.recipe_writer_path import (
    RecipeWriterPathfinder,
)
from metricflow_semantics.semantic_graph.builder.graph_builder import SemanticGraphBuilder
from metricflow_semantics.semantic_graph.lookups.manifest_object_lookup import ManifestObjectLookup
from metricflow_semantics.semantic_graph.nodes.node_labels import MetricLabel
from metricflow_semantics.semantic_graph.trie_resolver.group_by_metric_resolver import (
    GroupByMetricTrieResolver,
)
from metricflow_semantics.test_helpers.config_helpers import MetricFlowTestConfiguration
from metricflow_semantics.test_helpers.snapshot_helpers import assert_object_snapshot_equal
from metricflow_semantics.toolkit.collections.ordered_set import FrozenOrderedSet
from metricflow_semantics.toolkit.mf_graph.path_finding.pathfinder import MetricFlowPathfinder

logger = logging.getLogger(__name__)


def test_simple_metric(  # noqa: D103
    request: FixtureRequest,
    mf_test_configuration: MetricFlowTestConfiguration,
    sg_05_derived_metric_manifest: SemanticManifest,
) -> None:
    semantic_graph = SemanticGraphBuilder(ManifestObjectLookup(sg_05_derived_metric_manifest)).build()
    pathfinder: RecipeWriterPathfinder = MetricFlowPathfinder()
    resolver = GroupByMetricTrieResolver(semantic_graph=semantic_graph, path_finder=pathfinder)
    simple_metric_node = semantic_graph.node_with_label(MetricLabel.get_instance("bookings"))
    result = resolver.resolve_trie(source_nodes=FrozenOrderedSet((simple_metric_node,)), element_filter=None)

    assert_object_snapshot_equal(
        request=request,
        snapshot_configuration=mf_test_configuration,
        obj=GroupByItemSet.create_from_trie(result.dunder_name_trie),
    )
