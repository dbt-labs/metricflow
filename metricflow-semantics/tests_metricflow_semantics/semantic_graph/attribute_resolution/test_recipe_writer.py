from __future__ import annotations

import logging

import tabulate
from _pytest.fixtures import FixtureRequest
from dbt_semantic_interfaces.naming.keywords import DUNDER
from dbt_semantic_interfaces.protocols import SemanticManifest
from metricflow_semantics.semantic_graph.attribute_resolution.recipe_writer_path import (
    AttributeRecipeWriterPath,
    RecipeWriterPathfinder,
)
from metricflow_semantics.semantic_graph.attribute_resolution.recipe_writer_weight import (
    AttributeRecipeWriterWeightFunction,
)
from metricflow_semantics.semantic_graph.builder.graph_builder import SemanticGraphBuilder
from metricflow_semantics.semantic_graph.lookups.manifest_object_lookup import ManifestObjectLookup
from metricflow_semantics.semantic_graph.nodes.node_labels import (
    GroupByAttributeLabel,
    MetricLabel,
)
from metricflow_semantics.test_helpers.config_helpers import MetricFlowTestConfiguration
from metricflow_semantics.test_helpers.snapshot_helpers import (
    assert_str_snapshot_equal,
)
from metricflow_semantics.toolkit.mf_graph.path_finding.pathfinder import MetricFlowPathfinder
from metricflow_semantics.toolkit.mf_logging.pretty_print import mf_pformat
from metricflow_semantics.toolkit.mf_type_aliases import AnyLengthTuple

logger = logging.getLogger(__name__)


def test_recipe_writer_path(
    request: FixtureRequest,
    mf_test_configuration: MetricFlowTestConfiguration,
    sg_02_single_join_manifest: SemanticManifest,
) -> None:
    """Test generating recipes by traversing the semantic graph."""
    semantic_graph = SemanticGraphBuilder(ManifestObjectLookup(sg_02_single_join_manifest)).build()
    path_finder: RecipeWriterPathfinder = MetricFlowPathfinder()

    # Find all valid paths from the `bookings` simple metric to any group-by attribute node.
    source_node = semantic_graph.node_with_label(MetricLabel.get_instance("bookings"))
    target_nodes = semantic_graph.nodes_with_labels(GroupByAttributeLabel.get_instance())

    found_paths: list[AttributeRecipeWriterPath] = []
    for path in path_finder.find_paths_dfs(
        graph=semantic_graph,
        initial_path=AttributeRecipeWriterPath.create(source_node),
        target_nodes=target_nodes,
        weight_function=AttributeRecipeWriterWeightFunction(),
        max_path_weight=2,
    ):
        found_paths.append(path.copy())

    # Produce a table showing how the path relates to the dunder name and the recipe.
    table_headers = ("Path", "Dunder Name", "Recipe")
    table_rows: list[AnyLengthTuple[str]] = []

    for path in found_paths:
        path_str = "\n-> ".join(node.node_descriptor.node_name for node in path.nodes)
        dunder_name = DUNDER.join(path.latest_recipe.indexed_dunder_name)
        table_rows.append((path_str, dunder_name, mf_pformat(path.latest_recipe)))

    assert_str_snapshot_equal(
        request=request,
        snapshot_configuration=mf_test_configuration,
        snapshot_str=tabulate.tabulate(
            headers=table_headers,
            tabular_data=table_rows,
        ),
    )
