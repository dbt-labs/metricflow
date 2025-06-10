from __future__ import annotations

import logging

from _pytest.fixtures import FixtureRequest
from metricflow_semantics.experimental.semantic_graph.attribute_resolution.attribute_computation_path import (
    AttributeComputationPath,
)
from metricflow_semantics.experimental.semantic_graph.attribute_resolution.attribute_resolver import (
    AttributeResolver,
)
from metricflow_semantics.experimental.semantic_graph.builder.graph_builder import SemanticGraphBuilder
from metricflow_semantics.experimental.semantic_graph.builder.group_by_attribute_subgraph import (
    GroupByAttributeSubgraphGenerator,
)
from metricflow_semantics.experimental.semantic_graph.manifest_object_lookup import ManifestObjectLookup
from metricflow_semantics.experimental.semantic_graph.model_id import SemanticModelId
from metricflow_semantics.experimental.semantic_graph.nodes.attribute_node import MeasureNode, MetricAttributeNode
from metricflow_semantics.experimental.semantic_graph.nodes.node_label import (
    DsiEntityLabel,
    GroupByAttributeLabel,
    MeasureAttributeLabel,
)
from metricflow_semantics.experimental.semantic_graph.nodes.semantic_graph_node import (
    SemanticGraphEdge,
    SemanticGraphNode,
)
from metricflow_semantics.experimental.semantic_graph.path_finding.path_finder import (
    MetricflowGraphPathFinder,
)
from metricflow_semantics.experimental.semantic_graph.path_finding.path_finder_cache import PathFinderCache
from metricflow_semantics.experimental.semantic_graph.path_finding.weight_function import EdgeCountWeightFunction
from metricflow_semantics.mf_logging.lazy_formattable import LazyFormat
from metricflow_semantics.test_helpers.config_helpers import MetricFlowTestConfiguration
from metricflow_semantics.test_helpers.snapshot_helpers import assert_object_snapshot_equal, assert_str_snapshot_equal
from metricflow_semantics.test_helpers.svg_snapshot import write_svg_snapshot_for_review

from tests_metricflow_semantics.experimental.mf_graph.formatting.svg_formatter import SvgFormatter
from tests_metricflow_semantics.experimental.semantic_graph.builder.subgraph_generator.conftest import (
    check_subgraph_generation,
)

logger = logging.getLogger(__name__)


def test_all(  # noqa: D103
    request: FixtureRequest,
    mf_test_configuration: MetricFlowTestConfiguration,
    sg_02_single_join_lookup: ManifestObjectLookup,
) -> None:
    check_subgraph_generation(
        request=request,
        mf_test_configuration=mf_test_configuration,
        manifest_object_lookup=sg_02_single_join_lookup,
        subgraph_generators=SemanticGraphBuilder._ALL_SUBGRAPH_GENERATORS,
    )


def test_labels(  # noqa: D103
    request: FixtureRequest,
    mf_test_configuration: MetricFlowTestConfiguration,
    sg_02_single_join_lookup: ManifestObjectLookup,
) -> None:
    path_finder_cache = PathFinderCache[SemanticGraphNode, SemanticGraphEdge, AttributeComputationPath]()
    path_finder = MetricflowGraphPathFinder[SemanticGraphNode, SemanticGraphEdge, AttributeComputationPath](
        path_finder_cache
    )
    builder = SemanticGraphBuilder(
        manifest_object_lookup=sg_02_single_join_lookup,
        path_finder=path_finder,
    )
    graph = builder.build()
    labels = (DsiEntityLabel(), MeasureAttributeLabel(measure_name=None), GroupByAttributeLabel())
    assert_object_snapshot_equal(
        request=request,
        snapshot_configuration=mf_test_configuration,
        obj={label: sorted(graph.nodes_with_label(label)) for label in labels},
    )


def test_descendants(  # noqa: D103
    request: FixtureRequest,
    mf_test_configuration: MetricFlowTestConfiguration,
    sg_02_single_join_lookup: ManifestObjectLookup,
) -> None:
    path_finder_cache = PathFinderCache[SemanticGraphNode, SemanticGraphEdge, AttributeComputationPath]()
    path_finder = MetricflowGraphPathFinder[SemanticGraphNode, SemanticGraphEdge, AttributeComputationPath](
        path_finder_cache
    )
    builder = SemanticGraphBuilder(
        manifest_object_lookup=sg_02_single_join_lookup,
        path_finder=path_finder,
    )
    graph = builder.build()
    mutable_path = AttributeComputationPath.create()
    source_node = MeasureNode.get_instance(measure_name="sm_0_measure_0", model_id=SemanticModelId(model_name="sm_0"))
    candidate_target_nodes = graph.nodes_with_label(GroupByAttributeLabel())

    logger.info("Start path finding")

    descendants = path_finder.find_reachable_targets(
        graph=graph,
        mutable_path=mutable_path,
        source_node=source_node,
        candidate_target_nodes=candidate_target_nodes,
        max_path_weight=1,
        weight_function=EdgeCountWeightFunction(),
    )

    assert_str_snapshot_equal(
        request=request,
        snapshot_configuration=mf_test_configuration,
        snapshot_str=LazyFormat(
            "Computed descendants",
            descendants=sorted(descendants.descendant_nodes),
        ).evaluated_value,
    )


def test_group_by_attribute_subgraph(  # noqa: D103
    request: FixtureRequest,
    mf_test_configuration: MetricFlowTestConfiguration,
    sg_02_single_join_lookup: ManifestObjectLookup,
) -> None:
    path_finder_cache = PathFinderCache[SemanticGraphNode, SemanticGraphEdge, AttributeComputationPath]()

    path_finder = MetricflowGraphPathFinder[SemanticGraphNode, SemanticGraphEdge, AttributeComputationPath](
        path_finder_cache
    )
    builder = SemanticGraphBuilder(
        manifest_object_lookup=sg_02_single_join_lookup,
        path_finder=path_finder,
    )
    graph = builder.build()
    metric_attribute_node = MetricAttributeNode(attribute_name="sm_0_measure_0_metric")

    subgraph_generator = GroupByAttributeSubgraphGenerator(
        semantic_graph=graph,
        path_finder=MetricflowGraphPathFinder(path_finder_cache=path_finder_cache),
    )

    subgraph = subgraph_generator.generate_subgraph_for_one_metric(metric_attribute_node)

    write_svg_snapshot_for_review(
        request=request, snapshot_configuration=mf_test_configuration, svg_file_contents=subgraph.format(SvgFormatter())
    )


def test_resolver(  # noqa: D103
    request: FixtureRequest,
    mf_test_configuration: MetricFlowTestConfiguration,
    sg_02_single_join_lookup: ManifestObjectLookup,
) -> None:
    path_finder_cache = PathFinderCache[SemanticGraphNode, SemanticGraphEdge, AttributeComputationPath]()

    path_finder = MetricflowGraphPathFinder[SemanticGraphNode, SemanticGraphEdge, AttributeComputationPath](
        path_finder_cache
    )
    builder = SemanticGraphBuilder(
        manifest_object_lookup=sg_02_single_join_lookup,
        path_finder=path_finder,
    )
    semantic_graph = builder.build()
    spec_resolver = AttributeResolver(manifest_object_lookup=sg_02_single_join_lookup, semantic_graph=semantic_graph)
    attribute_descriptors = spec_resolver.resolve_attribute_descriptors("sm_0_measure_0_metric")
    logger.debug(LazyFormat("Resolved attributes", specs=attribute_descriptors))
