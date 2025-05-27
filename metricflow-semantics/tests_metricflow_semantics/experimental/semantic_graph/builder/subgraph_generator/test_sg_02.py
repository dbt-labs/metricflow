from __future__ import annotations

import logging

from _pytest.fixtures import FixtureRequest
from metricflow_semantics.experimental.semantic_graph.builder.graph_builder import SemanticGraphBuilder
from metricflow_semantics.experimental.semantic_graph.builder.group_by_attribute_subgraph import (
    GroupByAttributeSubgraphGenerator,
)
from metricflow_semantics.experimental.semantic_graph.manifest_object_lookup import ManifestObjectLookup
from metricflow_semantics.experimental.semantic_graph.model_id import SemanticModelId
from metricflow_semantics.experimental.semantic_graph.nodes.attribute_node import MeasureNode
from metricflow_semantics.experimental.semantic_graph.nodes.node_label import (
    DsiEntityLabel,
    GroupByAttributeLabel,
    MeasureAttributeLabel,
)
from metricflow_semantics.experimental.semantic_graph.path_finding.graph_path import MutableMetricflowGraphPath
from metricflow_semantics.experimental.semantic_graph.path_finding.path_finder import (
    DefaultWeightFunction,
    MetricflowGraphPathFinder,
)
from metricflow_semantics.experimental.semantic_graph.spec_resolution.spec_resolver import (
    SemanticGraphGroupByItemSpecResolver,
)
from metricflow_semantics.mf_logging.lazy_formattable import LazyFormat
from metricflow_semantics.test_helpers.config_helpers import MetricFlowTestConfiguration
from metricflow_semantics.test_helpers.snapshot_helpers import assert_object_snapshot_equal, assert_str_snapshot_equal

from tests_metricflow_semantics.experimental.graph_helpers import assert_graph_snapshot_equal

logger = logging.getLogger(__name__)


def test_all(  # noqa: D103
    request: FixtureRequest,
    mf_test_configuration: MetricFlowTestConfiguration,
    sg_02_single_join_lookup: ManifestObjectLookup,
) -> None:
    builder = SemanticGraphBuilder()
    graph = builder.build(
        manifest_object_lookup=sg_02_single_join_lookup,
        subgraph_generators=SemanticGraphBuilder._ALL_SUBGRAPH_GENERATORS,
    )
    assert_graph_snapshot_equal(request=request, snapshot_configuration=mf_test_configuration, graph=graph)


def test_labels(  # noqa: D103
    request: FixtureRequest,
    mf_test_configuration: MetricFlowTestConfiguration,
    sg_02_single_join_lookup: ManifestObjectLookup,
) -> None:
    builder = SemanticGraphBuilder()
    graph = builder.build(
        manifest_object_lookup=sg_02_single_join_lookup,
        subgraph_generators=SemanticGraphBuilder._ALL_SUBGRAPH_GENERATORS,
    )
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
    builder = SemanticGraphBuilder()
    graph = builder.build(
        manifest_object_lookup=sg_02_single_join_lookup,
        subgraph_generators=SemanticGraphBuilder._ALL_SUBGRAPH_GENERATORS,
    )

    source_node = MeasureNode.get_instance(measure_name="sm_0_measure_0", model_id=SemanticModelId(model_name="sm_0"))
    candidate_target_nodes = graph.nodes_with_label(GroupByAttributeLabel())

    logger.info("Start path finding")

    path_finder = MetricflowGraphPathFinder(graph)
    descendants = path_finder.find_descendant_nodes(
        source_node=source_node,
        candidate_target_nodes=candidate_target_nodes,
        max_path_weight=1,
        weight_function=DefaultWeightFunction(),
        mutable_path=MutableMetricflowGraphPath.create(),
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
    builder = SemanticGraphBuilder()
    graph = builder.build(
        manifest_object_lookup=sg_02_single_join_lookup,
        subgraph_generators=SemanticGraphBuilder._ALL_SUBGRAPH_GENERATORS,
    )

    measure_attribute_node = MeasureNode.get_instance(
        measure_name="sm_0_measure_0", model_id=SemanticModelId(model_name="sm_0")
    )
    subgraph_generator = GroupByAttributeSubgraphGenerator(
        semantic_graph=graph,
        path_finder=MetricflowGraphPathFinder(graph),
    )

    subgraph = subgraph_generator.generate_subgraph_for_one_measure(
        measure_attribute_node,
    )

    assert_graph_snapshot_equal(request=request, snapshot_configuration=mf_test_configuration, graph=subgraph)


def test_spec_resolver(  # noqa: D103
    request: FixtureRequest,
    mf_test_configuration: MetricFlowTestConfiguration,
    sg_02_single_join_lookup: ManifestObjectLookup,
) -> None:
    builder = SemanticGraphBuilder()
    semantic_graph = builder.build(
        manifest_object_lookup=sg_02_single_join_lookup,
    )
    spec_resolver = SemanticGraphGroupByItemSpecResolver(
        manifest_object_lookup=sg_02_single_join_lookup, semantic_graph=semantic_graph
    )
    specs = spec_resolver.get_specs_for_measure("sm_0_measure_0")
    logger.debug(LazyFormat("Got specs", specs=specs))
