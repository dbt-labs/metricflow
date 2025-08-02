from __future__ import annotations

import logging

from _pytest.fixtures import FixtureRequest
from dbt_semantic_interfaces.protocols import SemanticManifest
from metricflow_semantics.experimental.semantic_graph.model_id import SemanticModelId
from metricflow_semantics.experimental.semantic_graph.nodes.entity_nodes import LocalModelNode
from metricflow_semantics.test_helpers.config_helpers import MetricFlowTestConfiguration

from tests_metricflow_semantics.experimental.semantic_graph.sg_fixtures import SemanticGraphTestFixture
from tests_metricflow_semantics.experimental.semantic_graph.test_helpers import (
    SemanticGraphTester2,
)

logger = logging.getLogger(__name__)


def test_multi_hop_path_finding(
    request: FixtureRequest,
    mf_test_configuration: MetricFlowTestConfiguration,
    sg_03_multi_hop_join_manifest: SemanticManifest,
) -> None:
    fixture = SemanticGraphTestFixture(
        request=request, snapshot_configuration=mf_test_configuration, semantic_manifest=sg_03_multi_hop_join_manifest
    )
    tester = SemanticGraphTester2(fixture)
    source_node = LocalModelNode.get_instance(SemanticModelId.get_instance("bookings_source"))
    tester.assert_found_paths(source_node)

    # pathfinder = fixture.create_path_finder()
    # semantic_graph = fixture.semantic_graph
    # source_node = LocalModelNode.get_instance(SemanticModelId.get_instance("bookings_source"))
    # path = AttributeRecipeWriterPath.create()
    # target_nodes = semantic_graph.nodes_with_label(GroupByAttributeLabel.get_instance())
    # element_filter = LinkableElementFilter()
    #
    # found_paths: list[AttributeRecipeWriterPath] = []
    # for stop_event in pathfinder.traverse_dfs(
    #     graph=semantic_graph,
    #     mutable_path=path,
    #     source_node=source_node,
    #     target_nodes=target_nodes,
    #     weight_function=DunderNameWeightFunction(element_filter),
    #     max_path_weight=2,
    #     allow_node_revisits=True,
    #     node_allow_set=None,
    #     node_deny_set=None,
    # ):
    #     path = stop_event.current_path
    #     logger.debug(LazyFormat("Found path.", path=path))
    #     found_paths.append(path.copy())
    #
    # found_paths.sort()
    # assert_str_snapshot_equal(
    #     request=request,
    #     snapshot_configuration=mf_test_configuration,
    #     snapshot_str="\n".join([found_path.arrow_format() for found_path in found_paths])
    # )


def test_time_path_finding(
    request: FixtureRequest,
    mf_test_configuration: MetricFlowTestConfiguration,
    manifest_with_200_models_100_metrics: SemanticManifest,
) -> None:
    fixture = SemanticGraphTestFixture(
        request=request,
        snapshot_configuration=mf_test_configuration,
        semantic_manifest=manifest_with_200_models_100_metrics,
    )
    tester = SemanticGraphTester2(fixture)
    source_node = LocalModelNode.get_instance(SemanticModelId.get_instance("measure_model_000"))

    # tester.assert_found_paths(source_node)
    tester.time_path_finding(source_node)


def test_profile_path_finding(
    request: FixtureRequest,
    mf_test_configuration: MetricFlowTestConfiguration,
    manifest_with_200_models_100_metrics: SemanticManifest,
) -> None:
    fixture = SemanticGraphTestFixture(
        request=request,
        snapshot_configuration=mf_test_configuration,
        semantic_manifest=manifest_with_200_models_100_metrics,
    )
    tester = SemanticGraphTester2(fixture)
    source_node = LocalModelNode.get_instance(SemanticModelId.get_instance("measure_model_000"))

    tester.profile_path_finding(source_node)
