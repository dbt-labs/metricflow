from __future__ import annotations

import logging

import pytest
from _pytest.fixtures import FixtureRequest
from metricflow_semantics.test_helpers.config_helpers import MetricFlowTestConfiguration
from metricflow_semantics.test_helpers.snapshot_helpers import assert_object_snapshot_equal
from metricflow_semantics.toolkit.collections.ordered_set import FrozenOrderedSet
from metricflow_semantics.toolkit.dataclass_helpers import fast_frozen_dataclass
from metricflow_semantics.toolkit.mf_graph.path_finding.graph_path import MutableGraphPath
from metricflow_semantics.toolkit.mf_graph.path_finding.pathfinder import MetricFlowPathfinder
from metricflow_semantics.toolkit.mf_graph.path_finding.weight_function import EdgeCountWeightFunction

from tests_metricflow_semantics.toolkit.mf_graph.flow_graph import (
    FlowEdge,
    FlowGraph,
    FlowGraphPath,
    FlowGraphPathFinder,
    FlowNode,
    IntermediateNode,
    SinkNode,
    SourceNode,
)

logger = logging.getLogger(__name__)


@fast_frozen_dataclass()
class _PathFinderTestFixture:
    request: FixtureRequest
    snapshot_configuration: MetricFlowTestConfiguration
    graph: FlowGraph
    path: MutableGraphPath[FlowNode, FlowEdge]
    pathfinder: FlowGraphPathFinder
    source_node: FlowNode = SourceNode.get_instance(node_name="source")
    sink_node: FlowNode = SinkNode.get_instance(node_name="sink")
    a_node: FlowNode = IntermediateNode.get_instance(node_name="a")
    b_node: FlowNode = IntermediateNode.get_instance(node_name="b")


@pytest.fixture
def pathfinder_fixture(  # noqa: D103
    request: FixtureRequest, mf_test_configuration: MetricFlowTestConfiguration, flow_graph: FlowGraph
) -> _PathFinderTestFixture:
    return _PathFinderTestFixture(
        request=request,
        snapshot_configuration=mf_test_configuration,
        graph=flow_graph,
        pathfinder=MetricFlowPathfinder(),
        path=MutableGraphPath.create(),
    )


def test_find_paths_dfs(pathfinder_fixture: _PathFinderTestFixture) -> None:  # noqa: D103
    max_weight_to_found_paths: dict[int, list[FlowGraphPath]] = {}

    for max_path_weight in range(0, 5):
        found_paths: list[FlowGraphPath] = []
        for found_path in pathfinder_fixture.pathfinder.find_paths_dfs(
            graph=pathfinder_fixture.graph,
            initial_path=MutableGraphPath.create(pathfinder_fixture.source_node),
            target_nodes={pathfinder_fixture.sink_node},
            weight_function=EdgeCountWeightFunction(),
            max_path_weight=max_path_weight,
        ):
            found_paths.append(found_path.copy())

        max_weight_to_found_paths[max_path_weight] = found_paths.copy()

    assert_object_snapshot_equal(
        request=pathfinder_fixture.request,
        snapshot_configuration=pathfinder_fixture.snapshot_configuration,
        obj={
            max_path_weight: sorted(found_paths) for max_path_weight, found_paths in max_weight_to_found_paths.items()
        },
        expectation_description="The dictionary shows the max. allowed path weight to the paths found.",
    )


def test_find_ancestors(pathfinder_fixture: _PathFinderTestFixture) -> None:  # noqa: D103
    find_ancestors_result = pathfinder_fixture.pathfinder.find_ancestors(
        graph=pathfinder_fixture.graph,
        source_nodes=FrozenOrderedSet((pathfinder_fixture.source_node,)),
        target_nodes=FrozenOrderedSet((pathfinder_fixture.sink_node,)),
    )
    assert_object_snapshot_equal(
        request=pathfinder_fixture.request,
        snapshot_configuration=pathfinder_fixture.snapshot_configuration,
        obj=find_ancestors_result,
    )


def test_find_descendants(pathfinder_fixture: _PathFinderTestFixture) -> None:  # noqa: D103
    find_descendants_result = pathfinder_fixture.pathfinder.find_descendants(
        graph=pathfinder_fixture.graph,
        source_nodes=FrozenOrderedSet((pathfinder_fixture.a_node, pathfinder_fixture.b_node)),
        target_nodes=FrozenOrderedSet((pathfinder_fixture.sink_node,)),
    )
    assert_object_snapshot_equal(
        request=pathfinder_fixture.request,
        snapshot_configuration=pathfinder_fixture.snapshot_configuration,
        obj=find_descendants_result,
    )
