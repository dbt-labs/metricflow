from __future__ import annotations

import logging

from _pytest.fixtures import FixtureRequest
from metricflow_semantics.test_helpers.config_helpers import MetricFlowTestConfiguration

from tests_metricflow_semantics.helpers.graph_helpers import assert_graph_snapshot_equal
from tests_metricflow_semantics.toolkit.mf_graph.flow_graph import FlowGraph

logger = logging.getLogger(__name__)


def test_graph_snapshot(
    request: FixtureRequest, mf_test_configuration: MetricFlowTestConfiguration, flow_graph: FlowGraph
) -> None:
    """Check the graph snapshot."""
    assert_graph_snapshot_equal(request=request, snapshot_configuration=mf_test_configuration, graph=flow_graph)
