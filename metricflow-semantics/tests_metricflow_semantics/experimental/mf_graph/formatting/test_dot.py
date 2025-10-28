from __future__ import annotations

import logging

from _pytest.fixtures import FixtureRequest
from metricflow_semantics.test_helpers.config_helpers import MetricFlowTestConfiguration
from metricflow_semantics.test_helpers.snapshot_helpers import assert_str_snapshot_equal
from metricflow_semantics.toolkit.mf_graph.formatting.dot_formatter import DotNotationFormatter

from tests_metricflow_semantics.experimental.mf_graph.flow_graph import FlowGraph

logger = logging.getLogger(__name__)


def test_dot_text(
    request: FixtureRequest, mf_test_configuration: MetricFlowTestConfiguration, flow_graph: FlowGraph
) -> None:
    """Check formatting of the graph using DOT notation."""
    assert_str_snapshot_equal(
        request=request,
        snapshot_configuration=mf_test_configuration,
        snapshot_str=flow_graph.format(DotNotationFormatter()),
    )
