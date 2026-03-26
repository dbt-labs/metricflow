from __future__ import annotations

import logging

from _pytest.fixtures import FixtureRequest
from metricflow_semantics.test_helpers.config_helpers import MetricFlowTestConfiguration
from metricflow_semantics.test_helpers.snapshot_helpers import assert_str_snapshot_equal
from metricflow_semantics.toolkit.mf_graph.formatting.pretty_graph_formatter import PrettyFormatGraphFormatter

from tests_metricflow_semantics.toolkit.mf_graph.flow_graph import FlowGraph

logger = logging.getLogger(__name__)


def test_pretty_format(
    request: FixtureRequest, mf_test_configuration: MetricFlowTestConfiguration, flow_graph: FlowGraph
) -> None:
    """Check formatting of the graph using `PrettyFormatGraphFormatter`."""
    assert_str_snapshot_equal(
        request=request,
        snapshot_configuration=mf_test_configuration,
        snapshot_str=flow_graph.format(PrettyFormatGraphFormatter()),
    )
