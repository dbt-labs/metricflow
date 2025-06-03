from __future__ import annotations

import logging

from _pytest.fixtures import FixtureRequest
from metricflow_semantics.test_helpers.config_helpers import MetricFlowTestConfiguration
from metricflow_semantics.test_helpers.svg_snapshot import write_svg_snapshot_for_review
from tests_metricflow_semantics.experimental.mf_graph.flow_graph import FlowGraph
from tests_metricflow_semantics.experimental.mf_graph.formatting.svg_formatter import SvgFormatter

logger = logging.getLogger(__name__)


def test_format_to_svg(
    request: FixtureRequest, mf_test_configuration: MetricFlowTestConfiguration, flow_graph: FlowGraph
) -> None:
    """Check formatting of the graph to an SVG representation."""
    write_svg_snapshot_for_review(
        request=request,
        snapshot_configuration=mf_test_configuration,
        svg_file_contents=flow_graph.format(SvgFormatter()),
    )
