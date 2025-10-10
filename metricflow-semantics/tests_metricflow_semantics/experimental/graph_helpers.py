from __future__ import annotations

import logging
from typing import Callable, Optional

from _pytest.fixtures import FixtureRequest
from metricflow_semantics.test_helpers.snapshot_helpers import (
    SnapshotConfiguration,
    assert_str_snapshot_equal,
)
from metricflow_semantics.toolkit.mf_graph.formatting.dot_formatter import DotNotationFormatter
from metricflow_semantics.toolkit.mf_graph.mf_graph import MetricFlowGraph
from metricflow_semantics.toolkit.mf_logging.pretty_print import mf_pformat_dict

logger = logging.getLogger(__name__)


def assert_graph_snapshot_equal(
    request: FixtureRequest,
    snapshot_configuration: SnapshotConfiguration,
    graph: MetricFlowGraph,
    snapshot_id: str = "result",
    expectation_description: Optional[str] = None,
    incomparable_strings_replacement_function: Optional[Callable[[str], str]] = None,
) -> None:
    """Generate / compare a snapshot of the graph in different formats."""
    sorted_graph = graph.as_sorted()
    assert_str_snapshot_equal(
        request=request,
        snapshot_configuration=snapshot_configuration,
        snapshot_str=mf_pformat_dict(
            description=None,
            obj_dict={
                "dot_notation": sorted_graph.format(DotNotationFormatter()),
                "pretty_format": sorted_graph.format(),
            },
        ),
        snapshot_id=snapshot_id,
        expectation_description=expectation_description,
        incomparable_strings_replacement_function=incomparable_strings_replacement_function,
    )
