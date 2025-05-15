from __future__ import annotations

import logging
from typing import Callable, Optional

from _pytest.fixtures import FixtureRequest
from metricflow_semantics.experimental.mf_graph.mf_graph import MetricflowGraph
from metricflow_semantics.mf_logging.pretty_print import PrettyFormatDictOption, mf_pformat, mf_pformat_dict
from metricflow_semantics.test_helpers.snapshot_helpers import (
    SnapshotConfiguration,
    assert_snapshot_text_equal,
    assert_str_snapshot_equal,
)

logger = logging.getLogger(__name__)


def assert_graph_snapshot_equal(
    request: FixtureRequest,
    snapshot_configuration: SnapshotConfiguration,
    graph: MetricflowGraph,
    snapshot_id: str = "result",
    expectation_description: Optional[str] = None,
    incomparable_strings_replacement_function: Optional[Callable[[str], str]] = None,
) -> None:
    """Generate / compare a snapshot of the graph in different formats."""
    assert_str_snapshot_equal(
        request=request,
        snapshot_configuration=snapshot_configuration,
        snapshot_str=mf_pformat_dict(
            obj_dict={"mf_pformat": mf_pformat(graph), "dot_notation": graph.format_dot()},
            format_option=PrettyFormatDictOption(preserve_raw_strings=True),
        ),
        snapshot_id=snapshot_id,
        expectation_description=expectation_description,
        incomparable_strings_replacement_function=incomparable_strings_replacement_function,
    )
    # Create a separate SVG snapshot file for easier viewing.
    assert_snapshot_text_equal(
        request=request,
        snapshot_configuration=snapshot_configuration,
        group_id="svg",
        snapshot_id=snapshot_id,
        snapshot_text=graph.format_svg(),
        snapshot_file_extension=".svg",
        expectation_description=expectation_description,
        incomparable_strings_replacement_function=incomparable_strings_replacement_function,
        include_headers=False,
    )
