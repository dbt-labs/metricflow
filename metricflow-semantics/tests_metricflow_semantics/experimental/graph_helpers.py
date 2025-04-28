from __future__ import annotations

import logging
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import Callable, Optional

from _pytest.fixtures import FixtureRequest
from metricflow_semantics.experimental.mf_graph.formatting.dot_formatter import DotNotationFormatter
from metricflow_semantics.experimental.mf_graph.mf_graph import MetricflowGraph
from metricflow_semantics.mf_logging.pretty_print import mf_pformat, mf_pformat_dict
from metricflow_semantics.test_helpers.snapshot_helpers import (
    SnapshotConfiguration,
    assert_snapshot_text_equal,
    assert_str_snapshot_equal,
)

from tests_metricflow_semantics.experimental.mf_graph.formatting.svg_formatter import SvgFileFormatter

logger = logging.getLogger(__name__)


def assert_graph_snapshot_equal(
    request: FixtureRequest,
    snapshot_configuration: SnapshotConfiguration,
    graph: MetricflowGraph,
    snapshot_id: str = "result",
    expectation_description: Optional[str] = None,
    incomparable_strings_replacement_function: Optional[Callable[[str], str]] = None,
) -> None:
    assert_str_snapshot_equal(
        request=request,
        snapshot_configuration=snapshot_configuration,
        snapshot_str=mf_pformat_dict(
            obj_dict={"mf_pformat": mf_pformat(graph), "dot_notation": graph.format_graph(DotNotationFormatter())}
        ),
        snapshot_id=snapshot_id,
        expectation_description=expectation_description,
        incomparable_strings_replacement_function=incomparable_strings_replacement_function,
    )

    with TemporaryDirectory() as temp_directory:
        output_svg_file_path = Path(temp_directory) / "graph.svg"
        graph.format_graph(SvgFileFormatter(output_svg_file_path))
        with open(output_svg_file_path) as output_svg_file:
            svg_text = output_svg_file.read()

    assert_snapshot_text_equal(
        request=request,
        snapshot_configuration=snapshot_configuration,
        group_id="svg",
        snapshot_id=snapshot_id,
        snapshot_text=svg_text,
        snapshot_file_extension=".svg",
        expectation_description=expectation_description,
        incomparable_strings_replacement_function=incomparable_strings_replacement_function,
        include_headers=False,
    )
