from __future__ import annotations

import logging
from typing import Optional

from _pytest.fixtures import FixtureRequest
from metricflow_semantics.test_helpers.config_helpers import MetricFlowTestConfiguration
from metricflow_semantics.test_helpers.svg_snapshot import write_svg_snapshot_for_review
from typing_extensions import override

from tests_metricflow_semantics.experimental.mf_graph.formatting.svg_formatter import SvgFormatter
from tests_metricflow_semantics.experimental.mf_graph.presentation_graph import (
    PresentationEdge,
    PresentationGraph,
    PresentationNode,
)

logger = logging.getLogger(__name__)


class PresentationNodeFactory:
    @override
    def __init__(self) -> None:
        self._counter = 0

    def get_next_index(self) -> int:
        return_value = self._counter
        self._counter += 1
        return return_value

    def get_node(self, node_label: Optional[str] = None, invisible: bool = False) -> PresentationNode:
        return PresentationNode.get_instance(
            node_name=f"n_{self.get_next_index()}",
            node_label=node_label or "?",
            invisible=invisible,
        )


def test_presentation(  # noqa: D101
    request: FixtureRequest, mf_test_configuration: MetricFlowTestConfiguration
) -> None:
    node_factory = PresentationNodeFactory()

    top_row = [PresentationNode.get_instance(node_name=f"tr_{i}", node_label="?") for i in range(3)]

    matrix_row_0 = [PresentationNode.get_instance(node_name=f"mr_0_{i}", node_label="?") for i in range(3)]
    matrix_row_1 = [PresentationNode.get_instance(node_name=f"mr_1_{i}", node_label="?") for i in range(3)]

    top_row[0] = node_factory.get_node(invisible=True)
    top_row[1] = node_factory.get_node(invisible=True)
    # matrix_row_0[0] = node_factory.get_node(invisible=True)
    # matrix_row_1[2] = node_factory.get_node(invisible=True)
    matrix_row_1[0] = node_factory.get_node(invisible=True)
    # matrix_row_1[2] = node_factory.get_node(invisible=True, node_label="hour")

    graph = PresentationGraph.create()
    for tail_node, head_node in [
        (top_row[0], matrix_row_0[0]),
        (top_row[0], matrix_row_0[1]),
        (top_row[1], matrix_row_0[1]),
        # (top_row[1], matrix_row_0[2]),
        (top_row[2], matrix_row_0[1]),
        (top_row[2], matrix_row_0[2]),
        (matrix_row_0[0], matrix_row_1[0]),
        (matrix_row_0[0], matrix_row_1[1]),
        (matrix_row_0[1], matrix_row_1[0]),
        (matrix_row_0[1], matrix_row_1[1]),
        (matrix_row_0[2], matrix_row_1[2]),
        (matrix_row_0[2], matrix_row_1[1]),
    ]:
        graph.add_edge(
            PresentationEdge.create(
                tail_node=tail_node,
                head_node=head_node,
                invisible=tail_node.invisible or head_node.invisible,
            )
        )

    # graph.add_nodes(itertools.chain(top_row, matrix_row_0, matrix_row_1))

    write_svg_snapshot_for_review(
        request=request,
        snapshot_configuration=mf_test_configuration,
        svg_file_contents=graph.format(SvgFormatter()),
    )
