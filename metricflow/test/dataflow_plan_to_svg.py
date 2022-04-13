import logging
import os
import tempfile
from typing import TypeVar

import graphviz
from _pytest.fixtures import FixtureRequest

from metricflow.dag.mf_dag import DagNode, MetricFlowDag
from metricflow.object_utils import random_id
from metricflow.test.fixtures.setup_fixtures import MetricFlowTestSessionState
from metricflow.test.plan_utils import snapshot_path_prefix

logger = logging.getLogger(__name__)


DagNodeT = TypeVar("DagNodeT", bound=DagNode)


def add_nodes_to_digraph(node: DagNodeT, dot: graphviz.Digraph) -> None:
    """Adds the node (and parent nodes) to the dot for visualization."""
    for parent_node in node.parent_nodes:
        add_nodes_to_digraph(parent_node, dot)

    dot.node(name=node.node_id.id_str, label=node.graphviz_label)
    for parent_node in node.parent_nodes:
        dot.edge(tail_name=parent_node.node_id.id_str, head_name=node.node_id.id_str)


DagGraphT = TypeVar("DagGraphT", bound=MetricFlowDag)


def display_graph_if_requested(
    mf_test_session_state: MetricFlowTestSessionState,
    request: FixtureRequest,
    dag_graph: DagGraphT,
) -> None:
    """Create and display the plan as an SVG, if configured to do so."""

    if not mf_test_session_state.display_plans:
        return

    plan_svg_output_path_prefix = snapshot_path_prefix(
        request=request, snapshot_group=dag_graph.__class__.__name__, snapshot_id=dag_graph.dag_id
    )

    # Create parent directory since it might not exist
    os.makedirs(os.path.dirname(plan_svg_output_path_prefix), exist_ok=True)

    if mf_test_session_state.plans_displayed >= mf_test_session_state.max_plans_displayed:
        raise RuntimeError(
            f"Can't display plan - hit limit of {mf_test_session_state.max_plans_displayed} plans displayed."
        )
    _render_via_graphviz(dag_graph=dag_graph, file_path_without_svg_suffix=plan_svg_output_path_prefix)
    mf_test_session_state.plans_displayed += 1


def display_dag_as_svg(dag_graph: DagGraphT) -> None:
    """Create and display the plan as an SVG in the browser."""
    with tempfile.TemporaryDirectory() as temp_dir:
        random_file_path = os.path.join(temp_dir, f"dag_{random_id()}")
        _render_via_graphviz(dag_graph=dag_graph, file_path_without_svg_suffix=random_file_path)


def _render_via_graphviz(dag_graph: DagGraphT, file_path_without_svg_suffix: str) -> None:
    dot = graphviz.Digraph(comment=dag_graph.dag_id, node_attr={"shape": "box", "fontname": "Courier"})
    # Not quite correct if there are shared nodes.
    for sink_node in dag_graph.sink_nodes:
        add_nodes_to_digraph(sink_node, dot)
    dot.format = "svg"
    dot.render(file_path_without_svg_suffix, view=True, format="svg", cleanup=True)
