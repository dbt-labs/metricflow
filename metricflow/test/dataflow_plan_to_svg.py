from __future__ import annotations

import os

from _pytest.fixtures import FixtureRequest

from metricflow.dag.dag_visualization import DagGraphT, render_via_graphviz
from metricflow.test.fixtures.setup_fixtures import MetricFlowTestSessionState
from metricflow.test.snapshot_utils import snapshot_path_prefix


def display_graph_if_requested(
    mf_test_session_state: MetricFlowTestSessionState,
    request: FixtureRequest,
    dag_graph: DagGraphT,
) -> None:
    """Create and display the plan as an SVG, if requested to do so."""
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
    render_via_graphviz(dag_graph=dag_graph, file_path_without_svg_suffix=plan_svg_output_path_prefix)
    mf_test_session_state.plans_displayed += 1
