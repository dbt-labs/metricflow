from __future__ import annotations

import os

from _pytest.fixtures import FixtureRequest
from metricflow_semantics.dag.dag_visualization import DagGraphT, render_via_graphviz
from metricflow_semantics.test_helpers.config_helpers import MetricFlowTestConfiguration
from metricflow_semantics.test_helpers.snapshot_helpers import snapshot_path_prefix


def display_graph_if_requested(
    mf_test_configuration: MetricFlowTestConfiguration,
    request: FixtureRequest,
    dag_graph: DagGraphT,
) -> None:
    """Create and display the plan as an SVG, if requested to do so."""
    if not mf_test_configuration.display_graphs:
        return

    if len(request.session.items) > 1:
        raise ValueError("Displaying graphs is only supported when there's a single item in a testing session.")

    plan_svg_output_path_prefix = str(
        snapshot_path_prefix(
            request=request,
            snapshot_configuration=mf_test_configuration,
            snapshot_group=dag_graph.__class__.__name__,
            snapshot_id=str(dag_graph.dag_id),
        )
    )

    # Create parent directory since it might not exist
    os.makedirs(os.path.dirname(plan_svg_output_path_prefix), exist_ok=True)

    render_via_graphviz(dag_graph=dag_graph, file_path_without_svg_suffix=plan_svg_output_path_prefix)
