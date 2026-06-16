from __future__ import annotations

import logging
import os

from metricflow_semantics.toolkit.id_helpers import mf_random_id
from metricflow_semantics.toolkit.mf_graph.formatting.dag_visualization import DagGraphT, render_via_graphviz

logger = logging.getLogger(__name__)


def display_dag_as_svg(dag_graph: DagGraphT, directory_path: str) -> str:
    """Create and display the plan as an SVG in the browser.

    Returns the path where the SVG file was created within "mf_config_dir".
    """
    svg_dir = os.path.join(directory_path, "generated_svg")
    random_file_path = os.path.join(svg_dir, f"dag_{mf_random_id()}")
    render_via_graphviz(dag_graph=dag_graph, file_path_without_svg_suffix=random_file_path)
    return random_file_path + ".svg"
