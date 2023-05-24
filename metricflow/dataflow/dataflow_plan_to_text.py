"""Functions to help generate a text representation of a dataflow plan."""
from __future__ import annotations

import logging
import textwrap

import jinja2

from metricflow.dag.dag_to_text import MetricFlowDagToText
from metricflow.dataflow.dataflow_plan import (
    DataflowPlan,
    DataflowPlanNode,
)

logger = logging.getLogger(__name__)


def dataflow_dag_as_text(root_node: DataflowPlanNode) -> str:
    """Converts the dataflow dag starting from the given root node to a text representation.

    The text representation is similar to XML.
    """
    return MetricFlowDagToText().to_text(root_node)


def dataflow_plan_as_text(dataflow_plan: DataflowPlan) -> str:
    """Converts the dataflow plan to a text representation that can be used for tests / debugging.

    The text representation is similar to XML.
    """
    # Convert each of the components that are associated with the sink nodes to a text representation.
    component_from_sink_nodes_as_text = []
    for sink_node in dataflow_plan.sink_nodes:
        component_from_sink_nodes_as_text.append(dataflow_dag_as_text(sink_node))

    # Under <DataflowPlan>, render all components.
    return jinja2.Template(
        textwrap.dedent(
            """\
            <{{ node_class }}{%- if not inner_contents %}/>{%- else %}>
                {%- if inner_contents %}
                {{ inner_contents | indent(4) }}
                {%- endif %}
            </{{ node_class }}>
            {%- endif %}
            """
        ),
        undefined=jinja2.StrictUndefined,
    ).render(
        node_class=dataflow_plan.__class__.__name__,
        inner_contents="\n".join(component_from_sink_nodes_as_text),
    )
