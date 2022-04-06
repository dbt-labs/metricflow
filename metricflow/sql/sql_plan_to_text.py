"""Functions to convert a SQL query plan into a text representation."""

import textwrap

import jinja2
from jinja2 import StrictUndefined

from metricflow.dag.dag_to_text import MetricFlowDagToText
from metricflow.sql.sql_plan import (
    SqlQueryPlanNode,
    SqlQueryPlan,
)


def sql_query_plan_node_as_text(root_node: SqlQueryPlanNode) -> str:
    """Recursively convert the tree represented by the root node into a string."""

    return MetricFlowDagToText().to_text(root_node)


def sql_query_plan_as_text(sql_query_plan: SqlQueryPlan) -> str:
    """Converts the SQL query plan to a text representation that can be used for tests / debugging.

    The text representation is similar to XML.
    """
    component_from_render_node_as_text = sql_query_plan_node_as_text(sql_query_plan.render_node)

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
        undefined=StrictUndefined,
    ).render(
        node_class=sql_query_plan.__class__.__name__,
        inner_contents=component_from_render_node_as_text,
    )
