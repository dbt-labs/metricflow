import textwrap

import jinja2

from metricflow.dag.dag_to_text import MetricFlowDagToText
from metricflow.execution.execution_plan import ExecutionPlan


def execution_plan_to_text(execution_plan: ExecutionPlan) -> str:
    """Convert the execution plan to a text form that's like XML"""
    if len(execution_plan.sink_nodes) != 1:
        raise RuntimeError("Currently, only 1 sink node is supported in the execution plan.")

    node = execution_plan.sink_nodes[0]

    return jinja2.Template(
        textwrap.dedent(
            """\
            <{{ node_class }}{%- if not inner_contents and not node_fields %}/>{%- else %}>
                {%- if inner_contents %}
                {{ inner_contents | indent(4) }}
                {%- endif %}
            </{{ node_class }}>
            {%- endif %}
            """
        ),
        undefined=jinja2.StrictUndefined,
    ).render(
        node_class=execution_plan.__class__.__name__,
        inner_contents=MetricFlowDagToText().to_text(node),
    )
