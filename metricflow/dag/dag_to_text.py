"""Functions to help generate a text representation of a DAG."""
from __future__ import annotations

import logging
import textwrap
import typing
from typing import Optional

import jinja2

if typing.TYPE_CHECKING:
    from metricflow.dag.mf_dag import DagNode, DagNodeT, MetricFlowDag

from metricflow.mf_logging.pretty_print import mf_pformat

logger = logging.getLogger(__name__)


class MetricFlowDagTextFormatter:
    """Converts the given node and parents (recursively) to a text representation.

    The text representation should be similar to the XML format:

    <FilterElementsNode>
      ...
    </>
    """

    # Parameters for controlling the text output.
    MAX_WIDTH = 60

    def _format_to_text(self, node: DagNode, inner_contents: Optional[str]) -> str:
        """Convert the given node to the text representation.

        Properties is a dict from the property name to the property value that should be printed for the node.
        """
        # Generate the descriptions for the node
        node_fields = []
        for displayed_property in node.displayed_properties:
            if len(str(displayed_property.value)) > self.MAX_WIDTH or "\n" in str(displayed_property.value):
                value_str_split = mf_pformat(displayed_property.value).split("\n")
                max_value_str_length = max([len(x) for x in value_str_split])
                node_fields.append(
                    jinja2.Template(
                        textwrap.dedent(
                            """\
                            <!-- {{ key }} = {{ padding }} -->
                            """
                        ),
                        undefined=jinja2.StrictUndefined,
                    ).render(
                        key=displayed_property.key,
                        padding=" "
                        * (
                            (len("<!--   ") + max_value_str_length + len(" -->"))
                            - len("<!-- ")
                            - len(displayed_property.key)
                            - len(" = ")
                            - len("-->")
                        ),
                    )
                )
                for value_str in value_str_split:
                    node_fields.append(
                        jinja2.Template(
                            textwrap.dedent(
                                """\
                                <!--   {{ value }} {{ padding }} -->
                                """
                            ),
                            undefined=jinja2.StrictUndefined,
                        ).render(
                            value=value_str,
                            padding=" "
                            * (
                                (len("<!-- ") + max_value_str_length + len(" -->"))
                                - len("<!-- ")
                                - len(value_str)
                                - len(" -->")
                            ),
                        )
                    )
            else:
                node_fields.append(
                    jinja2.Template(
                        textwrap.dedent(
                            """\
                            <!-- {{ key }} = {{ value }} -->
                            """
                        ),
                        undefined=jinja2.StrictUndefined,
                    ).render(
                        key=displayed_property.key,
                        value=displayed_property.value,
                    )
                )

        return jinja2.Template(
            textwrap.dedent(
                """\
                <{{ node_class }}{%- if not inner_contents and not node_fields %}/>{%- else %}>
                    {%- if node_fields %}
                    {{ node_fields | indent(4) }}
                    {%- endif %}
                    {%- if inner_contents %}
                    {{ inner_contents | indent(4) }}
                    {%- endif %}
                </{{ node_class }}>
                {%- endif %}
                """
            ),
            undefined=jinja2.StrictUndefined,
        ).render(
            node_class=node.__class__.__name__,
            node_fields="\n".join(node_fields),
            inner_contents=inner_contents,
        )

    def _recursively_format_to_text(self, node: DagNode) -> str:
        """Converts the node and its parents to a text representation.

        The text representation is similar to XML.
        """
        parent_node_descriptions = []
        for parent_node in node.parent_nodes:
            parent_node_descriptions.append(self._recursively_format_to_text(parent_node))

        return self._format_to_text(node=node, inner_contents="\n".join(parent_node_descriptions))

    def dag_to_text(self, dag: MetricFlowDag[DagNodeT]) -> str:
        """Converts the DAG to a text representation that can be used for logging / tests.

        The text representation is similar to XML.
        """
        try:
            # Convert each of the components that are associated with the sink nodes to a text representation.
            component_from_sink_nodes_as_text = []
            for sink_node in dag.sink_nodes:
                component_from_sink_nodes_as_text.append(self.dag_component_to_text(sink_node))

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
                node_class=dag.__class__.__name__,
                inner_contents="\n".join(component_from_sink_nodes_as_text),
            )
        except Exception:
            logger.exception(f"Got an exception while converting {dag} to text")
            return str(dag)

    def dag_component_to_text(self, dag_component_leaf_node: DagNode) -> str:
        """Convert the DAG component starting from the given leaf node to a text representation."""
        try:
            return self._recursively_format_to_text(dag_component_leaf_node)
        except Exception:
            logger.exception(f"Got an exception while converting {dag_component_leaf_node} to text")
            return str(dag_component_leaf_node)
