"""Functions to help generate a text representation of a dataflow plan."""

import logging
import textwrap

import jinja2

from metricflow.dag.mf_dag import DagNodeVisitor, DagNode
from metricflow.object_utils import pformat_big_objects

logger = logging.getLogger(__name__)


class MetricFlowDagToText(DagNodeVisitor[str]):
    """Converts the given node and parents (recursively) to a text representation.

    The text representation should be similar to the XML format:

    <FilterElementsNode>
      ...
    </>
    """

    # Parameters for controlling the text output.
    MAX_WIDTH = 60

    def _format_to_text(self, node: DagNode, inner_contents: str) -> str:
        """Convert the given node to the text representation.

        Properties is a dict from the property name to the property value that should be printed for the node.
        """
        # Generate the descriptions for the node
        node_fields = []
        for displayed_property in node.displayed_properties:

            if len(str(displayed_property.value)) > self.MAX_WIDTH or "\n" in str(displayed_property.value):
                value_str_split = pformat_big_objects(displayed_property.value).split("\n")
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

    def visit_node(self, node: DagNode) -> str:  # noqa: D
        parent_node_descriptions = []
        for parent_node in node.parent_nodes:
            parent_node_descriptions.append(parent_node.accept_dag_node_visitor(self))

        return self._format_to_text(node=node, inner_contents="\n".join(parent_node_descriptions))

    def to_text(self, root_node: DagNode) -> str:
        """Converts the dag starting from the given root node to a text representation.

        The text representation is similar to XML.
        """
        return root_node.accept_dag_node_visitor(self)
