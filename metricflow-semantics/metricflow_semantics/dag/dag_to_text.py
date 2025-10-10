"""Functions to help generate a text representation of a DAG."""

from __future__ import annotations

import logging
import threading
import typing
from contextlib import contextmanager
from typing import Iterator, Optional

from metricflow_semantics.toolkit.string_helpers import mf_indent

if typing.TYPE_CHECKING:
    from metricflow_semantics.dag.mf_dag import DagNode, DagNodeT, DisplayedProperty, MetricFlowDag

from metricflow_semantics.toolkit.mf_logging.pretty_print import PrettyFormatDictOption, mf_pformat

logger = logging.getLogger(__name__)


class MaxWidthTracker:
    """Helps to number columns remaining as the DAG is formatted to text recursively.

    This is needed as the parents of a given node are formatted with an indent. For example, if we want to print a DAG
    with a column width of 80 and the indent is 4 spaces, then we would want to print the parents with a column width of
    76.
    """

    def __init__(self, max_width: int) -> None:  # noqa: D107
        self._current_max_width = max_width

    @contextmanager
    def update_max_width_for_indented_section(self, indent_prefix: str) -> Iterator[None]:
        """Context manager used to wrap the code that prints an indented section."""
        previous_max_width = self._current_max_width
        self._current_max_width = max(1, self._current_max_width - len(indent_prefix))
        yield None
        self._current_max_width = previous_max_width

    @property
    def current_max_width(self) -> int:  # noqa: D102
        return self._current_max_width


class MetricFlowDagTextFormatter:
    """Converts the given node and parents (recursively) to a text representation.

    The text representation should be similar to the XML format where the parents are printed in an indented section.:

    <FilterElementsNode>
      ...
    </>
    """

    def __init__(
        self, max_width: int = 120, node_parent_indent_prefix: str = "    ", value_indent_prefix: str = "  "
    ) -> None:
        """Initializer.

        Args:
            max_width: Try to keep the generated text to this many columns wide.
            node_parent_indent_prefix: When printing a parent node, use this as a prefix for the indent.
            value_indent_prefix: When printing the values for a DisplayedProperty, use this indent.
        """
        self._max_width = max_width
        self._node_parent_indent_prefix = node_parent_indent_prefix
        self._value_indent_prefix = value_indent_prefix

        # In case this gets used in a multi-threaded context, use a thread-local variable since it has mutable state.
        self._thread_local_data = threading.local()

    @property
    def _max_width_tracker(self) -> MaxWidthTracker:
        if not hasattr(self._thread_local_data, "max_width_tracker"):
            self._thread_local_data.max_width_tracker = MaxWidthTracker(self._max_width)
        return self._thread_local_data.max_width_tracker

    def _displayed_property_on_one_line(self, displayed_property: DisplayedProperty) -> str:
        key = displayed_property.key
        value = mf_pformat(
            displayed_property.value,
            format_option=PrettyFormatDictOption(max_line_length=self._max_width_tracker.current_max_width),
        )
        return f"<!-- {key} = {value} -->"

    def _format_to_text(self, node: DagNode, inner_contents: Optional[str]) -> str:
        """Convert the given node to the text representation.

        Properties is a dict from the property name to the property value that should be printed for the node.
        """
        # Generate the descriptions for the node
        node_fields = []
        max_width = self._max_width_tracker.current_max_width
        for displayed_property in node.displayed_properties:
            # See if the displayed property can be printed on one line.
            displayed_property_on_one_line = self._displayed_property_on_one_line(displayed_property)
            if len(displayed_property_on_one_line) <= max_width:
                node_fields.append(displayed_property_on_one_line)
                continue

            # If not, split them into multiple lines.
            value_str = mf_pformat(
                displayed_property.value,
                # The string representation of displayed_property.value will be wrapped with "<!-- ", " -->" so subtract
                # the width of those.
                format_option=PrettyFormatDictOption(
                    max_line_length=max(1, max_width - len("<!-- ") - len(" -->")),
                    indent_prefix=self._value_indent_prefix,
                ),
            )

            # Figure out the max width of the value so that we can add appropriate spacing so that the "<!--" / "-->"
            # line up.
            value_str_split = value_str.split("\n")
            max_value_str_length = max([len(x) for x in value_str_split])

            # Print the key on multiple lines.
            key = displayed_property.key
            # Add padding so that all fields of this object have <!-- and --> that align.
            key_padding = " " * (
                (len("<!-- ") + len(self._value_indent_prefix) + max_value_str_length + len(" -->"))
                - len("<!-- ")
                - len(key)
                - len(" = ")
                - len(" -->")
            )

            node_fields.append(f"<!-- {key} = {key_padding} -->")

            # Print the lines for the value in an indented section.
            for value_str in value_str_split:
                value_padding = " " * (
                    (len("<!-- ") + len(self._value_indent_prefix) + max_value_str_length + len(" -->"))
                    - len("<!-- ")
                    - len(self._value_indent_prefix)
                    - len(value_str)
                    - len(" -->")
                )
                node_fields.append(f"<!-- {self._value_indent_prefix}{value_str}{value_padding} -->")

        node_class = node.__class__.__name__
        if not node_fields and not inner_contents:
            return f"<{node_class}/>"

        lines = [f"<{node_class}>"]
        for line in node_fields:
            lines.append(mf_indent(line, indent_prefix=self._node_parent_indent_prefix))
        if inner_contents:
            lines.append(mf_indent(inner_contents, indent_prefix=self._node_parent_indent_prefix))
        lines.append(f"</{node_class}>")
        return "\n".join(lines)

    def _recursively_format_to_text(self, node: DagNode) -> str:
        """Converts the node and its parents to a text representation.

        The text representation is similar to XML.
        """
        parent_node_descriptions = []
        with self._max_width_tracker.update_max_width_for_indented_section(
            indent_prefix=self._node_parent_indent_prefix
        ):
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

            with self._max_width_tracker.update_max_width_for_indented_section(self._node_parent_indent_prefix):
                for sink_node in dag.sink_nodes:
                    component_from_sink_nodes_as_text.append(self.dag_component_to_text(sink_node))

            # Under <DataflowPlan>, render all components.
            node_class = dag.__class__.__name__
            if len(component_from_sink_nodes_as_text) == 0:
                return f"<{node_class}/>"

            lines = [f"<{node_class}>"]
            for line in component_from_sink_nodes_as_text:
                lines.append(mf_indent(line, indent_prefix=self._node_parent_indent_prefix))
            lines.append(f"</{node_class}>")

            return "\n".join(lines)

        except Exception:
            logger.exception(
                f"Got an exception while converting {dag} to text. This exception will be swallowed, and the built-in "
                f"string representation will be returned instead."
            )
            return str(dag)

    def dag_component_to_text(self, dag_component_leaf_node: DagNode) -> str:
        """Convert the DAG component starting from the given leaf node to a text representation."""
        try:
            return self._recursively_format_to_text(dag_component_leaf_node)
        except Exception:
            logger.exception(f"Got an exception while converting {dag_component_leaf_node} to text")
            return str(dag_component_leaf_node)
