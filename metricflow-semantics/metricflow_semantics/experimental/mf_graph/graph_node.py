"""Classes for modeling a DAG."""

from __future__ import annotations

import html
import logging
import textwrap
from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Generic, Sequence, TypeVar

import jinja2

from metricflow_semantics.dag.id_prefix import IdPrefix
from metricflow_semantics.dag.mf_dag import DisplayedProperty
from metricflow_semantics.dag.sequential_id import SequentialIdGenerator
from metricflow_semantics.experimental.mf_graph.displayable_graph_element import MetricflowGraphElement
from metricflow_semantics.experimental.mf_graph.graph_element_id import GraphElementId

logger = logging.getLogger(__name__)

DisplayableGraphNodeT = TypeVar("DisplayableGraphNodeT", bound="DisplayableGraphNode")


@dataclass(frozen=True)
class DisplayableGraphNode(MetricflowGraphElement, Generic[DisplayableGraphNodeT], ABC):
    """A node in a graph that can be displayed."""

    @property
    def displayed_properties(self) -> Sequence[DisplayedProperty]:
        """When the node is represented in a visualization, the properties to display with it."""
        return ()

    @property
    def graphviz_label(self) -> str:
        """When using graphviz to render this node, the label that should be used in the construction."""
        return make_graphviz_label(
            title=self.dot_label,
            properties=self.displayed_properties,
        )

    @classmethod
    @abstractmethod
    def id_prefix(cls) -> IdPrefix:
        """The prefix to use when generating IDs for nodes.

        e.g. a prefix of "my_node" will generate an ID like "my_node_0"
        """
        pass

    @classmethod
    def create_unique_id(cls) -> GraphElementId:
        """Create and return a unique identifier to use when creating nodes."""
        return GraphElementId(str_value=SequentialIdGenerator.create_next_id(cls.id_prefix()).str_value)


def make_graphviz_label(
    title: str, properties: Sequence[DisplayedProperty], title_font_size: int = 12, property_font_size: int = 6
) -> str:
    """Make a graphviz label that can be used for rendering to an image.

    The title will be in a large font, while the properties will be listed in a table in a smaller font.
    """
    # Convert all properties values into a HTML-safe string, then break the string into lines of 40 columns so that
    # the node boxes don't get so wide. Better to pretty-print the object, but unclear how to do so.
    formatted_properties = []
    for displayed_property in properties:
        lines = [html.escape(x) for x in textwrap.wrap(str(displayed_property.value), width=40)]
        formatted_properties.append(DisplayedProperty(displayed_property.key, "<BR/>".join(lines)))

    return jinja2.Template(
        # Formatting here: https://graphviz.org/doc/info/shapes.html#html
        textwrap.dedent(
            """\
            <<TABLE BORDER="0" CELLPADDING="1" CELLSPACING="0">
             <TR>
               <TD ALIGN="LEFT" BALIGN="LEFT" VALIGN="TOP" COLSPAN="2"><FONT point-size="{{ title_size }}">{{ title }}</FONT></TD>
             </TR>
             {%- for key, value in properties %}
             <TR>
               <TD ALIGN="LEFT" BALIGN="LEFT" VALIGN="TOP"><FONT point-size="{{ property_size }}">{{ key }}</FONT></TD>
               <TD ALIGN="LEFT" BALIGN="LEFT" VALIGN="TOP"><FONT point-size="{{ property_size }}">{{ value }}</FONT></TD>
             </TR>
             {%- endfor %}
            </TABLE>>
            """
        ),
        undefined=jinja2.StrictUndefined,
    ).render(
        title=title,
        title_size=title_font_size,
        property_size=property_font_size,
        properties=[(displayed_property.key, displayed_property.value) for displayed_property in formatted_properties],
    )
