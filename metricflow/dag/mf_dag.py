"""Classes for modeling a DAG."""

from __future__ import annotations

import html
import logging
import textwrap
from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Any, List, Sequence, TypeVar, Generic

import jinja2

from metricflow.dag.id_generation import IdGeneratorRegistry
from metricflow.visitor import VisitorOutputT

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class DisplayedProperty:  # type: ignore
    """When visualizing a node as text or a graphic, the properties to display with it.

    This should be displayed in the form "{key} = {str(value)}"
    """

    key: str
    value: Any  # type: ignore


@dataclass(frozen=True)
class NodeId:
    """Unique identifier for nodes in DAGs."""

    id_str: str

    def __repr__(self) -> str:  # noqa: D
        return self.id_str


class DagNodeVisitor(Generic[VisitorOutputT], ABC):
    """An object that can be used to visit the nodes of a DAG graph"""

    @abstractmethod
    def visit_node(self, node: DagNode) -> VisitorOutputT:  # noqa: D
        pass


class DagNode(ABC):
    """A node in a DAG. These should be immutable."""

    def __init__(self, node_id: NodeId) -> None:  # noqa: D
        self._node_id = node_id

    @property
    def node_id(self) -> NodeId:
        """ID for uniquely identifying a given node."""
        return self._node_id

    @property
    @abstractmethod
    def description(self) -> str:
        """A human-readable description for this node."""
        pass

    @property
    def displayed_properties(self) -> List[DisplayedProperty]:
        """When the node is represented in a visualization, the properties to display with it."""
        return [
            DisplayedProperty("description", self.description),
            DisplayedProperty("node_id", self.node_id),
        ]

    @property
    def graphviz_label(self) -> str:
        """When using graphviz to render this node, the label that should be used in the construction."""
        return make_graphviz_label(
            title=self.__class__.__name__,
            properties=self.displayed_properties,
        )

    @property
    @abstractmethod
    def parent_nodes(self) -> Sequence[DagNode]:  # noqa: D
        pass

    def __repr__(self) -> str:  # noqa: D
        return f"{self.__class__.__name__}(node_id={self.node_id})"

    @classmethod
    @abstractmethod
    def id_prefix(cls) -> str:
        """The prefix to use when generating IDs for nodes.

        e.g. a prefix of "my_node" will generate an ID like "my_node_0"
        """
        pass

    @classmethod
    def create_unique_id(cls) -> NodeId:
        """Create and return a unique identifier to use when creating nodes."""
        return NodeId(IdGeneratorRegistry.for_class(cls).create_id(cls.id_prefix()))

    def accept_dag_node_visitor(self, visitor: DagNodeVisitor[VisitorOutputT]) -> VisitorOutputT:
        """Visit this node."""
        return visitor.visit_node(self)


def make_graphviz_label(
    title: str, properties: List[DisplayedProperty], title_font_size: int = 12, property_font_size: int = 6
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


DagNodeT = TypeVar("DagNodeT", bound=DagNode)


class MetricFlowDag(Generic[DagNodeT]):  # noqa: D
    """Represents a directed acyclic graph. The sink nodes will have the connected components."""

    def __init__(self, dag_id: str, sink_nodes: List[DagNodeT]):  # noqa: D
        self._dag_id = dag_id
        self._sink_nodes = sink_nodes

    @property
    def dag_id(self) -> str:  # noqa: D
        return self._dag_id

    @property
    def sink_nodes(self) -> List[DagNodeT]:  # noqa: D
        return self._sink_nodes
