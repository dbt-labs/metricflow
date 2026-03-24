"""Classes for modeling a DAG."""

from __future__ import annotations

import html
import logging
import textwrap
from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Any, Generic, Optional, Sequence, Tuple, TypeVar

import jinja2
from typing_extensions import override

from metricflow_semantics.dag.dag_to_text import MetricFlowDagTextFormatter
from metricflow_semantics.dag.id_prefix import IdPrefix
from metricflow_semantics.dag.sequential_id import SequentialIdGenerator
from metricflow_semantics.toolkit.mf_logging.pretty_formattable import MetricFlowPrettyFormattable
from metricflow_semantics.toolkit.mf_logging.pretty_formatter import PrettyFormatContext
from metricflow_semantics.toolkit.visitor import VisitorOutputT

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class DisplayedProperty:  # type: ignore
    """When visualizing a node as text or a graphic, the properties to display with it.

    This should be displayed in the form "{key} = {str(value)}"
    """

    key: str
    value: Any  # type: ignore


@dataclass(frozen=True, order=True)
class NodeId:
    """Unique identifier for nodes in DAGs."""

    id_str: str

    def __repr__(self) -> str:  # noqa: D105
        return self.id_str

    @staticmethod
    def create_unique(id_prefix: IdPrefix) -> NodeId:  # noqa: D102
        return NodeId(str(SequentialIdGenerator.create_next_id(id_prefix)))


class DagNodeVisitor(Generic[VisitorOutputT], ABC):
    """An object that can be used to visit the nodes of a DAG graph."""

    @abstractmethod
    def visit_node(self, node: DagNode) -> VisitorOutputT:  # noqa: D102
        pass


DagNodeT = TypeVar("DagNodeT", bound="DagNode")


@dataclass(frozen=True, eq=False)
class DagNode(MetricFlowPrettyFormattable, Generic[DagNodeT], ABC):
    """A node in a DAG. These should be immutable.

    Since there should only be a single instance of a node with a given ID, `eq` can be set to false so that equality
    operations can be done without comparing the fields. Comparing the fields can be a slow process since the
    `parent_nodes` field is recursive.
    """

    parent_nodes: Tuple[DagNodeT, ...]

    def __post_init__(self) -> None:  # noqa: D105
        object.__setattr__(self, "_post_init_node_id", self.create_unique_id())

    @property
    def node_id(self) -> NodeId:
        """ID for uniquely identifying a given node.

        Ideally, this field would have a default value. However, setting a default field in this class means that all
        subclasses would have to have default values for all the fields as default fields must come at the end.
        This issue is resolved in Python 3.10 with `kw_only`, so this can be updated once this project's minimum Python
        version is 3.10.

        Set via `__setattr___` in  `__post__init__` to workaround limitations of frozen dataclasses.
        """
        return getattr(self, "_post_init_node_id")

    @property
    @abstractmethod
    def description(self) -> str:
        """A human-readable description for this node."""
        pass

    @property
    def displayed_properties(self) -> Sequence[DisplayedProperty]:
        """When the node is represented in a visualization, the properties to display with it."""
        return (
            DisplayedProperty("description", self.description),
            DisplayedProperty("node_id", self.node_id),
        )

    @property
    def graphviz_label(self) -> str:
        """When using graphviz to render this node, the label that should be used in the construction."""
        return make_graphviz_label(
            title=self.__class__.__name__,
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
    def create_unique_id(cls) -> NodeId:
        """Create and return a unique identifier to use when creating nodes."""
        return NodeId(id_str=SequentialIdGenerator.create_next_id(cls.id_prefix()).str_value)

    def accept_dag_node_visitor(self, visitor: DagNodeVisitor[VisitorOutputT]) -> VisitorOutputT:
        """Visit this node."""
        return visitor.visit_node(self)

    def structure_text(self, formatter: MetricFlowDagTextFormatter = MetricFlowDagTextFormatter()) -> str:
        """Return a text representation that shows the structure of the DAG component starting from this node."""
        return formatter.dag_component_to_text(self)

    @override
    def pretty_format(self, format_context: PrettyFormatContext) -> Optional[str]:
        return f"{self.__class__.__name__}(node_id={self.node_id.id_str})"


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


@dataclass(frozen=True)
class DagId:
    """Unique identifier for DAGs."""

    id_str: str

    def __str__(self) -> str:  # noqa: D105
        return self.id_str

    @staticmethod
    def from_str(id_str: str) -> DagId:
        """Migration helper to create DAG IDs."""
        return DagId(id_str)

    @staticmethod
    def from_id_prefix(id_prefix: IdPrefix) -> DagId:  # noqa: D102
        return DagId(id_str=SequentialIdGenerator.create_next_id(id_prefix).str_value)


class MetricFlowDag(Generic[DagNodeT]):
    """Represents a directed acyclic graph. The sink nodes will have the connected components."""

    def __init__(self, dag_id: DagId, sink_nodes: Sequence[DagNodeT]):  # noqa: D107
        self._dag_id = dag_id
        self._sink_nodes = tuple(sink_nodes)

    @property
    def dag_id(self) -> DagId:  # noqa: D102
        return self._dag_id

    @property
    def sink_nodes(self) -> Sequence[DagNodeT]:  # noqa: D102
        return self._sink_nodes

    def structure_text(self, formatter: MetricFlowDagTextFormatter = MetricFlowDagTextFormatter()) -> str:
        """Return a text representation that shows the structure of this DAG."""
        return formatter.dag_to_text(self)
