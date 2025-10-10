"""Classes for representing DOT objects as `graphviz` uses strings as arguments.

Generally, only the attributes / attribute values that are used in this project are listed. For a full list, see:
    https://www.graphviz.org/doc/info/attrs.html
"""
from __future__ import annotations

import logging
from collections.abc import Mapping
from enum import Enum
from typing import Optional

from metricflow_semantics.toolkit.dataclass_helpers import fast_frozen_dataclass
from metricflow_semantics.toolkit.syntactic_sugar import (
    mf_ensure_mapping,
    mf_first_non_none,
    mf_first_non_none_or_raise,
)

logger = logging.getLogger(__name__)


class DotNodeShape(Enum):
    """An enumeration of the `graphviz` shapes used in this project."""

    PLAIN = "plain"
    BOX = "box"
    ELLIPSE = "ellipse"


class DotEdgeArrowShape(Enum):
    """An enumeration of the `graphviz` edge-arrow shapes used in this project."""

    NORMAL = "normal"
    NONE = "none"


class DotColor(Enum):
    """An enumeration of the `graphviz` colors used in this project."""

    CORNFLOWER_BLUE = "#87CEFA"
    SALMON_PINK = "#FFA07A"
    EXTRA_DIM_GRAY = "#303030"
    GRAY = "#808080"
    DARK_GRAY = "#181818"
    LIGHT_GRAY = "#D3D3D3"
    LIME_GREEN = "#006400"
    BLACK = "#000000"
    GOLD = "#FFD700"
    # Note the 2 additional hex characters for the alpha.
    TRANSPARENT = "#00000000"


@fast_frozen_dataclass()
class DotGraphAttributeSet:
    """Attributes for graphs in DOT.

    Note: this does not contain the objects in the graph.
    """

    # Name of the graph.
    name: str
    # Additional keyword arguments for the call to create a DOT graph instance.
    additional_kwargs: Mapping[str, str]

    @staticmethod
    def create(name: str, additional_kwargs: Optional[Mapping[str, str]] = None) -> DotGraphAttributeSet:
        """See field comments."""
        return DotGraphAttributeSet(
            name=name,
            additional_kwargs=mf_ensure_mapping(additional_kwargs),
        )

    def with_attributes(
        self, name: Optional[str] = None, dot_kwargs: Optional[Mapping[str, str]] = None
    ) -> DotGraphAttributeSet:
        """Create a copy but with fields set to the present arguments."""
        return DotGraphAttributeSet(
            name=mf_first_non_none_or_raise(name, self.name),
            additional_kwargs=dict(self.additional_kwargs, **(mf_ensure_mapping(dot_kwargs))),
        )

    def merge(self, other: DotGraphAttributeSet) -> DotGraphAttributeSet:  # noqa: D102
        return DotGraphAttributeSet(
            name=mf_first_non_none_or_raise(self.name, other.name),
            additional_kwargs=dict(self.additional_kwargs, **(mf_ensure_mapping(other.additional_kwargs))),
        )

    @property
    def dot_graph_attrs(self) -> dict[str, str]:
        """Return the `graph_attr` dict for creating the graph in DOT."""
        kwargs = {"name": self.name}
        kwargs.update(self.additional_kwargs)
        return kwargs


@fast_frozen_dataclass()
class DotEdgeAttributeSet:
    """Attributes for edges in DOT."""

    tail_name: str
    head_name: str
    color: Optional[DotColor]
    label: Optional[str]
    arrow_shape: Optional[DotEdgeArrowShape]
    additional_kwargs: Mapping[str, str]

    @staticmethod
    def create(  # noqa: D102
        tail_name: str,
        head_name: str,
        color: Optional[DotColor] = None,
        label: Optional[str] = None,
        arrow_shape: Optional[DotEdgeArrowShape] = None,
        additional_kwargs: Optional[Mapping[str, str]] = None,
    ) -> DotEdgeAttributeSet:
        return DotEdgeAttributeSet(
            tail_name=tail_name,
            head_name=head_name,
            color=color,
            label=label,
            arrow_shape=arrow_shape,
            additional_kwargs=mf_ensure_mapping(additional_kwargs),
        )

    def merge(self, other: DotEdgeAttributeSet) -> DotEdgeAttributeSet:  # noqa: D102
        return DotEdgeAttributeSet(
            tail_name=other.tail_name,
            head_name=other.head_name,
            color=mf_first_non_none(other.color, self.color),
            label=mf_first_non_none(other.label, self.label),
            arrow_shape=mf_first_non_none(other.arrow_shape, self.arrow_shape),
            additional_kwargs=dict(**self.additional_kwargs, **other.additional_kwargs),
        )

    @property
    def dot_kwargs(self) -> Mapping[str, str]:
        """Return the keyword arguments for calling `.edge()` in DOT."""
        kwargs = {
            "tail_name": self.tail_name,
            "head_name": self.head_name,
        }
        if self.color is not None:
            kwargs["color"] = self.color.value

        if self.label is not None:
            kwargs["label"] = self.label

        if self.arrow_shape is not None:
            kwargs["arrowhead"] = self.arrow_shape.value

        kwargs.update(**self.additional_kwargs)

        return kwargs


class DotRankKey(Enum):
    """A key used to group nodes and assign them the same `graphviz` rank.

    Only edge-as-nodes are configured currently, but more nodes may be tagged.
    """

    EDGE_AS_NODE = "edge"


@fast_frozen_dataclass()
class DotNodeAttributeSet:
    """Attributes for nodes in DOT."""

    name: str
    color: Optional[DotColor]
    fill_color: Optional[DotColor]
    label: Optional[str]
    shape: Optional[DotNodeShape]

    # When rendering graphs, nodes with the same rank key are force to have the same rank in the rendering engine.
    rank_key: Optional[DotRankKey]

    # When rendering an edge `A -> B` as `A -> [Label Node] -> B`, the label node can be placed in a more organized
    # way if it is place in the same cluster as the one for `A` or `B`. Since the nodes could belong to different
    # clusters the cluster of the node with the highest priority is selected.
    cluster_priority_for_edge_as_node: Optional[int]

    # Additional keyword arguments for the call to create a DOT graph instance.
    additional_kwargs: Mapping[str, str]

    @staticmethod
    def create(  # noqa: D102
        name: str,
        color: Optional[DotColor] = None,
        fill_color: Optional[DotColor] = None,
        label: Optional[str] = None,
        shape: Optional[DotNodeShape] = None,
        rank_key: Optional[DotRankKey] = None,
        edge_node_priority: Optional[int] = None,
        additional_kwargs: Optional[Mapping[str, str]] = None,
    ) -> DotNodeAttributeSet:
        return DotNodeAttributeSet(
            name=name,
            color=color,
            fill_color=fill_color,
            label=label,
            shape=shape,
            rank_key=rank_key,
            cluster_priority_for_edge_as_node=edge_node_priority,
            additional_kwargs=mf_ensure_mapping(additional_kwargs),
        )

    def merge(self, other: DotNodeAttributeSet) -> DotNodeAttributeSet:  # noqa: D102
        return DotNodeAttributeSet.create(
            name=other.name,
            color=other.color or self.color,
            label=other.label or self.label,
            shape=other.shape or self.shape,
            rank_key=other.rank_key or self.rank_key,
            edge_node_priority=other.cluster_priority_for_edge_as_node or self.cluster_priority_for_edge_as_node,
            additional_kwargs=dict(**self.additional_kwargs, **other.additional_kwargs),
        )

    @property
    def dot_kwargs(self) -> Mapping[str, str]:
        """Return the keyword arguments for calling `.node()` in DOT."""
        kwargs = {
            "name": self.name,
        }
        if self.color is not None:
            kwargs["color"] = self.color.value

        if self.fill_color is not None:
            kwargs["fillcolor"] = self.fill_color.value

        if self.label is not None:
            kwargs["label"] = self.label

        if self.shape is not None:
            kwargs["shape"] = self.shape.value

        kwargs.update(**self.additional_kwargs)
        return kwargs

    def with_attributes(
        self,
        color: Optional[DotColor] = None,
        fill_color: Optional[DotColor] = None,
        label: Optional[str] = None,
        shape: Optional[DotNodeShape] = None,
        rank_key: Optional[DotRankKey] = None,
        edge_node_priority: Optional[int] = None,
        additional_kwargs: Optional[Mapping[str, str]] = None,
    ) -> DotNodeAttributeSet:
        """Return a copy of this with fields set to the present arguments."""
        return DotNodeAttributeSet(
            name=self.name,
            color=mf_first_non_none(color, self.color),
            fill_color=mf_first_non_none(fill_color, self.fill_color),
            label=mf_first_non_none(label, self.label),
            shape=mf_first_non_none(shape, self.shape),
            rank_key=mf_first_non_none(rank_key, self.rank_key),
            cluster_priority_for_edge_as_node=edge_node_priority,
            additional_kwargs=mf_ensure_mapping(mf_first_non_none(additional_kwargs, self.additional_kwargs)),
        )
