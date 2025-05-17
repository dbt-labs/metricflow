from __future__ import annotations

import logging
from abc import ABC
from enum import Enum
from typing import Optional

from typing_extensions import override

from metricflow_semantics.collection_helpers.merger import Mergeable
from metricflow_semantics.experimental.dataclass_helpers import fast_frozen_dataclass

logger = logging.getLogger(__name__)


class GraphvizShape(Enum):
    """An enumeration of the shapes supported by `graphviz`."""

    BOX = "box"
    ELLIPSE = "ellipse"


class GraphvizColor(Enum):
    CORNFLOWER_BLUE = "#87CEFA"
    SALMON_PINK = "#FFA07A"
    EXTRA_DIM_GRAY = "#303030"
    GRAY = "#808080"
    DARK_GRAY = "#181818"
    LIGHT_GRAY = "#D3D3D3"
    LIME_GREEN = "#006400"
    BLACK = "#000000"
    GOLD = "#FFD700"


@fast_frozen_dataclass()
class DotAttributeSet(ABC):
    """Supported `graphviz` attributes for rendering elements.

    See: https://www.graphviz.org/doc/info/attrs.html
    """

    label: Optional[str]
    shape: Optional[GraphvizShape]
    color: Optional[GraphvizColor]

    def as_kwargs(self) -> dict[str, str]:
        kwargs = {}
        if self.label is not None:
            kwargs["label"] = self.label
        if self.shape is not None:
            kwargs["shape"] = self.shape.value
        if self.color is not None:
            kwargs["color"] = self.color.value
        return kwargs


@fast_frozen_dataclass()
class DotNodeAttributeSet(DotAttributeSet, Mergeable):
    """Supported `graphviz` attributes for rendering elements.

    See: https://www.graphviz.org/doc/info/attrs.html
    """

    name: Optional[str]

    @staticmethod
    def create(
        name: Optional[str] = None,
        label: Optional[str] = None,
        shape: Optional[GraphvizShape] = None,
        color: Optional[GraphvizColor] = None,
    ) -> DotNodeAttributeSet:
        return DotNodeAttributeSet(
            name=name,
            label=label,
            shape=shape,
            color=color,
        )

    @override
    def as_kwargs(self) -> dict[str, str]:
        if self.name is not None:
            return super().as_kwargs() | {"name": self.name}
        else:
            return super().as_kwargs()

    @override
    def merge(self, other: DotNodeAttributeSet) -> DotNodeAttributeSet:
        return DotNodeAttributeSet.create(
            name=other.name or self.name,
            label=other.label or self.label,
            shape=other.shape or self.shape,
            color=other.color or self.color,
        )

    @classmethod
    @override
    def empty_instance(cls) -> DotNodeAttributeSet:
        return DotNodeAttributeSet.create()


@fast_frozen_dataclass()
class DotEdgeAttributeSet(DotAttributeSet, Mergeable):
    """Supported `graphviz` attributes for rendering elements.

    See: https://www.graphviz.org/doc/info/attrs.html
    """

    tail_node_name: Optional[str]
    head_node_name: Optional[str]

    @staticmethod
    def create(
        tail_node_name: Optional[str] = None,
        head_node_name: Optional[str] = None,
        label: Optional[str] = None,
        shape: Optional[GraphvizShape] = None,
        color: Optional[GraphvizColor] = None,
    ) -> DotEdgeAttributeSet:
        return DotEdgeAttributeSet(
            tail_node_name=tail_node_name,
            head_node_name=head_node_name,
            label=label,
            shape=shape,
            color=color,
        )

    @override
    def as_kwargs(self) -> dict[str, str]:
        result = dict(super().as_kwargs())
        if self.tail_node_name is not None:
            result["tail_name"] = self.tail_node_name
        if self.head_node_name is not None:
            result["head_name"] = self.head_node_name
        return result

    @override
    def merge(self, other: DotEdgeAttributeSet) -> DotEdgeAttributeSet:
        return DotEdgeAttributeSet.create(
            tail_node_name=other.tail_node_name or self.tail_node_name,
            head_node_name=other.head_node_name or self.head_node_name,
            label=other.label or self.label,
            shape=other.shape or self.shape,
            color=other.color or self.color,
        )

    @classmethod
    @override
    def empty_instance(cls) -> DotEdgeAttributeSet:
        return DotEdgeAttributeSet.create()
