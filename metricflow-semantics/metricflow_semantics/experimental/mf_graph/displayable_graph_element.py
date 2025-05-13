from __future__ import annotations

from abc import ABC, abstractmethod

from metricflow_semantics.experimental.ordered_set import OrderedSet


class MetricflowGraphProperty(ABC):
    pass


class MetricflowGraphElement(ABC):
    """An element in a graph (e.g. node) that can be displayed."""

    @property
    @abstractmethod
    def dot_label(self) -> str:
        """A short name that is used to describe this in DOT notation."""
        raise NotImplementedError

    @property
    @abstractmethod
    def graphviz_label(self) -> str:
        """The label to use when rendering this element using `graphviz`."""
        raise NotImplementedError

    @property
    @abstractmethod
    def properties(self) -> OrderedSet[MetricflowGraphProperty]:
        raise NotImplementedError
