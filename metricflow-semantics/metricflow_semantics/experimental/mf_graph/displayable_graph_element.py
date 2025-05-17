from __future__ import annotations

from abc import ABC, abstractmethod

from metricflow_semantics.collection_helpers.mf_type_aliases import AnyLengthTuple
from metricflow_semantics.dag.mf_dag import DisplayedProperty
from metricflow_semantics.experimental.mf_graph.formatting.graphviz_attributes import (
    DotAttributeSet,
)


class MetricflowGraphLabel(ABC):
    pass


class HasDisplayedProperty(ABC):
    @property
    def displayed_properties(self) -> AnyLengthTuple[DisplayedProperty]:
        return ()


class MetricflowGraphElement(ABC):
    """An element in a graph (e.g. node)."""

    @property
    @abstractmethod
    def dot_attributes(self) -> DotAttributeSet:
        """Attributes to supply to `graphviz` calls when rendering this element."""
        raise NotImplementedError

    @property
    def displayed_properties(self) -> AnyLengthTuple[DisplayedProperty]:
        return ()
