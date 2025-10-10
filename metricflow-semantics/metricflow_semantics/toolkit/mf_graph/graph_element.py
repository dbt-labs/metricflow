from __future__ import annotations

from abc import ABC

from metricflow_semantics.dag.mf_dag import DisplayedProperty
from metricflow_semantics.toolkit.mf_type_aliases import AnyLengthTuple


class HasDisplayedProperty(ABC):
    """Mixin for classes that have properties that should be displayed in visualizations."""

    @property
    def displayed_properties(self) -> AnyLengthTuple[DisplayedProperty]:
        """Return the properties that should be displayed in visualization."""
        return ()


class MetricFlowGraphElement(HasDisplayedProperty, ABC):
    """An element in a graph (e.g. node)."""

    pass
