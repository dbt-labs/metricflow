from __future__ import annotations

from abc import ABC
from functools import cached_property

from typing_extensions import override

from metricflow_semantics.toolkit.mf_graph.comparable import Comparable, ComparisonKey


class MetricFlowGraphLabel(Comparable, ABC):
    """Base class for objects that can be used to lookup nodes / edges in a graph."""

    @cached_property
    @override
    def comparison_key(self) -> ComparisonKey:
        return ()
