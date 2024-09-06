from __future__ import annotations

from dataclasses import dataclass

from metricflow_semantics.dag.id_prefix import IdPrefix
from metricflow_semantics.dag.sequential_id import SequentialIdGenerator


@dataclass(frozen=True)
class GraphElementId:
    """Unique identifier for elements (e.g. nodes, edges) in a graph."""

    str_value: str

    def __repr__(self) -> str:  # noqa: D105
        return self.str_value

    @staticmethod
    def create_unique(id_prefix: IdPrefix) -> GraphElementId:  # noqa: D102
        return GraphElementId(str(SequentialIdGenerator.create_next_id(id_prefix)))
