from __future__ import annotations

from dataclasses import dataclass
from typing import Tuple

from metricflow_semantics.experimental.semantic_graph.graph_edges import SemanticGraphEdge


@dataclass(frozen=True)
class SemanticGraphPath:
    edges: Tuple[SemanticGraphEdge, ...]

    def __post_init__(self) -> None:  # noqa: D105
        if len(self.edges) == 0:
            raise RuntimeError(f"Can't create with empty {self.edges=}")
