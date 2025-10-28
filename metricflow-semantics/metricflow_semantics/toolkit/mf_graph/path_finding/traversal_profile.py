from __future__ import annotations

import logging
from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Optional

from typing_extensions import override

from metricflow_semantics.toolkit.mf_logging.pretty_formattable import MetricFlowPrettyFormattable
from metricflow_semantics.toolkit.mf_logging.pretty_formatter import PrettyFormatContext

logger = logging.getLogger(__name__)


class GraphTraversalProfile(MetricFlowPrettyFormattable, ABC):
    """A read-only interface for a profile containing counters related to graph traversal.

    Used for debugging / logs.
    """

    @property
    @abstractmethod
    def visited_nodes_count(self) -> int:  # noqa: D102
        raise NotImplementedError()

    @property
    @abstractmethod
    def examined_edges_count(self) -> int:  # noqa: D102
        raise NotImplementedError()

    @property
    @abstractmethod
    def generated_paths_count(self) -> int:  # noqa: D102
        raise NotImplementedError()

    @override
    def pretty_format(self, format_context: PrettyFormatContext) -> Optional[str]:
        return format_context.formatter.pretty_format(
            {
                "node_visit_count": self.visited_nodes_count,
                "edge_examination_count": self.examined_edges_count,
                "path_creation_count": self.generated_paths_count,
            }
        )

    @abstractmethod
    def difference(self, other: GraphTraversalProfile) -> GraphTraversalProfile:  # noqa: D102
        raise NotImplementedError


@dataclass
class MutableGraphTraversalProfile(GraphTraversalProfile):
    """A mutable version of `GraphTraversalProfile` with counters that can be incremented."""

    _visited_nodes_count: int = 0
    _examined_edges_count: int = 0
    _generated_paths_count: int = 0

    def increment_node_visit_count(self, visit_count: int = 1) -> None:  # noqa: D102
        self._visited_nodes_count += visit_count

    def increment_edge_examined_count(self, examined_count: int = 1) -> None:  # noqa: D102
        self._examined_edges_count += examined_count

    def increment_generated_paths_count(self, generated_count: int = 1) -> None:  # noqa: D102
        self._generated_paths_count += generated_count

    @property
    def visited_nodes_count(self) -> int:  # noqa: D102
        return self._visited_nodes_count

    @property
    def examined_edges_count(self) -> int:  # noqa: D102
        return self._visited_nodes_count

    @property
    def generated_paths_count(self) -> int:  # noqa: D102
        return self._generated_paths_count

    @override
    def difference(self, other: GraphTraversalProfile) -> GraphTraversalProfile:  # noqa: D102
        return MutableGraphTraversalProfile(
            _visited_nodes_count=self._visited_nodes_count - other.visited_nodes_count,
            _examined_edges_count=self._examined_edges_count - other.examined_edges_count,
            _generated_paths_count=self._generated_paths_count - other.generated_paths_count,
        )

    def copy(self) -> MutableGraphTraversalProfile:  # noqa: D102
        return MutableGraphTraversalProfile(
            _visited_nodes_count=self._visited_nodes_count,
            _examined_edges_count=self._examined_edges_count,
            _generated_paths_count=self._generated_paths_count,
        )
