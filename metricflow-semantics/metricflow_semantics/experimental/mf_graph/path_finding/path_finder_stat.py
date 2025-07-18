from __future__ import annotations

import logging
from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Optional

from typing_extensions import override

from metricflow_semantics.mf_logging.pretty_formattable import MetricFlowPrettyFormattable
from metricflow_semantics.mf_logging.pretty_formatter import PrettyFormatContext

logger = logging.getLogger(__name__)


class PathFinderStat(MetricFlowPrettyFormattable, ABC):
    @property
    @abstractmethod
    def visited_nodes_count(self) -> int:
        raise NotImplementedError()

    @property
    @abstractmethod
    def examined_edges_count(self) -> int:
        raise NotImplementedError()

    @property
    @abstractmethod
    def generated_paths_count(self) -> int:
        raise NotImplementedError()

    @override
    def pretty_format(self, format_context: PrettyFormatContext) -> Optional[str]:
        return format_context.formatter.pretty_format(
            {
                "visited_nodes_count": self.visited_nodes_count,
                "examined_edges_count": self.examined_edges_count,
                "generated_paths_count": self.generated_paths_count,
            }
        )


@dataclass
class MutablePathFinderStat(PathFinderStat):
    _visited_nodes_count: int = 0
    _examined_edges_count: int = 0
    _generated_paths_count: int = 0

    def increment_node_visit_count(self, visit_count: int = 1) -> None:
        self._visited_nodes_count += visit_count

    def increment_edge_examined_count(self, examined_count: int = 1) -> None:
        self._examined_edges_count += examined_count

    def increment_generated_paths_count(self, generated_count: int = 1) -> None:
        self._generated_paths_count += generated_count

    @property
    def visited_nodes_count(self) -> int:
        return self._visited_nodes_count

    @property
    def examined_edges_count(self) -> int:
        return self._visited_nodes_count

    @property
    def generated_paths_count(self) -> int:
        return self._generated_paths_count

    def difference(self, other: MutablePathFinderStat) -> MutablePathFinderStat:
        return MutablePathFinderStat(
            _visited_nodes_count=self._visited_nodes_count - other.visited_nodes_count,
            _examined_edges_count=self._examined_edges_count - other.examined_edges_count,
            _generated_paths_count=self._generated_paths_count - other.generated_paths_count,
        )

    def copy(self) -> MutablePathFinderStat:
        return MutablePathFinderStat(
            _visited_nodes_count=self._visited_nodes_count,
            _examined_edges_count=self._examined_edges_count,
            _generated_paths_count=self._generated_paths_count,
        )
