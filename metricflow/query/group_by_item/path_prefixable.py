from __future__ import annotations

from abc import ABC, abstractmethod

from typing_extensions import TYPE_CHECKING, Self

if TYPE_CHECKING:
    from metricflow.query.group_by_item.resolution_path import MetricFlowQueryResolutionPath


class PathPrefixable(ABC):
    """Describes an object that contains a path that can be updated with a prefix path.

    During a recursive traversal of a DAG, we keep track of the path from the start node to the current node. From the
    current node, a method generates a relative path from the current node to a target node. To get the path from the
    start node to the target node, those to paths can be joined with the path from the start node to the current node
    as a prefix of the path from the current node to the target node.
    """

    @abstractmethod
    def with_path_prefix(self, path_prefix: MetricFlowQueryResolutionPath) -> Self:
        """Return a copy of Self, but with the associated path to include the path_prefix_node at the beginning."""
        raise NotImplementedError
