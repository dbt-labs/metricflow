from __future__ import annotations

from abc import ABC, abstractmethod

from typing_extensions import Self

from metricflow.query.group_by_item.resolution_dag.resolution_nodes.base_node import GroupByItemResolutionNode


class PathPrefixable(ABC):
    """Describes an object that contains a path that can be updated with a prefix node.

    This is useful for building a path to a node in the process of a recursive call. e.g. to create a path from a start
    node to target node, recursively traverse the DAG. During recursive traversal, when the traversal process reaches
    the target node, create a path that contains only that node as a path element. As the recursive call unwinds, add
    the node where the call unwinds. If this is done all the way to the leaf node, you'll have a path from the leaf node
    to the target node.
    """

    @abstractmethod
    def with_path_prefix(self, path_prefix_node: GroupByItemResolutionNode) -> Self:
        """Return a copy of Self, but with the associated path to include the path_prefix_node at the beginning."""
        raise NotImplementedError
