from __future__ import annotations

from dataclasses import dataclass
from typing import Tuple

from typing_extensions import override

from metricflow_semantics.query.group_by_item.path_prefixable import PathPrefixable
from metricflow_semantics.query.group_by_item.resolution_dag.resolution_nodes.base_node import GroupByItemResolutionNode
from metricflow_semantics.toolkit.string_helpers import mf_indent


@dataclass(frozen=True)
class MetricFlowQueryResolutionPath(PathPrefixable):
    """Represents a path in the group-by-item resolution DAG."""

    resolution_path_nodes: Tuple[GroupByItemResolutionNode, ...]

    @staticmethod
    def empty_instance() -> MetricFlowQueryResolutionPath:  # noqa: D102
        return MetricFlowQueryResolutionPath(
            resolution_path_nodes=(),
        )

    @property
    def last_item(self) -> GroupByItemResolutionNode:
        """Return the last node in this path."""
        return self.resolution_path_nodes[-1]

    @property
    def ui_description(self) -> str:  # noqa: D102
        if len(self.resolution_path_nodes) == 0:
            return "[Empty Path]"
        # TODO: Centralize handling of error message formatting.
        max_line_length = 80
        lines = []

        # Generate text that shows where the error occurred using indents to show the nested structure.
        # e.g.
        #
        #     [Resolve Query(['bookings'])]
        #       -> [Resolve Metric('bookings')]
        #         -> [Resolve Measure('bookings')]

        for i, path_node in enumerate(self.resolution_path_nodes):
            if i == 0:
                indent_prefix = ""
            else:
                indent_prefix = mf_indent("-> ", indent_level=i)
            path_node_description = path_node.ui_description
            untruncated_line = indent_prefix + f"[Resolve {path_node_description}]"
            untruncated_line_length = len(untruncated_line)

            if untruncated_line_length > max_line_length:
                ellipsis_str = "...)"
                shorten_description_amount = untruncated_line_length - max_line_length + len(ellipsis_str)
                # Using `max()` in case of edge cases.
                shortened_description = path_node_description[
                    : max(1, len(path_node_description) - shorten_description_amount)
                ]
                lines.append(indent_prefix + f"[Resolve {shortened_description + ellipsis_str}]")
            else:
                lines.append(untruncated_line)

        return "\n".join(lines)

    @override
    def with_path_prefix(self, path_prefix: MetricFlowQueryResolutionPath) -> MetricFlowQueryResolutionPath:
        return MetricFlowQueryResolutionPath(
            resolution_path_nodes=path_prefix.resolution_path_nodes + self.resolution_path_nodes
        )

    @override
    def __str__(self) -> str:
        items = [self.__class__.__name__, "(", ", ".join(tuple(str(node) for node in self.resolution_path_nodes)), ")"]
        return "".join(items)

    @staticmethod
    def from_path_item(node: GroupByItemResolutionNode) -> MetricFlowQueryResolutionPath:
        """Creates a single element path with the given node."""
        return MetricFlowQueryResolutionPath(
            resolution_path_nodes=(node,),
        )
