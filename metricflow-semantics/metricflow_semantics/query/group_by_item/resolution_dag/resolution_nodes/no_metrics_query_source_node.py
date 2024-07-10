from __future__ import annotations

from dataclasses import dataclass

from typing_extensions import override

from metricflow_semantics.dag.id_prefix import IdPrefix, StaticIdPrefix
from metricflow_semantics.query.group_by_item.resolution_dag.resolution_nodes.base_node import (
    GroupByItemResolutionNode,
    GroupByItemResolutionNodeSet,
    GroupByItemResolutionNodeVisitor,
)
from metricflow_semantics.visitor import VisitorOutputT


@dataclass(frozen=True)
class NoMetricsGroupByItemSourceNode(GroupByItemResolutionNode):
    """Outputs group-by-items that can be queried without any metrics."""

    def __post_init__(self) -> None:  # noqa: D105
        super().__post_init__()
        assert len(self.parent_nodes) == 0

    @staticmethod
    def create() -> NoMetricsGroupByItemSourceNode:  # noqa: D102
        return NoMetricsGroupByItemSourceNode(parent_nodes=())

    @override
    def accept(self, visitor: GroupByItemResolutionNodeVisitor[VisitorOutputT]) -> VisitorOutputT:
        return visitor.visit_no_metrics_query_node(self)

    @property
    @override
    def description(self) -> str:
        return "Output the available group-by-items for a query without any metrics."

    @classmethod
    @override
    def id_prefix(cls) -> IdPrefix:
        return StaticIdPrefix.VALUES_GROUP_BY_ITEM_RESOLUTION_NODE

    @property
    @override
    def ui_description(self) -> str:
        return f"{self.__class__.__name__}()"

    @override
    def _self_set(self) -> GroupByItemResolutionNodeSet:
        return GroupByItemResolutionNodeSet(no_metrics_query_nodes=(self,))
