from __future__ import annotations

from typing import Sequence

from typing_extensions import override

from metricflow_semantics.dag.id_prefix import IdPrefix, StaticIdPrefix
from metricflow_semantics.query.group_by_item.resolution_dag.resolution_nodes.base_node import (
    GroupByItemResolutionNode,
    GroupByItemResolutionNodeSet,
    GroupByItemResolutionNodeVisitor,
)
from metricflow_semantics.query.group_by_item.resolution_dag.resolution_nodes.metric_resolution_node import (
    MetricGroupByItemResolutionNode,
)
from metricflow_semantics.visitor import VisitorOutputT


class NoMetricsGroupByItemSourceNode(GroupByItemResolutionNode):
    """Outputs group-by-items that can be queried without any metrics."""

    def __init__(self) -> None:  # noqa: D107
        super().__init__()

    @override
    def accept(self, visitor: GroupByItemResolutionNodeVisitor[VisitorOutputT]) -> VisitorOutputT:
        return visitor.visit_no_metrics_query_node(self)

    @property
    @override
    def description(self) -> str:
        return "Output the available group-by-items for a query without any metrics."

    @property
    @override
    def parent_nodes(self) -> Sequence[MetricGroupByItemResolutionNode]:
        return ()

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
