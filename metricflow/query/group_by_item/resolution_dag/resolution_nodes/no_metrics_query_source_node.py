from __future__ import annotations

from typing import Sequence

from typing_extensions import override

from metricflow.dag.id_prefix import IdPrefix
from metricflow.query.group_by_item.resolution_dag.resolution_nodes.base_node import (
    GroupByItemResolutionNode,
    GroupByItemResolutionNodeVisitor,
)
from metricflow.query.group_by_item.resolution_dag.resolution_nodes.metric_resolution_node import (
    MetricGroupByItemResolutionNode,
)
from metricflow.visitor import VisitorOutputT


class NoMetricsGroupByItemSourceNode(GroupByItemResolutionNode):
    """Outputs group-by-items that can be queried without any metrics."""

    def __init__(self) -> None:  # noqa: D
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
    def id_prefix_enum(cls) -> IdPrefix:
        return IdPrefix.VALUES_GROUP_BY_ITEM_RESOLUTION_NODE

    @property
    @override
    def ui_description(self) -> str:
        return f"{self.__class__.__name__}()"
