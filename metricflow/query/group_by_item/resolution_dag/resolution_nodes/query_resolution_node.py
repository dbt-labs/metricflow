from __future__ import annotations

from typing import List, Sequence, Union

from dbt_semantic_interfaces.protocols import WhereFilterIntersection
from dbt_semantic_interfaces.references import MetricReference
from typing_extensions import override

from metricflow.dag.id_prefix import IdPrefix
from metricflow.dag.mf_dag import DisplayedProperty
from metricflow.query.group_by_item.resolution_dag.resolution_nodes.base_node import (
    GroupByItemResolutionNode,
    GroupByItemResolutionNodeVisitor,
)
from metricflow.query.group_by_item.resolution_dag.resolution_nodes.metric_resolution_node import (
    MetricGroupByItemResolutionNode,
)
from metricflow.query.group_by_item.resolution_dag.resolution_nodes.no_metrics_query_source_node import (
    NoMetricsGroupByItemSourceNode,
)
from metricflow.visitor import VisitorOutputT


class QueryGroupByItemResolutionNode(GroupByItemResolutionNode):
    """Output the group-by-items relevant to the query and based on the inputs."""

    def __init__(  # noqa: D
        self,
        parent_nodes: Sequence[Union[MetricGroupByItemResolutionNode, NoMetricsGroupByItemSourceNode]],
        metrics_in_query: Sequence[MetricReference],
        where_filter_intersection: WhereFilterIntersection,
    ) -> None:
        self._parent_nodes = tuple(parent_nodes)
        self._metrics_in_query = tuple(metrics_in_query)
        self._where_filter_intersection = where_filter_intersection
        super().__init__()

    @override
    def accept(self, visitor: GroupByItemResolutionNodeVisitor[VisitorOutputT]) -> VisitorOutputT:
        return visitor.visit_query_node(self)

    @property
    @override
    def description(self) -> str:
        return "Output the group-by items for query."

    @property
    @override
    def parent_nodes(self) -> Sequence[Union[MetricGroupByItemResolutionNode, NoMetricsGroupByItemSourceNode]]:
        return self._parent_nodes

    @classmethod
    @override
    def id_prefix_enum(cls) -> IdPrefix:
        return IdPrefix.QUERY_GROUP_BY_ITEM_RESOLUTION_NODE

    @property
    def metrics_in_query(self) -> Sequence[MetricReference]:
        """Return the metrics that are queried in this query."""
        return self._metrics_in_query

    @property
    @override
    def displayed_properties(self) -> List[DisplayedProperty]:
        properties = list(super().displayed_properties)

        if len(self.metrics_in_query) > 0:
            properties.append(
                DisplayedProperty(
                    key="metrics_in_query",
                    value=[metric_reference.element_name for metric_reference in self.metrics_in_query],
                )
            )

        if len(self.where_filter_intersection.where_filters) > 0:
            properties.append(
                DisplayedProperty(
                    key="where_filter",
                    value=[
                        where_filter.where_sql_template for where_filter in self.where_filter_intersection.where_filters
                    ],
                )
            )

        return properties

    @property
    def where_filter_intersection(self) -> WhereFilterIntersection:  # noqa: D
        return self._where_filter_intersection

    @property
    @override
    def ui_description(self) -> str:
        return f"Query({repr([metric_reference.element_name for metric_reference in self._metrics_in_query])})"
