from __future__ import annotations

from dataclasses import dataclass
from typing import List, Sequence, Tuple

from dbt_semantic_interfaces.protocols import WhereFilterIntersection
from dbt_semantic_interfaces.references import MetricReference
from typing_extensions import override

from metricflow_semantics.dag.id_prefix import IdPrefix, StaticIdPrefix
from metricflow_semantics.dag.mf_dag import DisplayedProperty
from metricflow_semantics.query.group_by_item.resolution_dag.resolution_nodes.base_node import (
    GroupByItemResolutionNode,
    GroupByItemResolutionNodeSet,
    GroupByItemResolutionNodeVisitor,
)
from metricflow_semantics.toolkit.mf_logging.format_option import PrettyFormatOption
from metricflow_semantics.toolkit.mf_logging.pretty_formatter import MetricFlowPrettyFormatter
from metricflow_semantics.toolkit.visitor import VisitorOutputT


@dataclass(frozen=True)
class QueryGroupByItemResolutionNode(GroupByItemResolutionNode):
    """Output the group-by-items relevant to the query and based on the inputs.

    Attributes:
        parent_nodes: The parent nodes of this query.
        metrics_in_query: The metrics that are queried in this query.
        where_filter_intersection: The intersection of where filters.
    """

    parent_nodes: Tuple[GroupByItemResolutionNode, ...]
    metrics_in_query: Tuple[MetricReference, ...]
    where_filter_intersection: WhereFilterIntersection

    @staticmethod
    def create(  # noqa: D102
        parent_nodes: Sequence[GroupByItemResolutionNode],
        metrics_in_query: Sequence[MetricReference],
        where_filter_intersection: WhereFilterIntersection,
    ) -> QueryGroupByItemResolutionNode:
        return QueryGroupByItemResolutionNode(
            parent_nodes=tuple(parent_nodes),
            metrics_in_query=tuple(metrics_in_query),
            where_filter_intersection=where_filter_intersection,
        )

    @override
    def accept(self, visitor: GroupByItemResolutionNodeVisitor[VisitorOutputT]) -> VisitorOutputT:
        return visitor.visit_query_node(self)

    @property
    @override
    def description(self) -> str:
        return "Output the group-by items for query."

    @classmethod
    @override
    def id_prefix(cls) -> IdPrefix:
        return StaticIdPrefix.QUERY_GROUP_BY_ITEM_RESOLUTION_NODE

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
    @override
    def ui_description(self) -> str:
        # TODO: Remove trailing comma.
        pretty_formatter = MetricFlowPrettyFormatter(
            PrettyFormatOption(include_object_field_names=False, max_line_length=None)
        )
        metric_descriptions = [metric_reference.element_name for metric_reference in self.metrics_in_query]
        return pretty_formatter.pretty_format_object_by_parts(
            class_name="Query",
            field_mapping={"metrics": metric_descriptions},
        )

    @override
    def _self_set(self) -> GroupByItemResolutionNodeSet:
        return GroupByItemResolutionNodeSet(query_nodes=(self,))
