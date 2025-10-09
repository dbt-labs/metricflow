from __future__ import annotations

import logging
from typing import Optional, Sequence, Union

from dbt_semantic_interfaces.implementations.filters.where_filter import PydanticWhereFilterIntersection
from dbt_semantic_interfaces.protocols import WhereFilterIntersection
from dbt_semantic_interfaces.references import MetricReference

from metricflow_semantics.model.semantic_manifest_lookup import SemanticManifestLookup
from metricflow_semantics.model.semantics.metric_lookup import MetricLookup
from metricflow_semantics.query.group_by_item.resolution_dag.dag import GroupByItemResolutionDag
from metricflow_semantics.query.group_by_item.resolution_dag.input_metric_location import InputMetricDefinitionLocation
from metricflow_semantics.query.group_by_item.resolution_dag.resolution_nodes.metric_resolution_node import (
    ComplexMetricGroupByItemResolutionNode,
)
from metricflow_semantics.query.group_by_item.resolution_dag.resolution_nodes.no_metrics_query_source_node import (
    NoMetricsGroupByItemSourceNode,
)
from metricflow_semantics.query.group_by_item.resolution_dag.resolution_nodes.query_resolution_node import (
    QueryGroupByItemResolutionNode,
)
from metricflow_semantics.query.group_by_item.resolution_dag.resolution_nodes.simple_metric_source_node import (
    SimpleMetricGroupByItemSourceNode,
)

logger = logging.getLogger(__name__)


class GroupByItemResolutionDagBuilder:
    """Builds a GroupByItemResolutionDag that can be used to resolve group-by-item specs from spec patterns."""

    def __init__(self, manifest_lookup: SemanticManifestLookup) -> None:  # noqa: D107
        self._manifest_lookup = manifest_lookup

    def _build_dag_component_for_metric(
        self,
        metric_reference: MetricReference,
        metric_input_location: Optional[InputMetricDefinitionLocation],
    ) -> Union[SimpleMetricGroupByItemSourceNode, ComplexMetricGroupByItemResolutionNode]:
        """Builds a DAG component that represents the resolution flow for a metric."""
        metric = self._manifest_lookup.metric_lookup.get_metric(metric_reference)

        metric_inputs = MetricLookup.metric_inputs(metric, include_conversion_metric_input=False)

        if len(metric_inputs) == 0:
            return SimpleMetricGroupByItemSourceNode.create(metric_reference, metric_input_location)

        parent_nodes: list[Union[SimpleMetricGroupByItemSourceNode, ComplexMetricGroupByItemResolutionNode]] = []
        for i, metric_input in enumerate(metric_inputs):
            metric_input_reference = MetricReference(metric_input.name)
            parent_nodes.append(
                self._build_dag_component_for_metric(
                    metric_reference=metric_input_reference,
                    metric_input_location=InputMetricDefinitionLocation(
                        metric_reference,
                        input_metric_list_index=i,
                    ),
                )
            )

        return ComplexMetricGroupByItemResolutionNode.create(
            metric_reference=metric_reference,
            metric_input_location=metric_input_location,
            parent_nodes=tuple(parent_nodes),
        )

    def _build_dag_component_for_query(
        self, metric_references: Sequence[MetricReference], where_filter_intersection: WhereFilterIntersection
    ) -> QueryGroupByItemResolutionNode:
        """Builds a DAG component that represents the resolution flow for a query."""
        if len(metric_references) == 0:
            return QueryGroupByItemResolutionNode.create(
                parent_nodes=(NoMetricsGroupByItemSourceNode.create(),),
                metrics_in_query=metric_references,
                where_filter_intersection=where_filter_intersection,
            )
        return QueryGroupByItemResolutionNode.create(
            parent_nodes=tuple(
                self._build_dag_component_for_metric(
                    metric_reference=metric_reference,
                    metric_input_location=None,
                )
                for metric_reference in metric_references
            ),
            metrics_in_query=metric_references,
            where_filter_intersection=where_filter_intersection,
        )

    def build(
        self, metric_references: Sequence[MetricReference], where_filter_intersection: Optional[WhereFilterIntersection]
    ) -> GroupByItemResolutionDag:
        """Build a resolution DAG for a query.

        Args:
            metric_references: The metrics in the query.
            where_filter_intersection: The filters in the query.

        Returns:
            The associated group-by-item resolution DAG.
        """
        return GroupByItemResolutionDag(
            sink_node=self._build_dag_component_for_query(
                metric_references=metric_references,
                where_filter_intersection=where_filter_intersection
                or PydanticWhereFilterIntersection(where_filters=[]),
            )
        )
