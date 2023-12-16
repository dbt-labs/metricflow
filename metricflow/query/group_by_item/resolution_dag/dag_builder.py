from __future__ import annotations

import logging
from typing import Optional, Sequence, Tuple

from dbt_semantic_interfaces.implementations.filters.where_filter import PydanticWhereFilterIntersection
from dbt_semantic_interfaces.protocols import WhereFilterIntersection
from dbt_semantic_interfaces.references import MeasureReference, MetricReference
from dbt_semantic_interfaces.type_enums import MetricType

from metricflow.model.semantic_manifest_lookup import SemanticManifestLookup
from metricflow.query.group_by_item.resolution_dag.dag import GroupByItemResolutionDag
from metricflow.query.group_by_item.resolution_dag.input_metric_location import InputMetricDefinitionLocation
from metricflow.query.group_by_item.resolution_dag.resolution_nodes.measure_source_node import (
    MeasureGroupByItemSourceNode,
)
from metricflow.query.group_by_item.resolution_dag.resolution_nodes.metric_resolution_node import (
    MetricGroupByItemResolutionNode,
)
from metricflow.query.group_by_item.resolution_dag.resolution_nodes.no_metrics_query_source_node import (
    NoMetricsGroupByItemSourceNode,
)
from metricflow.query.group_by_item.resolution_dag.resolution_nodes.query_resolution_node import (
    QueryGroupByItemResolutionNode,
)

logger = logging.getLogger(__name__)


class GroupByItemResolutionDagBuilder:
    """Builds a GroupByItemResolutionDag that can be used to resolve group-by-item specs from spec patterns."""

    def __init__(self, manifest_lookup: SemanticManifestLookup) -> None:  # noqa: D
        self._manifest_lookup = manifest_lookup

    def _build_dag_component_for_metric(
        self,
        metric_reference: MetricReference,
        metric_input_location: Optional[InputMetricDefinitionLocation],
    ) -> MetricGroupByItemResolutionNode:
        """Builds a DAG component that represents the resolution flow for a metric."""
        metric = self._manifest_lookup.metric_lookup.get_metric(metric_reference)

        # For a base metric, the parents are measure nodes
        if len(metric.input_metrics) == 0:
            measure_references_for_metric: Tuple[MeasureReference, ...]
            if metric.type is MetricType.CONVERSION:
                conversion_type_params = metric.type_params.conversion_type_params
                assert (
                    conversion_type_params
                ), "A conversion metric should have type_params.conversion_type_params defined."
                measure_references_for_metric = (conversion_type_params.base_measure.measure_reference,)
            else:
                measure_references_for_metric = tuple(
                    input_measure.measure_reference for input_measure in metric.input_measures
                )

            source_candidates_for_measure_nodes = tuple(
                MeasureGroupByItemSourceNode(
                    measure_reference=measure_reference,
                    child_metric_reference=metric_reference,
                )
                for measure_reference in measure_references_for_metric
            )
            return MetricGroupByItemResolutionNode(
                metric_reference=metric_reference,
                metric_input_location=metric_input_location,
                parent_nodes=source_candidates_for_measure_nodes,
            )
        # For a derived metric, the parents are other metrics.
        return MetricGroupByItemResolutionNode(
            metric_reference=metric_reference,
            metric_input_location=metric_input_location,
            parent_nodes=tuple(
                self._build_dag_component_for_metric(
                    metric_reference=metric_input.as_reference,
                    metric_input_location=InputMetricDefinitionLocation(
                        derived_metric_reference=metric_reference,
                        input_metric_list_index=metric_input_index,
                    ),
                )
                for metric_input_index, metric_input in enumerate(metric.input_metrics)
            ),
        )

    def _build_dag_component_for_query(
        self, metric_references: Sequence[MetricReference], where_filter_intersection: WhereFilterIntersection
    ) -> QueryGroupByItemResolutionNode:
        """Builds a DAG component that represents the resolution flow for a query."""
        if len(metric_references) == 0:
            return QueryGroupByItemResolutionNode(
                parent_nodes=(NoMetricsGroupByItemSourceNode(),),
                metrics_in_query=metric_references,
                where_filter_intersection=where_filter_intersection,
            )
        return QueryGroupByItemResolutionNode(
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
