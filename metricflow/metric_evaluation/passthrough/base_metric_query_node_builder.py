from __future__ import annotations

import logging
from collections import defaultdict
from collections.abc import Iterable, Sequence
from typing import Optional

from dbt_semantic_interfaces.enum_extension import assert_values_exhausted
from dbt_semantic_interfaces.type_enums import MetricType, TimeGranularity
from metricflow_semantics.errors.error_classes import MetricFlowInternalError
from metricflow_semantics.semantic_graph.lookups.manifest_object_lookup import ManifestObjectLookup
from metricflow_semantics.semantic_graph.model_id import SemanticModelId
from metricflow_semantics.specs.metric_spec import MetricSpec
from metricflow_semantics.specs.time_window import TimeWindow
from metricflow_semantics.specs.where_filter.where_filter_spec import WhereFilterSpec
from metricflow_semantics.toolkit.dataclass_helpers import fast_frozen_dataclass
from metricflow_semantics.toolkit.mf_logging.lazy_formattable import LazyFormat
from metricflow_semantics.toolkit.mf_type_aliases import AnyLengthTuple

from metricflow.metric_evaluation.plan.me_nodes import (
    ConversionMetricQueryNode,
    CumulativeMetricQueryNode,
    MetricQueryNode,
    SimpleMetricsQueryNode,
)
from metricflow.metric_evaluation.plan.query_element import MetricQueryElement, MetricQueryPropertySet

logger = logging.getLogger(__name__)


class BaseMetricQueryNodeBuilder:
    """Builds `MetricQueryNode` for query elements corresponding to non-derived metrics.

    This creates the corresponding `MetricQueryNode` subclass instances appropriate for the metric type in
    the query element e.g. create a `SimpleMetricsQueryNode` for simple metrics, a `CumulativeMetricQueryNode` for
    cumulative metrics, etc.

    However, query elements for a simple metric with a shared semantic model should be grouped into a single
    `SimpleMetricsQueryNode` when:

    * The query element's metric spec does not specify an alias.
    * The query properties / filters are the same.
    * The time-offsets are the same.

    Consolidating to a single query node reduces the number of full-outer joins required in the final query.

    Otherwise, separate nodes are created. Since each node is eventually converted into a SQL query that reads from a
    semantic model, these requirements reflect SQL semantics (for example, different `WHERE` clauses must be emitted
    as separate queries).
    """

    def __init__(self, manifest_object_lookup: ManifestObjectLookup) -> None:  # noqa: D107
        self._manifest_object_lookup = manifest_object_lookup

    def build_nodes(self, query_elements: Iterable[MetricQueryElement]) -> Sequence[MetricQueryNode]:
        """Build metric query nodes for the provided base metric query elements.

        Query elements are grouped when they can be evaluated by the same underlying query node. Grouping is
        done in a way that follows SQL behavior.
        """
        nodes: list[MetricQueryNode] = []
        group_key_to_metric_specs: defaultdict[BaseMetricsQueryElementGroupKey, list[MetricSpec]] = defaultdict(list)

        for query_element in query_elements:
            group_key = self._build_group_key(query_element=query_element)
            group_key_to_metric_specs[group_key].append(query_element.metric_spec)

        for group_key, grouped_metric_specs in group_key_to_metric_specs.items():
            metric_type = group_key.metric_type
            query_properties = group_key.query_properties
            if metric_type is MetricType.SIMPLE:
                if group_key.model_id is None:
                    raise MetricFlowInternalError(
                        LazyFormat(
                            "Expected the group key for a simple metric to contain a model ID", group_key=group_key
                        )
                    )

                nodes.append(
                    SimpleMetricsQueryNode.create(
                        model_id=group_key.model_id,
                        metric_specs=grouped_metric_specs,
                        query_properties=query_properties,
                    )
                )
            elif metric_type is MetricType.CUMULATIVE:
                nodes.append(
                    CumulativeMetricQueryNode.create(
                        metric_spec=self._single_metric_spec_for_group(
                            metric_specs=grouped_metric_specs, metric_type=metric_type
                        ),
                        query_properties=query_properties,
                    )
                )
            elif metric_type is MetricType.CONVERSION:
                nodes.append(
                    ConversionMetricQueryNode.create(
                        metric_spec=self._single_metric_spec_for_group(
                            metric_specs=grouped_metric_specs, metric_type=metric_type
                        ),
                        query_properties=query_properties,
                    )
                )
            elif metric_type is MetricType.RATIO or metric_type is MetricType.DERIVED:
                raise MetricFlowInternalError(
                    LazyFormat(
                        "Only base metrics should have been provided to this method",
                        group_key=group_key,
                        metric_specs=grouped_metric_specs,
                    )
                )
            else:
                assert_values_exhausted(metric_type)

        return nodes

    def _build_group_key(self, query_element: MetricQueryElement) -> BaseMetricsQueryElementGroupKey:
        """Construct a grouping key for a metric query element."""
        metric_name = query_element.metric_name
        metric_definition = self._manifest_object_lookup.get_metric(metric_name)
        metric_query_spec = query_element.metric_spec
        metric_type = metric_definition.type

        # Set model ID for simple metrics.
        model_id: Optional[SemanticModelId] = None
        simple_metric_input = self._manifest_object_lookup.simple_metric_name_to_input.get(metric_name)
        if simple_metric_input is not None:
            model_id = simple_metric_input.model_id

        # Set the filters defined in the metric.
        filters_from_metric_definition: AnyLengthTuple[str] = ()
        metric_filter = self._manifest_object_lookup.get_metric(metric_name).filter
        if metric_filter is not None:
            filters_from_metric_definition = tuple(
                metric_filter_spec.where_sql_template for metric_filter_spec in metric_filter.where_filters
            )

        return BaseMetricsQueryElementGroupKey(
            model_id=model_id,
            metric_type=metric_type,
            non_simple_metric_spec=metric_query_spec if metric_type is not MetricType.SIMPLE else None,
            aliased_metric_spec=metric_query_spec if metric_query_spec.alias is not None else None,
            filters_from_metric_definition=filters_from_metric_definition,
            filters_from_metric_spec=metric_query_spec.where_filter_specs,
            offset_window_from_metric_spec=metric_query_spec.offset_window,
            offset_to_grain_from_metric_spec=metric_query_spec.offset_to_grain,
            query_properties=query_element.query_properties,
        )

    @staticmethod
    def _single_metric_spec_for_group(metric_specs: Sequence[MetricSpec], metric_type: MetricType) -> MetricSpec:
        """Return the single metric spec in a group for non-groupable metric types."""
        if len(metric_specs) != 1:
            raise MetricFlowInternalError(
                LazyFormat(
                    "Expected exactly 1 metric spec for a non-groupable metric type",
                    metric_type=metric_type,
                    metric_specs=metric_specs,
                )
            )

        return metric_specs[0]


@fast_frozen_dataclass()
class BaseMetricsQueryElementGroupKey:
    """Grouping key for combining compatible base metric query elements.

    The fields encode all properties that must match for elements to share a query node.
    """

    # Ensures that metrics with an alias are each put in a separate group.
    aliased_metric_spec: Optional[MetricSpec]
    # `non_simple_metric_spec` and `model_id` together ensures that only simple metrics with the same model can be put
    # into the same group. Each element of other types will be put into a separate group.
    # `non_simple_metric_spec` is only set for non-simple metrics.
    # `model_id` is only set for simple metrics.
    non_simple_metric_spec: Optional[MetricSpec]
    model_id: Optional[SemanticModelId]
    # Fields below ensure that elements with filters and other properties are the same in each group.
    filters_from_metric_definition: AnyLengthTuple[str]
    filters_from_metric_spec: AnyLengthTuple[WhereFilterSpec]
    offset_window_from_metric_spec: Optional[TimeWindow]
    offset_to_grain_from_metric_spec: Optional[TimeGranularity]
    query_properties: MetricQueryPropertySet

    # Not needed as `non_simple_metric_spec` will prevent grouping of simple metrics with non-simple metrics, but
    # saves a lookup.
    metric_type: MetricType
