from __future__ import annotations

import logging
from collections import defaultdict
from collections.abc import Iterable, Mapping, Sequence
from typing import Optional

from dbt_semantic_interfaces.enum_extension import assert_values_exhausted
from dbt_semantic_interfaces.type_enums import MetricType, TimeGranularity
from metricflow_semantics.semantic_graph.lookups.manifest_object_lookup import ManifestObjectLookup
from metricflow_semantics.semantic_graph.model_id import SemanticModelId
from metricflow_semantics.specs.time_window import TimeWindow
from metricflow_semantics.specs.where_filter.where_filter_spec import WhereFilterSpec
from metricflow_semantics.toolkit.dataclass_helpers import fast_frozen_dataclass
from metricflow_semantics.toolkit.mf_logging.pretty_formatter import PrettyFormatContext
from metricflow_semantics.toolkit.mf_type_aliases import AnyLengthTuple
from typing_extensions import override

from metricflow.metric_evaluation.plan.query_element import MetricQueryElement, MetricQueryPropertySet

logger = logging.getLogger(__name__)


@fast_frozen_dataclass()
class SimpleMetricQueryPropertySet(MetricQueryPropertySet):
    """Properties that can be used to group queries for simple metrics."""

    model_id: SemanticModelId
    filters_from_metric_definition: AnyLengthTuple[str]
    filters_from_metric_spec: AnyLengthTuple[WhereFilterSpec]
    offset_window_from_metric_spec: Optional[TimeWindow]
    offset_to_grain_from_metric_spec: Optional[TimeGranularity]

    @override
    def pretty_format(self, format_context: PrettyFormatContext) -> Optional[str]:
        return format_context.formatter.pretty_format_object_by_parts(
            class_name=self.__class__.__name__,
            field_mapping={
                "model": self.model_id.model_name,
                "filters_from_metric_definition": self.filters_from_metric_definition,
                "filters_from_metric_spec": self.filters_from_metric_spec,
                "group_by_item_specs": [spec.dunder_name for spec in self.group_by_item_specs],
                "pushdown_enabled_types": self.predicate_pushdown_state.pushdown_enabled_types,
            },
        )


@fast_frozen_dataclass()
class QueryElementGroupingResult:
    """The result of grouping query elements for composing metric queries."""

    property_set_to_simple_metric_query_elements: Mapping[SimpleMetricQueryPropertySet, Sequence[MetricQueryElement]]
    non_grouped_query_elements: AnyLengthTuple[MetricQueryElement]


class QueryElementGrouper:
    """Groups query elements to use for generating metric queries that computes multiple metrics."""

    def __init__(self, manifest_object_lookup: ManifestObjectLookup) -> None:  # noqa: D107
        self._manifest_object_lookup = manifest_object_lookup

    def group_query_elements(  # noqa: D102
        self, query_elements: Iterable[MetricQueryElement]
    ) -> QueryElementGroupingResult:
        property_set_to_simple_metric_query_elements: defaultdict[
            SimpleMetricQueryPropertySet, list[MetricQueryElement]
        ] = defaultdict(list)
        non_grouped_query_elements: list[MetricQueryElement] = []

        for query_element in query_elements:
            metric_name = query_element.metric_name
            metric = self._manifest_object_lookup.get_metric(metric_name)
            metric_type = metric.type

            if metric_type is MetricType.SIMPLE:
                model_id = self._manifest_object_lookup.simple_metric_name_to_input[metric_name].model_id
                metric_spec = query_element.metric_spec

                if metric_spec.alias is not None:
                    # For cases where the simple metric has an alias, don't group to simplify reasoning for alias
                    # collisions.
                    non_grouped_query_elements.append(query_element)
                    continue

                property_set = SimpleMetricQueryPropertySet(
                    model_id=model_id,
                    filters_from_metric_definition=tuple(
                        (
                            (metric_filter.where_sql_template for metric_filter in metric.filter.where_filters)
                            if metric.filter is not None
                            else ()
                        )
                    ),
                    filters_from_metric_spec=metric_spec.where_filter_specs,
                    offset_window_from_metric_spec=metric_spec.offset_window,
                    offset_to_grain_from_metric_spec=metric_spec.offset_to_grain,
                    group_by_item_specs=query_element.group_by_item_specs.as_frozen(),
                    predicate_pushdown_state=query_element.predicate_pushdown_state,
                )
                property_set_to_simple_metric_query_elements[property_set].append(query_element)

            elif (
                metric_type is MetricType.CUMULATIVE
                or metric_type is MetricType.CONVERSION
                or metric_type is MetricType.RATIO
                or metric_type is MetricType.DERIVED
            ):
                non_grouped_query_elements.append(query_element)
            else:
                assert_values_exhausted(metric_type)
        return QueryElementGroupingResult(
            property_set_to_simple_metric_query_elements=property_set_to_simple_metric_query_elements,
            non_grouped_query_elements=tuple(non_grouped_query_elements),
        )
