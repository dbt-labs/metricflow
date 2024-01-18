from __future__ import annotations

from typing import List, Sequence, Tuple

from dbt_semantic_interfaces.enum_extension import assert_values_exhausted
from dbt_semantic_interfaces.naming.keywords import METRIC_TIME_ELEMENT_NAME
from dbt_semantic_interfaces.protocols import WhereFilterIntersection
from dbt_semantic_interfaces.references import EntityReference, MetricReference, TimeDimensionReference
from dbt_semantic_interfaces.type_enums import MetricType, TimeGranularity
from dbt_semantic_interfaces.type_enums.date_part import DatePart
from typing_extensions import override

from metricflow.model.semantic_manifest_lookup import SemanticManifestLookup
from metricflow.query.group_by_item.resolution_path import MetricFlowQueryResolutionPath
from metricflow.query.issues.issues_base import MetricFlowQueryResolutionIssueSet
from metricflow.query.issues.parsing.cumulative_metric_requires_metric_time import (
    CumulativeMetricRequiresMetricTimeIssue,
)
from metricflow.query.issues.parsing.offset_metric_requires_metric_time import OffsetMetricRequiresMetricTimeIssue
from metricflow.query.resolver_inputs.query_resolver_inputs import ResolverInputForQuery
from metricflow.query.validation_rules.base_validation_rule import PostResolutionQueryValidationRule
from metricflow.specs.specs import TimeDimensionSpec


class MetricTimeQueryValidationRule(PostResolutionQueryValidationRule):
    """Validates cases where a query requires metric_time to be specified as a group-by-item.

    Currently, known cases are:

    * Cumulative metrics.
    * Derived metrics with an offset time.g
    """

    def __init__(self, manifest_lookup: SemanticManifestLookup) -> None:  # noqa: D
        super().__init__(manifest_lookup=manifest_lookup)

        self._metric_time_specs = tuple(
            self._generate_valid_specs_for_time_dimension(
                time_dimension_reference=TimeDimensionReference(element_name=METRIC_TIME_ELEMENT_NAME), entity_links=()
            )
        )

    def _generate_valid_specs_for_time_dimension(
        self, time_dimension_reference: TimeDimensionReference, entity_links: Tuple[EntityReference, ...]
    ) -> List[TimeDimensionSpec]:
        time_dimension_specs: List[TimeDimensionSpec] = []
        for time_granularity in TimeGranularity:
            time_dimension_specs.append(
                TimeDimensionSpec(
                    element_name=time_dimension_reference.element_name,
                    entity_links=entity_links,
                    time_granularity=time_granularity,
                    date_part=None,
                )
            )
        for date_part in DatePart:
            for time_granularity in date_part.compatible_granularities:
                time_dimension_specs.append(
                    TimeDimensionSpec(
                        element_name=time_dimension_reference.element_name,
                        entity_links=entity_links,
                        time_granularity=time_granularity,
                        date_part=date_part,
                    )
                )
        return time_dimension_specs

    def _group_by_items_include_metric_time(self, query_resolver_input: ResolverInputForQuery) -> bool:
        for group_by_item_input in query_resolver_input.group_by_item_inputs:
            if group_by_item_input.spec_pattern.matches_any(self._metric_time_specs):
                return True

        return False

    def _group_by_items_include_agg_time_dimension(
        self, query_resolver_input: ResolverInputForQuery, metric_reference: MetricReference
    ) -> bool:
        metric = self._manifest_lookup.metric_lookup.get_metric(metric_reference=metric_reference)
        semantic_model_lookup = self._manifest_lookup.semantic_model_lookup

        valid_agg_time_dimension_specs: List[TimeDimensionSpec] = []
        for measure_reference in metric.measure_references:
            agg_time_dimension_reference = semantic_model_lookup.get_agg_time_dimension_for_measure(measure_reference)
            semantic_model = semantic_model_lookup.get_semantic_model_for_measure(measure_reference)
            assert semantic_model, f"No semantic model found for measure {measure_reference}."

            # is this too broad? need to narrow entity links?
            possible_entity_links = semantic_model_lookup.entity_links_for_local_elements(semantic_model)
            for entity_link in possible_entity_links:
                valid_agg_time_dimension_specs.extend(
                    self._generate_valid_specs_for_time_dimension(
                        time_dimension_reference=agg_time_dimension_reference, entity_links=(entity_link,)
                    )
                )
        print("valid:::")
        for x in valid_agg_time_dimension_specs:
            print(f"\n{x}")

        print("requested:::")
        for group_by_item_input in query_resolver_input.group_by_item_inputs:
            print(print(f"\n{group_by_item_input}"))
            if group_by_item_input.spec_pattern.matches_any(valid_agg_time_dimension_specs):
                return True

        return False

    @override
    def validate_metric_in_resolution_dag(
        self,
        metric_reference: MetricReference,
        resolver_input_for_query: ResolverInputForQuery,
        resolution_path: MetricFlowQueryResolutionPath,
    ) -> MetricFlowQueryResolutionIssueSet:
        metric = self._get_metric(metric_reference)
        query_includes_metric_time_or_agg_time_dimension = self._group_by_items_include_metric_time(
            resolver_input_for_query
        ) or self._group_by_items_include_agg_time_dimension(
            query_resolver_input=resolver_input_for_query, metric_reference=metric_reference
        )

        if metric.type is MetricType.SIMPLE or metric.type is MetricType.CONVERSION:
            return MetricFlowQueryResolutionIssueSet.empty_instance()
        elif metric.type is MetricType.CUMULATIVE:
            if (
                metric.type_params is not None
                and (metric.type_params.window is not None or metric.type_params.grain_to_date is not None)
                and not query_includes_metric_time_or_agg_time_dimension
            ):
                return MetricFlowQueryResolutionIssueSet.from_issue(
                    CumulativeMetricRequiresMetricTimeIssue.from_parameters(
                        metric_reference=metric_reference,
                        query_resolution_path=resolution_path,
                    )
                )
            return MetricFlowQueryResolutionIssueSet.empty_instance()

        elif metric.type is MetricType.RATIO or metric.type is MetricType.DERIVED:
            has_time_offset = any(
                input_metric.offset_window is not None or input_metric.offset_to_grain is not None
                for input_metric in metric.input_metrics
            )

            if has_time_offset and not query_includes_metric_time_or_agg_time_dimension:
                return MetricFlowQueryResolutionIssueSet.from_issue(
                    OffsetMetricRequiresMetricTimeIssue.from_parameters(
                        metric_reference=metric_reference,
                        input_metrics=metric.input_metrics,
                        query_resolution_path=resolution_path,
                    )
                )
            return MetricFlowQueryResolutionIssueSet.empty_instance()
        else:
            assert_values_exhausted(metric.type)

    @override
    def validate_query_in_resolution_dag(
        self,
        metrics_in_query: Sequence[MetricReference],
        where_filter_intersection: WhereFilterIntersection,
        resolver_input_for_query: ResolverInputForQuery,
        resolution_path: MetricFlowQueryResolutionPath,
    ) -> MetricFlowQueryResolutionIssueSet:
        return MetricFlowQueryResolutionIssueSet.empty_instance()
