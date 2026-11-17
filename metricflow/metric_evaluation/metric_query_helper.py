from __future__ import annotations

import itertools
import logging
from collections.abc import Iterable, Sequence

from metricflow_semantics.errors.error_classes import (
    ExcludeDimensionsWithTimeOffsetError,
    InvalidExcludedFilterError,
    UnmatchedExcludeDimensionsError,
)
from metricflow_semantics.model.semantics.excluded_dimensions import (
    is_declared_excluded_annotated_dimension,
    is_declared_excluded_dimension,
)
from metricflow_semantics.model.semantics.linkable_element import LinkableElementType
from metricflow_semantics.model.semantics.metric_lookup import MetricLookup
from metricflow_semantics.specs.instance_spec import LinkableInstanceSpec
from metricflow_semantics.specs.linkable_spec_set import LinkableSpecSet
from metricflow_semantics.specs.metric_spec import MetricSpec
from metricflow_semantics.specs.where_filter.where_filter_spec import WhereFilterSpec
from metricflow_semantics.toolkit.collections.ordered_set import MutableOrderedSet, OrderedSet
from metricflow_semantics.toolkit.dataclass_helpers import fast_frozen_dataclass
from metricflow_semantics.toolkit.mf_type_aliases import AnyLengthTuple

from metricflow.metric_evaluation.plan.query_element import MetricQueryElement
from metricflow.metric_evaluation.where_filter_relaxer import relax_filter_spec_for_excluded_dimensions
from metricflow.plan_conversion.node_processor import PredicatePushdownState
from metricflow_semantic_interfaces.references import MetricReference

logger = logging.getLogger(__name__)


class MetricQueryHelper:
    """Helper methods for metric queries.

    This is a WIP as additional consolidation / restructuring is needed.
    """

    def __init__(self, metric_lookup: MetricLookup) -> None:  # noqa: D107
        self._metric_lookup = metric_lookup

    def resolve_group_by_specs_for_time_offset_metric_input(
        self,
        queried_group_by_specs: Iterable[LinkableInstanceSpec],
        filter_specs: Iterable[WhereFilterSpec],
    ) -> OrderedSet[LinkableInstanceSpec]:
        """Helper to get the required group-by items for inputs to a time offset metric.

        The required group-by items may be a superset of the queried group-by items because:

        * A filter may contain group-by items that are not in the query, so we must generate a query that includes
        intersection of group-by items in the query and referenced in the filters, filter the result, then re-aggregate
        to only the group-by items in the query.

        * If there are any references to a time dimension at a custom time grain, we need to get the time dimension at
        the custom grain and then map it to the custom grain.
        """
        group_by_specs_referenced_in_filters: MutableOrderedSet[LinkableInstanceSpec] = MutableOrderedSet(
            itertools.chain.from_iterable(filter_spec.linkable_specs for filter_spec in filter_specs)
        )

        required_group_by_specs: MutableOrderedSet[LinkableInstanceSpec] = MutableOrderedSet(
            itertools.chain(
                queried_group_by_specs,
                group_by_specs_referenced_in_filters,
            )
        )
        required_time_dimension_specs_with_custom_grain = tuple(
            spec
            for spec in LinkableSpecSet.create_from_specs(required_group_by_specs).time_dimension_specs
            if spec.has_custom_grain
        )

        # Custom grains require joining to their base grain, so add base grain to extraneous specs.
        if len(required_time_dimension_specs_with_custom_grain) > 0:
            required_group_by_specs.update(
                spec.with_base_grain() for spec in required_time_dimension_specs_with_custom_grain
            )

        return required_group_by_specs

    @staticmethod
    def resolve_group_by_specs_for_input_metric(
        queried_group_by_specs: Iterable[LinkableInstanceSpec],
        exclude_dimensions: Iterable[str],
    ) -> OrderedSet[LinkableInstanceSpec]:
        """Return the queried group-by specs without the dimensions that the input metric excludes."""
        excluded_dimension_names = frozenset(exclude_dimensions)
        queried_group_by_specs = tuple(queried_group_by_specs)
        excluded_dimension_specs = frozenset(
            spec
            for spec in LinkableSpecSet.create_from_specs(queried_group_by_specs).dimension_specs
            if is_declared_excluded_dimension(spec, excluded_dimension_names)
        )
        return MutableOrderedSet(spec for spec in queried_group_by_specs if spec not in excluded_dimension_specs)

    @staticmethod
    def is_any_excluded_dimension_queried(
        queried_group_by_specs: Iterable[LinkableInstanceSpec],
        exclude_dimensions: Iterable[str],
    ) -> bool:
        """Return whether a query group-bys include a dimension that the input metric excludes."""
        excluded_dimension_names = frozenset(exclude_dimensions)
        if len(excluded_dimension_names) == 0:
            return False
        return any(
            is_declared_excluded_dimension(spec, excluded_dimension_names)
            for spec in LinkableSpecSet.create_from_specs(queried_group_by_specs).dimension_specs
        )

    @staticmethod
    def is_any_excluded_dimension_used(
        queried_group_by_specs: Iterable[LinkableInstanceSpec],
        filter_specs: Iterable[WhereFilterSpec],
        exclude_dimensions: Iterable[str],
    ) -> bool:
        """Return whether a query group-bys or filters reference a dimension that the input metric excludes."""
        excluded_dimension_names = frozenset(exclude_dimensions)
        if len(excluded_dimension_names) == 0:
            return False
        return any(
            is_declared_excluded_dimension(spec, excluded_dimension_names)
            for spec in itertools.chain(
                LinkableSpecSet.create_from_specs(queried_group_by_specs).dimension_specs,
                itertools.chain.from_iterable(
                    filter_spec.linkable_spec_set.dimension_specs for filter_spec in filter_specs
                ),
            )
        )

    @staticmethod
    def select_filter_specs_for_input_metric(
        metric_name: str,
        input_metric_name: str,
        filter_specs: Iterable[WhereFilterSpec],
        exclude_dimensions: Iterable[str],
    ) -> Sequence[WhereFilterSpec]:
        """Return the filters that apply to an input metric that excludes the given dimensions.

        Filters that reference only excluded dimensions don't apply to the input metric. In a filter that references
        other group-by items too, the predicates on excluded dimensions are relaxed (see
        `relax_filter_spec_for_excluded_dimensions`).
        """
        excluded_dimension_names = frozenset(exclude_dimensions)
        if len(excluded_dimension_names) == 0:
            return tuple(filter_specs)

        selected_filter_specs: list[WhereFilterSpec] = []
        for filter_spec in filter_specs:
            referenced_excluded_dimension_count = sum(
                1
                for spec in filter_spec.linkable_spec_set.dimension_specs
                if is_declared_excluded_dimension(spec, excluded_dimension_names)
            )
            if referenced_excluded_dimension_count == 0:
                selected_filter_specs.append(filter_spec)
            elif referenced_excluded_dimension_count < len(filter_spec.linkable_specs):
                try:
                    relaxed_filter_spec = relax_filter_spec_for_excluded_dimensions(
                        filter_spec, excluded_dimension_names
                    )
                except InvalidExcludedFilterError as e:
                    raise InvalidExcludedFilterError(
                        f"Can't apply a filter to input metric {input_metric_name!r} of metric {metric_name!r},"
                        f" which excludes dimensions via `exclude_dimensions`: {e}"
                    ) from e
                if relaxed_filter_spec is not None:
                    selected_filter_specs.append(relaxed_filter_spec)
        return tuple(selected_filter_specs)

    def error_if_declared_dimension_matches_no_group_by_item(  # noqa: D102
        self, metric_name: str, input_metric_specs: Sequence[MetricSpec]
    ) -> None:
        if all(len(input_metric_spec.exclude_dimensions) == 0 for input_metric_spec in input_metric_specs):
            return
        reachable_annotated_specs = tuple(
            annotated_spec
            for input_metric_spec in input_metric_specs
            for annotated_spec in self._metric_lookup.get_common_group_by_items(
                metric_references=(input_metric_spec.reference,)
            ).annotated_specs
        )
        for input_metric_spec in input_metric_specs:
            for declared_name in input_metric_spec.exclude_dimensions:
                if any(
                    is_declared_excluded_annotated_dimension(annotated_spec, (declared_name,))
                    for annotated_spec in reachable_annotated_specs
                ):
                    continue
                reachable_dimension_names = sorted(
                    {
                        annotated_spec.spec.dunder_name
                        for annotated_spec in reachable_annotated_specs
                        if annotated_spec.element_type is LinkableElementType.DIMENSION
                    }
                )
                raise UnmatchedExcludeDimensionsError(
                    f"Input metric {input_metric_spec.element_name!r} of metric {metric_name!r} declares"
                    f" {declared_name!r} in `exclude_dimensions`, but it matches no dimension that the inputs of"
                    f" {metric_name!r} can be grouped by. Examples of dimensions that can be used:"
                    f" {reachable_dimension_names[:5]}"
                )

    def create_input_query_element(  # noqa: D102
        self,
        metric_spec: MetricSpec,
        input_metric_spec: MetricSpec,
        group_by_item_specs: Iterable[LinkableInstanceSpec],
        predicate_pushdown_state: PredicatePushdownState,
    ) -> MetricQueryElement:
        if len(input_metric_spec.exclude_dimensions) > 0 and (
            metric_spec.has_time_offset or input_metric_spec.has_time_offset
        ):
            raise ExcludeDimensionsWithTimeOffsetError(
                f"Input metric {input_metric_spec.element_name!r} of metric {metric_spec.element_name!r} declares"
                " `exclude_dimensions`, which is not supported together with a time offset on the input metric or"
                " on the metric."
            )
        return MetricQueryElement.create(
            metric_spec=input_metric_spec,
            group_by_item_specs=self.resolve_group_by_specs_for_input_metric(
                queried_group_by_specs=group_by_item_specs,
                exclude_dimensions=input_metric_spec.exclude_dimensions,
            ),
            predicate_pushdown_state=self.resolve_predicate_pushdown_state_for_input_metric(
                metric_name=metric_spec.element_name,
                input_metric_name=input_metric_spec.element_name,
                predicate_pushdown_state=predicate_pushdown_state,
                exclude_dimensions=input_metric_spec.exclude_dimensions,
            ),
        )

    def resolve_predicate_pushdown_state_for_input_metric(
        self,
        metric_name: str,
        input_metric_name: str,
        predicate_pushdown_state: PredicatePushdownState,
        exclude_dimensions: Iterable[str],
    ) -> PredicatePushdownState:
        """Return the pushdown state for an input metric, without filters that don't apply to it."""
        if len(predicate_pushdown_state.where_filter_specs) == 0:
            return predicate_pushdown_state
        return PredicatePushdownState.with_where_filter_specs(
            original_pushdown_state=predicate_pushdown_state,
            where_filter_specs=self.select_filter_specs_for_input_metric(
                metric_name=metric_name,
                input_metric_name=input_metric_name,
                filter_specs=predicate_pushdown_state.where_filter_specs,
                exclude_dimensions=exclude_dimensions,
            ),
        )

    def split_filters_by_aggregation_time_dimension_references(
        self, metric_reference: MetricReference, filter_specs: Iterable[WhereFilterSpec]
    ) -> AggregationTimeDimensionFilterSplit:
        """Split filters by whether they reference aggregation time dimensions.

        The main use case is to figure out how different filters should be applied when a time-spine join is used for
        a time-offset metric.
        """
        agg_time_dimension_specs: OrderedSet[
            LinkableInstanceSpec
        ] = self._metric_lookup.get_aggregation_time_dimension_specs(metric_reference)

        filters_with_mixed_references: list[WhereFilterSpec] = []
        filters_with_only_agg_time_dimension_references: list[WhereFilterSpec] = []
        filters_without_agg_time_dimension_references: list[WhereFilterSpec] = []

        for filter_spec in filter_specs:
            agg_time_dimension_specs_in_filter = agg_time_dimension_specs.intersection(filter_spec.linkable_specs)

            if agg_time_dimension_specs_in_filter:
                if len(agg_time_dimension_specs_in_filter) == len(filter_spec.linkable_specs):
                    filters_with_only_agg_time_dimension_references.append(filter_spec)
                else:
                    filters_with_mixed_references.append(filter_spec)
            else:
                filters_without_agg_time_dimension_references.append(filter_spec)

        return AggregationTimeDimensionFilterSplit(
            filters_with_mixed_references=tuple(filters_with_mixed_references),
            filters_without_agg_time_dimension_references=tuple(filters_without_agg_time_dimension_references),
            filters_with_only_agg_time_dimension_references=tuple(filters_with_only_agg_time_dimension_references),
        )


@fast_frozen_dataclass()
class AggregationTimeDimensionFilterSplit:
    """Split based on how aggregation time dimension references appear in the filter."""

    # Filters that reference only aggregation time dimensions.
    filters_with_only_agg_time_dimension_references: AnyLengthTuple[WhereFilterSpec]
    # Filters that reference both aggregation time dimensions and other linkable specs.
    filters_with_mixed_references: AnyLengthTuple[WhereFilterSpec]
    # Filters that do not reference aggregation time dimensions, including filters with no linkable specs.
    filters_without_agg_time_dimension_references: AnyLengthTuple[WhereFilterSpec]
