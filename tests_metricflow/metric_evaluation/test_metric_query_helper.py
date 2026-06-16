from __future__ import annotations

import logging

from metricflow_semantics.model.semantic_manifest_lookup import SemanticManifestLookup
from metricflow_semantics.model.semantics.linkable_element import LinkableElementType
from metricflow_semantics.model.semantics.linkable_element_set_base import AnnotatedSpec
from metricflow_semantics.semantic_graph.attribute_resolution.group_by_item_set import GroupByItemSet
from metricflow_semantics.specs.dimension_spec import DimensionSpec
from metricflow_semantics.specs.time_dimension_spec import TimeDimensionSpec
from metricflow_semantics.specs.where_filter.where_filter_spec import WhereFilterSpec
from metricflow_semantics.sql.sql_bind_parameters import SqlBindParameterSet
from metricflow_semantics.time.granularity import ExpandedTimeGranularity

from metricflow.metric_evaluation.metric_query_helper import MetricQueryHelper
from metricflow_semantic_interfaces.references import EntityReference, MetricReference
from metricflow_semantic_interfaces.type_enums import TimeGranularity

logger = logging.getLogger(__name__)


def test_split_filters_by_aggregation_time_dimension_references(
    simple_semantic_manifest_lookup: SemanticManifestLookup,
) -> None:
    """Filters with no linkable specs should stay out of the time-spine-only bucket."""
    agg_time_dimension_specs = simple_semantic_manifest_lookup.metric_lookup.get_aggregation_time_dimension_specs(
        MetricReference("bookings")
    )
    agg_time_dimension_spec = next(iter(agg_time_dimension_specs))
    dimension_spec = DimensionSpec(
        element_name="country_latest",
        entity_links=(EntityReference("listing"),),
    )
    helper = MetricQueryHelper(metric_lookup=simple_semantic_manifest_lookup.metric_lookup)

    agg_time_filter = _create_where_filter_spec(_create_time_dimension_annotated_spec(agg_time_dimension_spec))
    mixed_filter = _create_where_filter_spec(
        _create_time_dimension_annotated_spec(agg_time_dimension_spec),
        _create_dimension_annotated_spec(dimension_spec),
    )
    non_agg_time_filter = _create_where_filter_spec(_create_dimension_annotated_spec(dimension_spec))
    filter_without_linkable_specs = _create_where_filter_spec()

    filter_split = helper.split_filters_by_aggregation_time_dimension_references(
        metric_reference=MetricReference("bookings"),
        filter_specs=(agg_time_filter, mixed_filter, non_agg_time_filter, filter_without_linkable_specs),
    )

    assert set(filter_split.filters_with_only_agg_time_dimension_references) == {agg_time_filter}
    assert set(filter_split.filters_with_mixed_references) == {mixed_filter}
    assert set(filter_split.filters_without_agg_time_dimension_references) == {
        non_agg_time_filter,
        filter_without_linkable_specs,
    }


def test_resolve_group_by_specs_for_time_offset_metric_input(
    simple_semantic_manifest_lookup: SemanticManifestLookup,
) -> None:
    """Filter-only specs and base grain for custom time dimensions should be included."""
    queried_group_by_spec = DimensionSpec(
        element_name="country_latest",
        entity_links=(EntityReference("listing"),),
    )
    filter_only_group_by_spec = DimensionSpec(
        element_name="capacity_latest",
        entity_links=(EntityReference("listing"),),
    )
    custom_grain_time_dimension_spec = TimeDimensionSpec(
        element_name="metric_time",
        entity_links=(),
        time_granularity=ExpandedTimeGranularity(name="alien_day", base_granularity=TimeGranularity.DAY),
    )
    helper = MetricQueryHelper(metric_lookup=simple_semantic_manifest_lookup.metric_lookup)

    required_group_by_specs = helper.resolve_group_by_specs_for_time_offset_metric_input(
        queried_group_by_specs=(queried_group_by_spec,),
        filter_specs=(
            _create_where_filter_spec(
                _create_time_dimension_annotated_spec(custom_grain_time_dimension_spec),
                _create_dimension_annotated_spec(filter_only_group_by_spec),
            ),
        ),
    )

    assert set(required_group_by_specs) == {
        queried_group_by_spec,
        custom_grain_time_dimension_spec,
        filter_only_group_by_spec,
        custom_grain_time_dimension_spec.with_base_grain(),
    }


def _create_where_filter_spec(*annotated_specs: AnnotatedSpec) -> WhereFilterSpec:
    return WhereFilterSpec(
        where_sql="where_sql",
        bind_parameters=SqlBindParameterSet(),
        element_set=GroupByItemSet.create(*annotated_specs),
    )


def _create_time_dimension_annotated_spec(time_dimension_spec: TimeDimensionSpec) -> AnnotatedSpec:
    return AnnotatedSpec.create(
        element_type=LinkableElementType.TIME_DIMENSION,
        element_name=time_dimension_spec.element_name,
        properties=(),
        origin_model_ids=(),
        derived_from_semantic_models=(),
        entity_links=time_dimension_spec.entity_links,
        metric_subquery_entity_links=None,
        time_grain=time_dimension_spec.time_granularity,
        date_part=time_dimension_spec.date_part,
    )


def _create_dimension_annotated_spec(dimension_spec: DimensionSpec) -> AnnotatedSpec:
    return AnnotatedSpec.create(
        element_type=LinkableElementType.DIMENSION,
        element_name=dimension_spec.element_name,
        properties=(),
        origin_model_ids=(),
        derived_from_semantic_models=(),
        entity_links=dimension_spec.entity_links,
        metric_subquery_entity_links=None,
        time_grain=None,
        date_part=None,
    )
