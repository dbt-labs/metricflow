from __future__ import annotations

import logging

import pytest
from dbt_semantic_interfaces.call_parameter_sets import (
    DimensionCallParameterSet,
    EntityCallParameterSet,
    MetricCallParameterSet,
    TimeDimensionCallParameterSet,
)
from dbt_semantic_interfaces.implementations.filters.where_filter import (
    PydanticWhereFilter,
    PydanticWhereFilterIntersection,
)
from dbt_semantic_interfaces.naming.keywords import METRIC_TIME_ELEMENT_NAME
from dbt_semantic_interfaces.protocols import WhereFilterIntersection
from dbt_semantic_interfaces.references import (
    DimensionReference,
    EntityReference,
    MetricReference,
    SemanticModelReference,
    TimeDimensionReference,
)
from dbt_semantic_interfaces.type_enums.date_part import DatePart
from dbt_semantic_interfaces.type_enums.time_granularity import TimeGranularity
from metricflow_semantics.model.linkable_element_property import GroupByItemProperty
from metricflow_semantics.model.semantic_manifest_lookup import SemanticManifestLookup
from metricflow_semantics.model.semantics.linkable_element import LinkableElementType
from metricflow_semantics.model.semantics.linkable_element_set_base import AnnotatedSpec, BaseGroupByItemSet
from metricflow_semantics.naming.object_builder_scheme import ObjectBuilderNamingScheme
from metricflow_semantics.query.group_by_item.filter_spec_resolution.filter_location import WhereFilterLocation
from metricflow_semantics.query.group_by_item.filter_spec_resolution.filter_spec_lookup import (
    CallParameterSet,
    FilterSpecResolution,
    FilterSpecResolutionLookUp,
    ResolvedSpecLookUpKey,
)
from metricflow_semantics.query.group_by_item.resolution_path import MetricFlowQueryResolutionPath
from metricflow_semantics.query.issues.issues_base import MetricFlowQueryResolutionIssueSet
from metricflow_semantics.semantic_graph.attribute_resolution.group_by_item_set import (
    GroupByItemSet,
)
from metricflow_semantics.semantic_graph.model_id import SemanticModelId
from metricflow_semantics.specs.column_assoc import ColumnAssociationResolver
from metricflow_semantics.specs.dimension_spec import DimensionSpec
from metricflow_semantics.specs.entity_spec import EntitySpec
from metricflow_semantics.specs.group_by_metric_spec import GroupByMetricSpec
from metricflow_semantics.specs.instance_spec import LinkableInstanceSpec
from metricflow_semantics.specs.linkable_spec_set import LinkableSpecSet
from metricflow_semantics.specs.time_dimension_spec import TimeDimensionSpec
from metricflow_semantics.specs.where_filter.where_filter_spec import WhereFilterSpec
from metricflow_semantics.specs.where_filter.where_filter_transform import WhereSpecFactory
from metricflow_semantics.time.granularity import ExpandedTimeGranularity

from tests_metricflow_semantics.specs.conftest import EXAMPLE_FILTER_LOCATION

logger = logging.getLogger(__name__)


def create_spec_lookup(
    call_parameter_set: CallParameterSet,
    resolved_spec: LinkableInstanceSpec,
    resolved_linkable_element_set: BaseGroupByItemSet,
    semantic_manifest_lookup: SemanticManifestLookup,
) -> FilterSpecResolutionLookUp:
    """Create a FilterSpecResolutionLookUp where the call_parameter_set maps to resolved_spec."""
    return FilterSpecResolutionLookUp(
        spec_resolutions=(
            FilterSpecResolution(
                lookup_key=ResolvedSpecLookUpKey(
                    filter_location=EXAMPLE_FILTER_LOCATION,
                    call_parameter_set=call_parameter_set,
                ),
                filter_location_path=MetricFlowQueryResolutionPath.empty_instance(),
                where_filter_intersection=create_where_filter_intersection("Dimension('dummy__dimension')"),
                resolved_group_by_item_set=resolved_linkable_element_set,
                issue_set=MetricFlowQueryResolutionIssueSet.empty_instance(),
                spec_pattern=ObjectBuilderNamingScheme().spec_pattern(
                    "Dimension('dummy__dimension')",
                    semantic_manifest_lookup=semantic_manifest_lookup,
                ),
                object_builder_str="Dimension('dummy__dimension')",
            ),
        ),
        non_parsable_resolutions=(),
    )


def create_where_filter_intersection(sql_template: str) -> WhereFilterIntersection:
    """Create a WhereFilterIntersection with 1 filter that has the given sql_template."""
    return PydanticWhereFilterIntersection(where_filters=[PydanticWhereFilter(where_sql_template=sql_template)])


def test_dimension_in_filter(  # noqa: D103
    column_association_resolver: ColumnAssociationResolver,
    simple_semantic_manifest_lookup: SemanticManifestLookup,
) -> None:
    where_filter_specs = WhereSpecFactory(
        column_association_resolver=column_association_resolver,
        spec_resolution_lookup=create_spec_lookup(
            call_parameter_set=DimensionCallParameterSet(
                entity_path=(EntityReference("listing"),),
                dimension_reference=DimensionReference("country_latest"),
            ),
            resolved_spec=DimensionSpec(element_name="country_latest", entity_links=(EntityReference("listing"),)),
            resolved_linkable_element_set=GroupByItemSet.create(
                AnnotatedSpec.create(
                    element_type=LinkableElementType.DIMENSION,
                    element_name="country_latest",
                    entity_links=(EntityReference("listing"),),
                    time_grain=None,
                    date_part=None,
                    metric_subquery_entity_links=None,
                    properties=frozenset(),
                    origin_model_ids=[SemanticModelId.get_instance("bookings")],
                    derived_from_semantic_models=[SemanticModelReference("bookings_source")],
                ),
            ),
            semantic_manifest_lookup=simple_semantic_manifest_lookup,
        ),
        semantic_model_lookup=simple_semantic_manifest_lookup.semantic_model_lookup,
    ).create_from_where_filter_intersection(
        filter_location=EXAMPLE_FILTER_LOCATION,
        filter_intersection=create_where_filter_intersection("{{ Dimension('listing__country_latest') }} = 'US'"),
    )
    assert len(where_filter_specs) == 1
    where_filter_spec = where_filter_specs[0]
    assert where_filter_spec.where_sql == "listing__country_latest = 'US'"
    assert LinkableSpecSet.create_from_specs(where_filter_spec.linkable_specs) == LinkableSpecSet(
        dimension_specs=(
            DimensionSpec(element_name="country_latest", entity_links=(EntityReference(element_name="listing"),)),
        ),
        time_dimension_specs=(),
        entity_specs=(),
        group_by_metric_specs=(),
    )


def test_dimension_in_filter_with_grain(  # noqa: D103
    column_association_resolver: ColumnAssociationResolver,
    simple_semantic_manifest_lookup: SemanticManifestLookup,
) -> None:
    where_filter_specs = WhereSpecFactory(
        column_association_resolver=column_association_resolver,
        spec_resolution_lookup=create_spec_lookup(
            call_parameter_set=TimeDimensionCallParameterSet(
                entity_path=(EntityReference("listing"),),
                time_dimension_reference=TimeDimensionReference("created_at"),
                time_granularity_name=TimeGranularity.WEEK.value,
            ),
            resolved_spec=TimeDimensionSpec(
                element_name="created_at",
                entity_links=(EntityReference("listing"),),
                time_granularity=ExpandedTimeGranularity.from_time_granularity(TimeGranularity.WEEK),
            ),
            resolved_linkable_element_set=GroupByItemSet.create(
                AnnotatedSpec.create(
                    element_type=LinkableElementType.TIME_DIMENSION,
                    element_name="created_at",
                    entity_links=(EntityReference("listing"),),
                    time_grain=ExpandedTimeGranularity.from_time_granularity(TimeGranularity.WEEK),
                    date_part=None,
                    metric_subquery_entity_links=None,
                    properties=frozenset(),
                    origin_model_ids=[SemanticModelId.get_instance("listings_source")],
                    derived_from_semantic_models=[SemanticModelReference("bookings_source")],
                ),
            ),
            semantic_manifest_lookup=simple_semantic_manifest_lookup,
        ),
        semantic_model_lookup=simple_semantic_manifest_lookup.semantic_model_lookup,
    ).create_from_where_filter_intersection(
        filter_location=EXAMPLE_FILTER_LOCATION,
        filter_intersection=create_where_filter_intersection(
            "{{ Dimension('listing__created_at').grain('WEEK') }} = 'US'"
        ),
    )
    assert len(where_filter_specs) == 1
    where_filter_spec = where_filter_specs[0]
    assert where_filter_spec.where_sql == "listing__created_at__week = 'US'"
    assert LinkableSpecSet.create_from_specs(where_filter_spec.linkable_specs) == LinkableSpecSet(
        dimension_specs=(),
        time_dimension_specs=(
            TimeDimensionSpec(
                element_name="created_at",
                entity_links=(EntityReference(element_name="listing"),),
                time_granularity=ExpandedTimeGranularity.from_time_granularity(TimeGranularity.WEEK),
            ),
        ),
        entity_specs=(),
        group_by_metric_specs=(),
    )


def test_time_dimension_in_filter(  # noqa: D103
    column_association_resolver: ColumnAssociationResolver,
    simple_semantic_manifest_lookup: SemanticManifestLookup,
) -> None:
    where_filter_specs = WhereSpecFactory(
        column_association_resolver=column_association_resolver,
        spec_resolution_lookup=create_spec_lookup(
            call_parameter_set=TimeDimensionCallParameterSet(
                entity_path=(EntityReference("listing"),),
                time_dimension_reference=TimeDimensionReference("created_at"),
                time_granularity_name=TimeGranularity.MONTH.value,
            ),
            resolved_spec=TimeDimensionSpec(
                element_name="created_at",
                entity_links=(EntityReference("listing"),),
                time_granularity=ExpandedTimeGranularity.from_time_granularity(TimeGranularity.MONTH),
            ),
            resolved_linkable_element_set=GroupByItemSet.create(
                AnnotatedSpec.create(
                    element_type=LinkableElementType.TIME_DIMENSION,
                    element_name="created_at",
                    entity_links=(EntityReference("listing"),),
                    time_grain=ExpandedTimeGranularity.from_time_granularity(TimeGranularity.MONTH),
                    date_part=None,
                    metric_subquery_entity_links=None,
                    properties=frozenset(),
                    origin_model_ids=[SemanticModelId.get_instance("listings_source")],
                    derived_from_semantic_models=[SemanticModelReference("bookings_source")],
                ),
            ),
            semantic_manifest_lookup=simple_semantic_manifest_lookup,
        ),
        semantic_model_lookup=simple_semantic_manifest_lookup.semantic_model_lookup,
    ).create_from_where_filter_intersection(
        filter_location=EXAMPLE_FILTER_LOCATION,
        filter_intersection=create_where_filter_intersection(
            "{{ TimeDimension('listing__created_at', 'month') }} = '2020-01-01'"
        ),
    )
    assert len(where_filter_specs) == 1
    where_filter_spec = where_filter_specs[0]
    assert where_filter_spec.where_sql == "listing__created_at__month = '2020-01-01'"
    assert LinkableSpecSet.create_from_specs(where_filter_spec.linkable_specs) == LinkableSpecSet(
        dimension_specs=(),
        time_dimension_specs=(
            TimeDimensionSpec(
                element_name="created_at",
                entity_links=(EntityReference(element_name="listing"),),
                time_granularity=ExpandedTimeGranularity.from_time_granularity(TimeGranularity.MONTH),
            ),
        ),
        entity_specs=(),
        group_by_metric_specs=(),
    )


def test_time_dimension_with_grain_in_name(  # noqa: D103
    column_association_resolver: ColumnAssociationResolver,
    simple_semantic_manifest_lookup: SemanticManifestLookup,
) -> None:
    where_filter_specs = WhereSpecFactory(
        column_association_resolver=column_association_resolver,
        spec_resolution_lookup=create_spec_lookup(
            call_parameter_set=TimeDimensionCallParameterSet(
                entity_path=(EntityReference("listing"),),
                time_dimension_reference=TimeDimensionReference("created_at"),
                time_granularity_name=TimeGranularity.MONTH.value,
            ),
            resolved_spec=TimeDimensionSpec(
                element_name="created_at",
                entity_links=(EntityReference("listing"),),
                time_granularity=ExpandedTimeGranularity.from_time_granularity(TimeGranularity.MONTH),
            ),
            resolved_linkable_element_set=GroupByItemSet.create(
                AnnotatedSpec.create(
                    element_type=LinkableElementType.TIME_DIMENSION,
                    element_name="created_at",
                    entity_links=(EntityReference("listing"),),
                    time_grain=ExpandedTimeGranularity.from_time_granularity(TimeGranularity.MONTH),
                    date_part=None,
                    metric_subquery_entity_links=None,
                    properties=frozenset(),
                    origin_model_ids=[SemanticModelId.get_instance("listings_source")],
                    derived_from_semantic_models=[SemanticModelReference("bookings_source")],
                ),
            ),
            semantic_manifest_lookup=simple_semantic_manifest_lookup,
        ),
        semantic_model_lookup=simple_semantic_manifest_lookup.semantic_model_lookup,
    ).create_from_where_filter_intersection(
        filter_location=EXAMPLE_FILTER_LOCATION,
        filter_intersection=create_where_filter_intersection(
            "{{ TimeDimension('listing__created_at__month') }} = '2020-01-01'"
        ),
    )
    assert len(where_filter_specs) == 1
    where_filter_spec = where_filter_specs[0]
    assert where_filter_spec.where_sql == "listing__created_at__month = '2020-01-01'"
    assert LinkableSpecSet.create_from_specs(where_filter_spec.linkable_specs) == LinkableSpecSet(
        dimension_specs=(),
        time_dimension_specs=(
            TimeDimensionSpec(
                element_name="created_at",
                entity_links=(EntityReference(element_name="listing"),),
                time_granularity=ExpandedTimeGranularity.from_time_granularity(TimeGranularity.MONTH),
            ),
        ),
        entity_specs=(),
        group_by_metric_specs=(),
    )


def test_date_part_in_filter(  # noqa: D103
    column_association_resolver: ColumnAssociationResolver,
    simple_semantic_manifest_lookup: SemanticManifestLookup,
) -> None:
    where_filter_specs = WhereSpecFactory(
        column_association_resolver=column_association_resolver,
        spec_resolution_lookup=create_spec_lookup(
            call_parameter_set=TimeDimensionCallParameterSet(
                entity_path=(),
                time_dimension_reference=TimeDimensionReference("metric_time"),
                date_part=DatePart.YEAR,
            ),
            resolved_spec=TimeDimensionSpec(
                element_name="metric_time",
                entity_links=(),
                date_part=DatePart.YEAR,
            ),
            resolved_linkable_element_set=GroupByItemSet.create(
                AnnotatedSpec.create(
                    element_type=LinkableElementType.TIME_DIMENSION,
                    element_name="metric_time",
                    entity_links=(),
                    time_grain=None,
                    date_part=DatePart.YEAR,
                    metric_subquery_entity_links=None,
                    properties=frozenset(),
                    origin_model_ids=[SemanticModelId.get_instance("bookings")],
                    derived_from_semantic_models=[SemanticModelReference("bookings_source")],
                ),
            ),
            semantic_manifest_lookup=simple_semantic_manifest_lookup,
        ),
        semantic_model_lookup=simple_semantic_manifest_lookup.semantic_model_lookup,
    ).create_from_where_filter_intersection(
        filter_location=EXAMPLE_FILTER_LOCATION,
        filter_intersection=create_where_filter_intersection(
            "{{ Dimension('metric_time').date_part('year') }} = '2020'"
        ),
    )
    assert len(where_filter_specs) == 1
    where_filter_spec = where_filter_specs[0]
    assert where_filter_spec.where_sql == "metric_time__extract_year = '2020'"
    assert LinkableSpecSet.create_from_specs(where_filter_spec.linkable_specs) == LinkableSpecSet(
        dimension_specs=(),
        time_dimension_specs=(
            TimeDimensionSpec(
                element_name="metric_time",
                entity_links=(),
                date_part=DatePart.YEAR,
            ),
        ),
        entity_specs=(),
        group_by_metric_specs=(),
    )


@pytest.fixture(scope="session")
def resolved_spec_lookup(
    simple_semantic_manifest_lookup: SemanticManifestLookup,
) -> FilterSpecResolutionLookUp:
    """A spec lookup that maps "TimeDimension('metric_time', 'week', 'year')" to the corresponding spec."""
    return FilterSpecResolutionLookUp(
        spec_resolutions=(
            FilterSpecResolution(
                lookup_key=ResolvedSpecLookUpKey.from_parameters(
                    filter_location=EXAMPLE_FILTER_LOCATION,
                    call_parameter_set=TimeDimensionCallParameterSet(
                        time_dimension_reference=TimeDimensionReference(element_name=METRIC_TIME_ELEMENT_NAME),
                        entity_path=(),
                        time_granularity_name=TimeGranularity.WEEK.value,
                        date_part=DatePart.YEAR,
                    ),
                ),
                filter_location_path=MetricFlowQueryResolutionPath.empty_instance(),
                where_filter_intersection=create_where_filter_intersection(
                    "TimeDimension('metric_time', 'week', 'year')"
                ),
                resolved_group_by_item_set=GroupByItemSet.create(
                    AnnotatedSpec.create(
                        element_type=LinkableElementType.TIME_DIMENSION,
                        element_name="metric_time",
                        entity_links=(),
                        time_grain=None,
                        date_part=DatePart.YEAR,
                        metric_subquery_entity_links=None,
                        properties=frozenset(),
                        origin_model_ids=[SemanticModelId.get_instance("bookings")],
                        derived_from_semantic_models=[SemanticModelReference("bookings_source")],
                    ),
                ),
                spec_pattern=ObjectBuilderNamingScheme().spec_pattern(
                    "Dimension('dummy__dimension')", semantic_manifest_lookup=simple_semantic_manifest_lookup
                ),
                issue_set=MetricFlowQueryResolutionIssueSet.empty_instance(),
                object_builder_str="Dimension('dummy__dimension')",
            ),
        ),
        non_parsable_resolutions=(),
    )


@pytest.mark.parametrize(
    "where_sql",
    (
        ("{{ TimeDimension('metric_time', 'WEEK', date_part_name='year') }} = '2020'"),
        ("{{ Dimension('metric_time').date_part('year').grain('WEEK') }} = '2020'"),
        ("{{ Dimension('metric_time').grain('WEEK').date_part('year') }} = '2020'"),
    ),
)
def test_date_part_and_grain_in_filter(  # noqa: D103
    column_association_resolver: ColumnAssociationResolver,
    resolved_spec_lookup: FilterSpecResolutionLookUp,
    simple_semantic_manifest_lookup: SemanticManifestLookup,
    where_sql: str,
) -> None:
    where_filter = PydanticWhereFilter(where_sql_template=where_sql)

    where_filter_spec = WhereSpecFactory(
        column_association_resolver=column_association_resolver,
        spec_resolution_lookup=resolved_spec_lookup,
        semantic_model_lookup=simple_semantic_manifest_lookup.semantic_model_lookup,
    ).create_from_where_filter(EXAMPLE_FILTER_LOCATION, where_filter)

    assert where_filter_spec.where_sql == "metric_time__extract_year = '2020'"
    assert LinkableSpecSet.create_from_specs(where_filter_spec.linkable_specs) == LinkableSpecSet(
        dimension_specs=(),
        time_dimension_specs=(
            TimeDimensionSpec(
                element_name="metric_time",
                entity_links=(),
                date_part=DatePart.YEAR,
            ),
        ),
        entity_specs=(),
        group_by_metric_specs=(),
    )


def test_entity_in_filter(  # noqa: D103
    column_association_resolver: ColumnAssociationResolver,
    simple_semantic_manifest_lookup: SemanticManifestLookup,
) -> None:
    where_filter = PydanticWhereFilter(
        where_sql_template="{{ Entity('user', entity_path=['listing']) }} == 'example_user_id'"
    )

    where_filter_spec = WhereSpecFactory(
        column_association_resolver=column_association_resolver,
        spec_resolution_lookup=create_spec_lookup(
            call_parameter_set=EntityCallParameterSet(
                entity_path=(EntityReference("listing"),),
                entity_reference=EntityReference("user"),
            ),
            resolved_spec=EntitySpec(element_name="user", entity_links=(EntityReference("listing"),)),
            resolved_linkable_element_set=GroupByItemSet.create(
                AnnotatedSpec.create(
                    element_type=LinkableElementType.ENTITY,
                    element_name="user",
                    entity_links=(EntityReference("listing"),),
                    time_grain=None,
                    date_part=None,
                    metric_subquery_entity_links=None,
                    properties=frozenset(),
                    origin_model_ids=[SemanticModelId.get_instance("bookings")],
                    derived_from_semantic_models=[SemanticModelReference("bookings_source")],
                ),
            ),
            semantic_manifest_lookup=simple_semantic_manifest_lookup,
        ),
        semantic_model_lookup=simple_semantic_manifest_lookup.semantic_model_lookup,
    ).create_from_where_filter(filter_location=EXAMPLE_FILTER_LOCATION, where_filter=where_filter)

    assert where_filter_spec.where_sql == "listing__user == 'example_user_id'"
    assert LinkableSpecSet.create_from_specs(where_filter_spec.linkable_specs) == LinkableSpecSet(
        dimension_specs=(),
        time_dimension_specs=(),
        entity_specs=(EntitySpec(element_name="user", entity_links=(EntityReference(element_name="listing"),)),),
        group_by_metric_specs=(),
    )


def test_metric_in_filter(  # noqa: D103
    column_association_resolver: ColumnAssociationResolver,
    simple_semantic_manifest_lookup: SemanticManifestLookup,
) -> None:
    where_filter = PydanticWhereFilter(where_sql_template="{{ Metric('bookings', group_by=['listing']) }} > 2")

    group_by_metric_spec = GroupByMetricSpec(
        element_name="bookings",
        entity_links=(EntityReference("listing"),),
        metric_subquery_entity_links=(EntityReference(element_name="listing"),),
    )
    where_filter_spec = WhereSpecFactory(
        column_association_resolver=column_association_resolver,
        spec_resolution_lookup=create_spec_lookup(
            call_parameter_set=MetricCallParameterSet(
                group_by=(EntityReference("listing"),),
                metric_reference=MetricReference("bookings"),
            ),
            resolved_spec=group_by_metric_spec,
            resolved_linkable_element_set=GroupByItemSet.create(
                AnnotatedSpec.create(
                    element_type=LinkableElementType.METRIC,
                    element_name="bookings",
                    entity_links=(EntityReference("listing"),),
                    time_grain=None,
                    date_part=None,
                    metric_subquery_entity_links=(EntityReference("listing"),),
                    properties=frozenset({GroupByItemProperty.METRIC, GroupByItemProperty.JOINED}),
                    origin_model_ids=[SemanticModelId.get_instance("listings_latest")],
                    derived_from_semantic_models=[SemanticModelReference("bookings")],
                ),
            ),
            semantic_manifest_lookup=simple_semantic_manifest_lookup,
        ),
        semantic_model_lookup=simple_semantic_manifest_lookup.semantic_model_lookup,
    ).create_from_where_filter(filter_location=EXAMPLE_FILTER_LOCATION, where_filter=where_filter)

    assert where_filter_spec.where_sql == "listing__bookings > 2"
    assert LinkableSpecSet.create_from_specs(where_filter_spec.linkable_specs) == LinkableSpecSet(
        dimension_specs=(),
        time_dimension_specs=(),
        entity_specs=(),
        group_by_metric_specs=(group_by_metric_spec,),
    )


def test_dimension_time_dimension_parity(  # noqa: D103
    column_association_resolver: ColumnAssociationResolver,
    simple_semantic_manifest_lookup: SemanticManifestLookup,
) -> None:
    def get_spec(dimension: str) -> WhereFilterSpec:
        where_filter = PydanticWhereFilter(where_sql_template="{{" + dimension + "}} = '2020'")
        filter_location = WhereFilterLocation.for_query((MetricReference("example_metric"),))
        return WhereSpecFactory(
            column_association_resolver=column_association_resolver,
            spec_resolution_lookup=FilterSpecResolutionLookUp(
                spec_resolutions=(
                    FilterSpecResolution(
                        lookup_key=ResolvedSpecLookUpKey(
                            filter_location=filter_location,
                            call_parameter_set=TimeDimensionCallParameterSet(
                                entity_path=(),
                                time_dimension_reference=TimeDimensionReference(METRIC_TIME_ELEMENT_NAME),
                                time_granularity_name=TimeGranularity.WEEK.value,
                            ),
                        ),
                        filter_location_path=MetricFlowQueryResolutionPath(()),
                        where_filter_intersection=PydanticWhereFilterIntersection(where_filters=[where_filter]),
                        resolved_group_by_item_set=GroupByItemSet.create(
                            AnnotatedSpec.create(
                                element_type=LinkableElementType.TIME_DIMENSION,
                                element_name=METRIC_TIME_ELEMENT_NAME,
                                entity_links=(EntityReference("listing"),),
                                time_grain=ExpandedTimeGranularity.from_time_granularity(TimeGranularity.WEEK),
                                date_part=None,
                                metric_subquery_entity_links=None,
                                properties=frozenset(),
                                origin_model_ids=[SemanticModelId.get_instance("bookings")],
                                derived_from_semantic_models=[SemanticModelReference("bookings_source")],
                            ),
                        ),
                        spec_pattern=ObjectBuilderNamingScheme().spec_pattern(
                            "Dimension('dummy__dimension')", semantic_manifest_lookup=simple_semantic_manifest_lookup
                        ),
                        issue_set=MetricFlowQueryResolutionIssueSet.empty_instance(),
                        object_builder_str="Dimension('dummy__dimension')",
                    ),
                ),
                non_parsable_resolutions=(),
            ),
            semantic_model_lookup=simple_semantic_manifest_lookup.semantic_model_lookup,
        ).create_from_where_filter(filter_location, where_filter)

    time_dimension_spec = get_spec("TimeDimension('metric_time', 'week')")
    dimension_spec = get_spec("Dimension('metric_time').grain('week')")

    assert time_dimension_spec == dimension_spec
