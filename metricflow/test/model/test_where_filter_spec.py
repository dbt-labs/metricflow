from __future__ import annotations

import logging

import pytest
from dbt_semantic_interfaces.implementations.filters.where_filter import PydanticWhereFilter
from dbt_semantic_interfaces.references import EntityReference
from dbt_semantic_interfaces.type_enums.date_part import DatePart
from dbt_semantic_interfaces.type_enums.time_granularity import TimeGranularity

from metricflow.query.query_exceptions import InvalidQueryException
from metricflow.specs.column_assoc import ColumnAssociationResolver
from metricflow.specs.specs import (
    DimensionSpec,
    EntitySpec,
    LinkableSpecSet,
    TimeDimensionSpec,
    WhereFilterSpec,
)
from metricflow.specs.where_filter_transform import WhereSpecFactory

logger = logging.getLogger(__name__)


def test_dimension_in_filter(  # noqa: D
    column_association_resolver: ColumnAssociationResolver,
) -> None:
    where_filter = PydanticWhereFilter(where_sql_template="{{ Dimension('listing__country_latest') }} = 'US'")

    where_filter_spec = WhereSpecFactory(
        column_association_resolver=column_association_resolver,
    ).create_from_where_filter(where_filter)

    assert where_filter_spec.where_sql == "listing__country_latest = 'US'"
    assert where_filter_spec.linkable_spec_set == LinkableSpecSet(
        dimension_specs=(
            DimensionSpec(element_name="country_latest", entity_links=(EntityReference(element_name="listing"),)),
        ),
        time_dimension_specs=(),
        entity_specs=(),
    )


def test_dimension_in_filter_with_grain(  # noqa: D
    column_association_resolver: ColumnAssociationResolver,
) -> None:
    where_filter = PydanticWhereFilter(
        where_sql_template="{{ Dimension('listing__country_latest').grain('WEEK') }} = 'US'"
    )

    where_filter_spec = WhereSpecFactory(
        column_association_resolver=column_association_resolver,
    ).create_from_where_filter(where_filter)

    assert where_filter_spec.where_sql == "listing__country_latest__week = 'US'"
    assert where_filter_spec.linkable_spec_set == LinkableSpecSet(
        dimension_specs=(),
        time_dimension_specs=(
            TimeDimensionSpec(
                element_name="country_latest",
                entity_links=(EntityReference(element_name="listing"),),
                time_granularity=TimeGranularity.WEEK,
            ),
        ),
        entity_specs=(),
    )


def test_time_dimension_without_grain(column_association_resolver: ColumnAssociationResolver) -> None:  # noqa
    where_filter = PydanticWhereFilter(where_sql_template="{{ TimeDimension('metric_time') }} > '2023-10-17'")

    with pytest.raises(InvalidQueryException):
        WhereSpecFactory(
            column_association_resolver=column_association_resolver,
        ).create_from_where_filter(where_filter)


def test_time_dimension_in_filter(  # noqa: D
    column_association_resolver: ColumnAssociationResolver,
) -> None:
    where_filter = PydanticWhereFilter(
        where_sql_template="{{ TimeDimension('listing__created_at', 'month') }} = '2020-01-01'"
    )

    where_filter_spec = WhereSpecFactory(
        column_association_resolver=column_association_resolver,
    ).create_from_where_filter(where_filter)

    assert where_filter_spec.where_sql == "listing__created_at__month = '2020-01-01'"
    assert where_filter_spec.linkable_spec_set == LinkableSpecSet(
        dimension_specs=(),
        time_dimension_specs=(
            TimeDimensionSpec(
                element_name="created_at",
                entity_links=(EntityReference(element_name="listing"),),
                time_granularity=TimeGranularity.MONTH,
            ),
        ),
        entity_specs=(),
    )


def test_date_part_in_filter(  # noqa: D
    column_association_resolver: ColumnAssociationResolver,
) -> None:
    where_filter = PydanticWhereFilter(where_sql_template="{{ Dimension('metric_time').date_part('year') }} = '2020'")

    where_filter_spec = WhereSpecFactory(
        column_association_resolver=column_association_resolver,
    ).create_from_where_filter(where_filter)

    assert where_filter_spec.where_sql == "metric_time__extract_year = '2020'"
    assert where_filter_spec.linkable_spec_set == LinkableSpecSet(
        dimension_specs=(),
        time_dimension_specs=(
            TimeDimensionSpec(
                element_name="metric_time",
                entity_links=(),
                time_granularity=TimeGranularity.DAY,
                date_part=DatePart.YEAR,
            ),
        ),
        entity_specs=(),
    )


@pytest.mark.parametrize(
    "where_sql",
    (
        ("{{ TimeDimension('metric_time', 'WEEK', date_part_name='year') }} = '2020'"),
        ("{{ Dimension('metric_time').date_part('year').grain('WEEK') }} = '2020'"),
        ("{{ Dimension('metric_time').grain('WEEK').date_part('year') }} = '2020'"),
    ),
)
def test_date_part_and_grain_in_filter(  # noqa: D
    column_association_resolver: ColumnAssociationResolver, where_sql: str
) -> None:
    where_filter = PydanticWhereFilter(where_sql_template=where_sql)

    where_filter_spec = WhereSpecFactory(
        column_association_resolver=column_association_resolver,
    ).create_from_where_filter(where_filter)

    assert where_filter_spec.where_sql == "metric_time__extract_year = '2020'"
    assert where_filter_spec.linkable_spec_set == LinkableSpecSet(
        dimension_specs=(),
        time_dimension_specs=(
            TimeDimensionSpec(
                element_name="metric_time",
                entity_links=(),
                time_granularity=TimeGranularity.WEEK,
                date_part=DatePart.YEAR,
            ),
        ),
        entity_specs=(),
    )


@pytest.mark.parametrize(
    "where_sql",
    (
        ("{{ TimeDimension('metric_time', 'WEEK', date_part_name='day') }} = '2020'"),
        ("{{ Dimension('metric_time').date_part('day').grain('WEEK') }} = '2020'"),
        ("{{ Dimension('metric_time').grain('WEEK').date_part('day') }} = '2020'"),
    ),
)
def test_date_part_less_than_grain_in_filter(  # noqa: D
    column_association_resolver: ColumnAssociationResolver, where_sql: str
) -> None:
    where_filter = PydanticWhereFilter(where_sql_template=where_sql)

    where_filter_spec = WhereSpecFactory(
        column_association_resolver=column_association_resolver,
    ).create_from_where_filter(where_filter)

    assert where_filter_spec.where_sql == "metric_time__extract_day = '2020'"
    assert where_filter_spec.linkable_spec_set == LinkableSpecSet(
        dimension_specs=(),
        time_dimension_specs=(
            TimeDimensionSpec(
                element_name="metric_time",
                entity_links=(),
                time_granularity=TimeGranularity.WEEK,
                date_part=DatePart.DAY,
            ),
        ),
        entity_specs=(),
    )


def test_entity_in_filter(  # noqa: D
    column_association_resolver: ColumnAssociationResolver,
) -> None:
    where_filter = PydanticWhereFilter(
        where_sql_template="{{ Entity('user', entity_path=['listing']) }} == 'example_user_id'"
    )

    where_filter_spec = WhereSpecFactory(
        column_association_resolver=column_association_resolver,
    ).create_from_where_filter(where_filter)

    assert where_filter_spec.where_sql == "listing__user == 'example_user_id'"
    assert where_filter_spec.linkable_spec_set == LinkableSpecSet(
        dimension_specs=(),
        time_dimension_specs=(),
        entity_specs=(EntitySpec(element_name="user", entity_links=(EntityReference(element_name="listing"),)),),
    )


def test_dimension_time_dimension_parity(column_association_resolver: ColumnAssociationResolver) -> None:  # noqa
    def get_spec(dimension: str) -> WhereFilterSpec:
        where_filter = PydanticWhereFilter(where_sql_template="{{" + dimension + "}} = '2020'")

        return WhereSpecFactory(
            column_association_resolver=column_association_resolver,
        ).create_from_where_filter(where_filter)

    time_dimension_spec = get_spec("TimeDimension('metric_time', 'week', date_part_name='day')")
    dimension_spec = get_spec("Dimension('metric_time').date_part('day').grain('week')")

    assert time_dimension_spec == dimension_spec
