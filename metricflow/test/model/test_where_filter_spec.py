from __future__ import annotations

import logging

from dbt_semantic_interfaces.implementations.filters.where_filter import PydanticWhereFilter
from dbt_semantic_interfaces.references import EntityReference
from dbt_semantic_interfaces.type_enums.time_granularity import TimeGranularity

from metricflow.specs.column_assoc import ColumnAssociationResolver
from metricflow.specs.specs import (
    DimensionSpec,
    EntitySpec,
    LinkableSpecSet,
    TimeDimensionSpec,
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
