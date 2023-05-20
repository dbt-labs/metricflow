import logging

from dbt_semantic_interfaces.objects.filters.where_filter import WhereFilter
from dbt_semantic_interfaces.type_enums.time_granularity import TimeGranularity
from dbt_semantic_interfaces.references import EntityReference
from metricflow.specs.specs import (
    ColumnAssociationResolver,
    LinkableSpecSet,
    DimensionSpec,
    EntitySpec,
    TimeDimensionSpec,
)
from metricflow.specs.where_filter_transform import ConvertToWhereSpec

logger = logging.getLogger(__name__)


def test_dimension_in_filter(  # noqa: D
    column_association_resolver: ColumnAssociationResolver,
) -> None:
    where_filter = WhereFilter(where_sql_template="{{ dimension('country_latest', entity_path=['listing']) }} = 'US'")

    where_filter_spec = where_filter.transform(
        ConvertToWhereSpec(
            column_association_resolver=column_association_resolver,
        )
    )

    assert where_filter_spec.where_sql == "listing__country_latest = 'US'"
    assert where_filter_spec.linkable_spec_set == LinkableSpecSet(
        dimension_specs=(
            DimensionSpec(element_name="country_latest", entity_links=(EntityReference(element_name="listing"),)),
        ),
        time_dimension_specs=(),
        entity_specs=(),
    )


def test_time_dimension_in_filter(  # noqa: D
    column_association_resolver: ColumnAssociationResolver,
) -> None:
    where_filter = WhereFilter(
        where_sql_template="{{ time_dimension('created_at', 'month', entity_path=['listing']) }} = '2020-01-01'"
    )

    where_filter_spec = where_filter.transform(
        ConvertToWhereSpec(
            column_association_resolver=column_association_resolver,
        )
    )

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
    where_filter = WhereFilter(where_sql_template="{{ entity('user', entity_path=['listing']) }} == 'example_user_id'")

    where_filter_spec = where_filter.transform(
        ConvertToWhereSpec(
            column_association_resolver=column_association_resolver,
        )
    )

    assert where_filter_spec.where_sql == "listing__user == 'example_user_id'"
    assert where_filter_spec.linkable_spec_set == LinkableSpecSet(
        dimension_specs=(),
        time_dimension_specs=(),
        entity_specs=(EntitySpec(element_name="user", entity_links=(EntityReference(element_name="listing"),)),),
    )
