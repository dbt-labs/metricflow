import logging

import pytest

from dbt_semantic_interfaces.objects.filters.where_filter import WhereFilter
from dbt_semantic_interfaces.objects.time_granularity import TimeGranularity
from dbt_semantic_interfaces.references import EntityReference
from metricflow.model.semantic_model import SemanticModel
from metricflow.plan_conversion.column_resolver import DefaultColumnAssociationResolver
from metricflow.specs import (
    ColumnAssociationResolver,
    LinkableSpecSet,
    DimensionSpec,
    EntitySpec,
    TimeDimensionSpec,
    ResolvedWhereFilter,
)

logger = logging.getLogger(__name__)


@pytest.fixture
def column_association_resolver(simple_semantic_model: SemanticModel) -> ColumnAssociationResolver:  # noqa: D
    return DefaultColumnAssociationResolver(simple_semantic_model)


def test_dimension_in_filter(  # noqa: D
    simple_semantic_model: SemanticModel,
    column_association_resolver: ColumnAssociationResolver,
) -> None:
    where_filter = WhereFilter(where_sql_template="{{ dimension('country_latest', entity_path=['listing']) }} = 'US'")

    resolved_where_filter = ResolvedWhereFilter.create_from_where_filter(
        where_filter=where_filter,
        column_association_resolver=column_association_resolver,
    )

    assert resolved_where_filter.where_sql == "listing__country_latest = 'US'"
    assert resolved_where_filter.linkable_spec_set == LinkableSpecSet(
        dimension_specs=(
            DimensionSpec(element_name="country_latest", entity_links=(EntityReference(element_name="listing"),)),
        ),
        time_dimension_specs=(),
        entity_specs=(),
    )


def test_time_dimension_in_filter(  # noqa: D
    simple_semantic_model: SemanticModel,
    column_association_resolver: ColumnAssociationResolver,
) -> None:
    where_filter = WhereFilter(
        where_sql_template="{{ time_dimension('created_at', 'month', entity_path=['listing']) }} = '2020-01-01'"
    )

    resolved_where_filter = ResolvedWhereFilter.create_from_where_filter(
        where_filter=where_filter,
        column_association_resolver=column_association_resolver,
    )

    assert resolved_where_filter.where_sql == "listing__created_at__month = '2020-01-01'"
    assert resolved_where_filter.linkable_spec_set == LinkableSpecSet(
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
    simple_semantic_model: SemanticModel,
    column_association_resolver: ColumnAssociationResolver,
) -> None:
    where_filter = WhereFilter(where_sql_template="{{ entity('user', entity_path=['listing']) }} == 'example_user_id'")

    resolved_where_filter = ResolvedWhereFilter.create_from_where_filter(
        where_filter=where_filter,
        column_association_resolver=column_association_resolver,
    )

    assert resolved_where_filter.where_sql == "listing__user == 'example_user_id'"
    assert resolved_where_filter.linkable_spec_set == LinkableSpecSet(
        dimension_specs=(),
        time_dimension_specs=(),
        entity_specs=(EntitySpec(element_name="user", entity_links=(EntityReference(element_name="listing"),)),),
    )
