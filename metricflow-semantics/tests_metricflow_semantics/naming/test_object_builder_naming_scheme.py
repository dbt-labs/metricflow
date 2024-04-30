from __future__ import annotations

from typing import Sequence

import pytest
from dbt_semantic_interfaces.references import EntityReference
from dbt_semantic_interfaces.type_enums import TimeGranularity
from dbt_semantic_interfaces.type_enums.date_part import DatePart
from metricflow_semantics.naming.object_builder_scheme import ObjectBuilderNamingScheme
from metricflow_semantics.specs.spec_classes import (
    DimensionSpec,
    EntitySpec,
    GroupByMetricSpec,
    LinkableInstanceSpec,
    TimeDimensionSpec,
)
from metricflow_semantics.test_helpers.metric_time_dimension import MTD_SPEC_MONTH, MTD_SPEC_WEEK, MTD_SPEC_YEAR


@pytest.fixture(scope="session")
def object_builder_naming_scheme() -> ObjectBuilderNamingScheme:  # noqa: D103
    return ObjectBuilderNamingScheme()


def test_input_str(object_builder_naming_scheme: ObjectBuilderNamingScheme) -> None:  # noqa: D103
    assert (
        object_builder_naming_scheme.input_str(
            DimensionSpec(
                element_name="country",
                entity_links=(EntityReference(element_name="booking"), EntityReference(element_name="listing")),
            )
        )
        == "Dimension('listing__country', entity_path=['booking'])"
    )

    assert object_builder_naming_scheme.input_str(
        TimeDimensionSpec(
            element_name="creation_time",
            entity_links=(EntityReference(element_name="booking"), EntityReference(element_name="listing")),
            time_granularity=TimeGranularity.MONTH,
            date_part=DatePart.DAY,
        )
    ) == ("TimeDimension('listing__creation_time', 'month', date_part_name='day', entity_path=['booking'])")

    assert (
        object_builder_naming_scheme.input_str(
            EntitySpec(
                element_name="user",
                entity_links=(EntityReference(element_name="booking"), EntityReference(element_name="listing")),
            )
        )
        == "Entity('listing__user', entity_path=['booking'])"
    )

    assert (
        object_builder_naming_scheme.input_str(
            GroupByMetricSpec(
                element_name="bookings",
                entity_links=(EntityReference(element_name="listing"),),
                metric_subquery_entity_links=(EntityReference(element_name="listing"),),
            )
        )
        == "Metric('bookings', group_by=['listing'])"
    )


def test_input_follows_scheme(object_builder_naming_scheme: ObjectBuilderNamingScheme) -> None:  # noqa: D103
    assert object_builder_naming_scheme.input_str_follows_scheme(
        "Dimension('listing__country', entity_path=['booking'])"
    )
    assert object_builder_naming_scheme.input_str_follows_scheme(
        "TimeDimension('listing__creation_time', time_granularity_name='month', date_part_name='day', "
        "entity_path=['booking'])"
    )
    assert object_builder_naming_scheme.input_str_follows_scheme(
        "Entity('user', entity_path=['booking', 'listing'])",
    )
    assert not object_builder_naming_scheme.input_str_follows_scheme("listing__creation_time__extract_month")
    assert not object_builder_naming_scheme.input_str_follows_scheme("123")
    assert not object_builder_naming_scheme.input_str_follows_scheme("NotADimension('listing__country')")


def test_spec_pattern(  # noqa: D103
    object_builder_naming_scheme: ObjectBuilderNamingScheme, specs: Sequence[LinkableInstanceSpec]
) -> None:
    assert tuple(
        object_builder_naming_scheme.spec_pattern("Dimension('listing__country', entity_path=['booking'])").match(specs)
    ) == (
        DimensionSpec(
            element_name="country",
            entity_links=(
                EntityReference(element_name="booking"),
                EntityReference(element_name="listing"),
            ),
        ),
    )

    assert tuple(
        object_builder_naming_scheme.spec_pattern(
            "TimeDimension('listing__creation_time', time_granularity_name='month', date_part_name='day', "
            "entity_path=['booking'])"
        ).match(specs)
    ) == (
        TimeDimensionSpec(
            element_name="creation_time",
            entity_links=(EntityReference("booking"), EntityReference("listing")),
            time_granularity=TimeGranularity.MONTH,
            date_part=DatePart.DAY,
        ),
    )

    assert tuple(object_builder_naming_scheme.spec_pattern("TimeDimension('metric_time')").match(specs)) == (
        MTD_SPEC_WEEK,
        MTD_SPEC_MONTH,
        MTD_SPEC_YEAR,
    )

    assert tuple(
        object_builder_naming_scheme.spec_pattern("Entity('user', entity_path=['booking', 'listing'])").match(specs)
    ) == (
        EntitySpec(
            element_name="user",
            entity_links=(EntityReference(element_name="booking"), EntityReference(element_name="listing")),
        ),
    )

    assert tuple(
        object_builder_naming_scheme.spec_pattern("Metric('bookings', group_by=['listing'])").match(specs)
    ) == (
        GroupByMetricSpec(
            element_name="bookings",
            entity_links=(EntityReference(element_name="listing"),),
            metric_subquery_entity_links=(EntityReference(element_name="listing"),),
        ),
    )
