from __future__ import annotations

from typing import Sequence

import pytest
from dbt_semantic_interfaces.references import EntityReference
from dbt_semantic_interfaces.type_enums import TimeGranularity
from dbt_semantic_interfaces.type_enums.date_part import DatePart

from metricflow.naming.dunder_scheme import DunderNamingScheme
from metricflow.specs.specs import DimensionSpec, EntitySpec, LinkableInstanceSpec, TimeDimensionSpec
from metricflow.test.time.metric_time_dimension import MTD_SPEC_MONTH, MTD_SPEC_WEEK, MTD_SPEC_YEAR


@pytest.fixture(scope="session")
def dunder_naming_scheme() -> DunderNamingScheme:  # noqa: D
    return DunderNamingScheme()


def test_input_str(dunder_naming_scheme: DunderNamingScheme) -> None:  # noqa: D
    assert (
        dunder_naming_scheme.input_str(
            DimensionSpec(
                element_name="country",
                entity_links=(
                    EntityReference(element_name="booking"),
                    EntityReference(element_name="listing"),
                ),
            )
        )
        == "booking__listing__country"
    )

    assert (
        dunder_naming_scheme.input_str(
            TimeDimensionSpec(
                element_name="creation_time",
                entity_links=(EntityReference(element_name="booking"), EntityReference(element_name="listing")),
                time_granularity=TimeGranularity.MONTH,
                date_part=DatePart.DAY,
            )
        )
        is None
    )

    assert (
        dunder_naming_scheme.input_str(
            TimeDimensionSpec(
                element_name="creation_time",
                entity_links=(
                    EntityReference(element_name="booking"),
                    EntityReference(element_name="listing"),
                ),
                time_granularity=TimeGranularity.MONTH,
            )
        )
        == "booking__listing__creation_time__month"
    )

    assert (
        dunder_naming_scheme.input_str(
            EntitySpec(
                element_name="user",
                entity_links=(
                    EntityReference(element_name="booking"),
                    EntityReference(element_name="listing"),
                ),
            )
        )
        == "booking__listing__user"
    )


def test_input_follows_scheme(dunder_naming_scheme: DunderNamingScheme) -> None:  # noqa: D
    assert dunder_naming_scheme.input_str_follows_scheme("listing__country")
    assert dunder_naming_scheme.input_str_follows_scheme("listing__creation_time__month")
    assert dunder_naming_scheme.input_str_follows_scheme("booking__listing")
    assert not dunder_naming_scheme.input_str_follows_scheme("listing__creation_time__extract_month")
    assert not dunder_naming_scheme.input_str_follows_scheme("123")
    assert not dunder_naming_scheme.input_str_follows_scheme("TimeDimension('metric_time')")


def test_spec_pattern(  # noqa: D
    dunder_naming_scheme: DunderNamingScheme, specs: Sequence[LinkableInstanceSpec]
) -> None:
    assert tuple(dunder_naming_scheme.spec_pattern("listing__user__country").match(specs)) == (
        DimensionSpec(
            element_name="country",
            entity_links=(
                EntityReference(element_name="listing"),
                EntityReference(element_name="user"),
            ),
        ),
    )

    assert tuple(dunder_naming_scheme.spec_pattern("metric_time").match(specs)) == (
        MTD_SPEC_WEEK,
        MTD_SPEC_MONTH,
        MTD_SPEC_YEAR,
    )

    assert tuple(dunder_naming_scheme.spec_pattern("booking__listing__user").match(specs)) == (
        EntitySpec(
            element_name="user",
            entity_links=(
                EntityReference(element_name="booking"),
                EntityReference(element_name="listing"),
            ),
        ),
    )

    assert tuple(dunder_naming_scheme.spec_pattern("metric_time__month").match(specs)) == (MTD_SPEC_MONTH,)
