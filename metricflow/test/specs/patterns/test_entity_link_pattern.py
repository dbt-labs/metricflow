from __future__ import annotations

import logging
from dataclasses import asdict
from typing import Sequence

import pytest
from dbt_semantic_interfaces.naming.keywords import METRIC_TIME_ELEMENT_NAME
from dbt_semantic_interfaces.references import EntityReference
from dbt_semantic_interfaces.type_enums import TimeGranularity
from dbt_semantic_interfaces.type_enums.date_part import DatePart

from metricflow.specs.patterns.entity_link_pattern import (
    EntityLinkPattern,
    EntityLinkPatternParameterSet,
    ParameterSetField,
)
from metricflow.specs.specs import DimensionSpec, EntitySpec, LinkableInstanceSpec, TimeDimensionSpec
from metricflow.test.time.metric_time_dimension import MTD_SPEC_MONTH, MTD_SPEC_WEEK, MTD_SPEC_YEAR

logger = logging.getLogger(__name__)


@pytest.fixture(scope="session")
def specs() -> Sequence[LinkableInstanceSpec]:  # noqa: D
    return (
        # Time dimensions
        MTD_SPEC_WEEK,
        MTD_SPEC_MONTH,
        MTD_SPEC_YEAR,
        TimeDimensionSpec(
            element_name="creation_time",
            entity_links=(EntityReference("booking"), EntityReference("listing")),
            time_granularity=TimeGranularity.MONTH,
            date_part=DatePart.YEAR,
        ),
        # Dimensions
        DimensionSpec(
            element_name="country",
            entity_links=(
                EntityReference(element_name="listing"),
                EntityReference(element_name="user"),
            ),
        ),
        DimensionSpec(element_name="is_instant", entity_links=(EntityReference(element_name="booking"),)),
        # Entities
        EntitySpec(
            element_name="listing",
            entity_links=(EntityReference(element_name="booking"),),
        ),
        EntitySpec(
            element_name="host",
            entity_links=(EntityReference(element_name="booking"),),
        ),
    )


def test_valid_parameter_fields() -> None:
    """Tests that ParameterSetField.value maps to a valid field in EntityLinkPatternParameterSet."""
    parameter_set = EntityLinkPatternParameterSet.from_parameters(
        fields_to_compare=(),
        element_name=None,
        entity_links=None,
        time_granularity=None,
        date_part=None,
    )
    parameter_set_dict = set(asdict(parameter_set).keys())
    for spec_field in ParameterSetField:
        assert spec_field.value in parameter_set_dict, f"{spec_field} is not a valid field for {parameter_set}"


def test_dimension_match(specs: Sequence[LinkableInstanceSpec]) -> None:  # noqa: D
    pattern = EntityLinkPattern(
        EntityLinkPatternParameterSet.from_parameters(
            element_name="is_instant",
            entity_links=(EntityReference(element_name="booking"),),
            time_granularity=None,
            date_part=None,
            fields_to_compare=(
                ParameterSetField.ELEMENT_NAME,
                ParameterSetField.ENTITY_LINKS,
            ),
        )
    )

    assert tuple(pattern.match(specs)) == (
        DimensionSpec(element_name="is_instant", entity_links=(EntityReference(element_name="booking"),)),
    )


def test_entity_match(specs: Sequence[LinkableInstanceSpec]) -> None:  # noqa: D
    pattern = EntityLinkPattern(
        EntityLinkPatternParameterSet.from_parameters(
            element_name="listing",
            entity_links=(EntityReference(element_name="booking"),),
            time_granularity=None,
            date_part=None,
            fields_to_compare=(
                ParameterSetField.ELEMENT_NAME,
                ParameterSetField.ENTITY_LINKS,
            ),
        )
    )

    assert tuple(pattern.match(specs)) == (
        EntitySpec(element_name="listing", entity_links=(EntityReference(element_name="booking"),)),
    )


def test_time_dimension_match(specs: Sequence[LinkableInstanceSpec]) -> None:  # noqa: D
    pattern = EntityLinkPattern(
        EntityLinkPatternParameterSet.from_parameters(
            element_name=METRIC_TIME_ELEMENT_NAME,
            entity_links=(),
            time_granularity=TimeGranularity.WEEK,
            date_part=None,
            fields_to_compare=(
                ParameterSetField.ELEMENT_NAME,
                ParameterSetField.ENTITY_LINKS,
                ParameterSetField.TIME_GRANULARITY,
            ),
        )
    )

    assert tuple(pattern.match(specs)) == (MTD_SPEC_WEEK,)


def test_time_dimension_match_without_grain_specified(specs: Sequence[LinkableInstanceSpec]) -> None:  # noqa: D
    pattern = EntityLinkPattern(
        EntityLinkPatternParameterSet.from_parameters(
            element_name=METRIC_TIME_ELEMENT_NAME,
            entity_links=(),
            time_granularity=None,
            date_part=None,
            fields_to_compare=(
                ParameterSetField.ELEMENT_NAME,
                ParameterSetField.ENTITY_LINKS,
            ),
        )
    )

    assert tuple(pattern.match(specs)) == (
        MTD_SPEC_WEEK,
        MTD_SPEC_MONTH,
        MTD_SPEC_YEAR,
    )


def test_time_dimension_date_part_mismatch(specs: Sequence[LinkableInstanceSpec]) -> None:
    """Checks that a None for the date_part field does not match to a non-None value."""
    pattern = EntityLinkPattern(
        EntityLinkPatternParameterSet.from_parameters(
            element_name="creation_time",
            entity_links=None,
            time_granularity=None,
            date_part=None,
            fields_to_compare=(
                ParameterSetField.ELEMENT_NAME,
                ParameterSetField.DATE_PART,
            ),
        )
    )

    assert tuple(pattern.match(specs)) == ()


def test_time_dimension_date_part_match(specs: Sequence[LinkableInstanceSpec]) -> None:
    """Checks that a correct date_part field produces a match."""
    pattern = EntityLinkPattern(
        EntityLinkPatternParameterSet.from_parameters(
            element_name="creation_time",
            entity_links=None,
            time_granularity=None,
            date_part=DatePart.YEAR,
            fields_to_compare=(
                ParameterSetField.ELEMENT_NAME,
                ParameterSetField.DATE_PART,
            ),
        )
    )

    assert tuple(pattern.match(specs)) == (
        TimeDimensionSpec(
            element_name="creation_time",
            entity_links=(EntityReference("booking"), EntityReference("listing")),
            time_granularity=TimeGranularity.MONTH,
            date_part=DatePart.YEAR,
        ),
    )
