from __future__ import annotations

import logging
from dataclasses import asdict
from typing import Sequence

import pytest
from dbt_semantic_interfaces.naming.keywords import METRIC_TIME_ELEMENT_NAME
from dbt_semantic_interfaces.references import EntityReference
from dbt_semantic_interfaces.type_enums import TimeGranularity
from dbt_semantic_interfaces.type_enums.date_part import DatePart
from metricflow_semantics.specs.dimension_spec import DimensionSpec
from metricflow_semantics.specs.entity_spec import EntitySpec
from metricflow_semantics.specs.group_by_metric_spec import GroupByMetricSpec
from metricflow_semantics.specs.instance_spec import LinkableInstanceSpec
from metricflow_semantics.specs.patterns.entity_link_pattern import (
    EntityLinkPattern,
    ParameterSetField,
    SpecPatternParameterSet,
)
from metricflow_semantics.specs.time_dimension_spec import TimeDimensionSpec
from metricflow_semantics.test_helpers.metric_time_dimension import MTD_SPEC_MONTH, MTD_SPEC_WEEK, MTD_SPEC_YEAR

logger = logging.getLogger(__name__)


@pytest.fixture(scope="session")
def specs() -> Sequence[LinkableInstanceSpec]:  # noqa: D103
    return (
        # Time dimensions
        MTD_SPEC_WEEK,
        MTD_SPEC_MONTH,
        MTD_SPEC_YEAR,
        TimeDimensionSpec(
            element_name="creation_time",
            entity_links=(EntityReference("booking"), EntityReference("listing")),
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
        # Group by metrics
        GroupByMetricSpec(
            element_name="bookings",
            entity_links=(EntityReference(element_name="listing"),),
            metric_subquery_entity_links=(EntityReference(element_name="listing"),),
        ),
    )


def test_valid_parameter_fields() -> None:
    """Tests that ParameterSetField.value maps to a valid field in SpecPatternParameterSet."""
    parameter_set = SpecPatternParameterSet.from_parameters(fields_to_compare=())
    parameter_set_dict = set(asdict(parameter_set).keys())
    for spec_field in ParameterSetField:
        assert spec_field.value in parameter_set_dict, f"{spec_field} is not a valid field for {parameter_set}"


def test_dimension_match(specs: Sequence[LinkableInstanceSpec]) -> None:  # noqa: D103
    pattern = EntityLinkPattern(
        SpecPatternParameterSet.from_parameters(
            element_name="is_instant",
            entity_links=(EntityReference(element_name="booking"),),
            fields_to_compare=(
                ParameterSetField.ELEMENT_NAME,
                ParameterSetField.ENTITY_LINKS,
            ),
        )
    )

    assert tuple(pattern.match(specs)) == (
        DimensionSpec(element_name="is_instant", entity_links=(EntityReference(element_name="booking"),)),
    )


def test_entity_match(specs: Sequence[LinkableInstanceSpec]) -> None:  # noqa: D103
    pattern = EntityLinkPattern(
        SpecPatternParameterSet.from_parameters(
            element_name="listing",
            entity_links=(EntityReference(element_name="booking"),),
            fields_to_compare=(
                ParameterSetField.ELEMENT_NAME,
                ParameterSetField.ENTITY_LINKS,
            ),
        )
    )

    assert tuple(pattern.match(specs)) == (
        EntitySpec(element_name="listing", entity_links=(EntityReference(element_name="booking"),)),
    )


def test_group_by_metric_match(specs: Sequence[LinkableInstanceSpec]) -> None:  # noqa: D103
    pattern = EntityLinkPattern(
        SpecPatternParameterSet.from_parameters(
            element_name="bookings",
            entity_links=(EntityReference(element_name="listing"),),
            fields_to_compare=(
                ParameterSetField.ELEMENT_NAME,
                ParameterSetField.ENTITY_LINKS,
            ),
        )
    )

    assert tuple(pattern.match(specs)) == (
        GroupByMetricSpec(
            element_name="bookings",
            entity_links=(EntityReference(element_name="listing"),),
            metric_subquery_entity_links=(EntityReference(element_name="listing"),),
        ),
    )


def test_time_dimension_match(specs: Sequence[LinkableInstanceSpec]) -> None:  # noqa: D103
    pattern = EntityLinkPattern(
        SpecPatternParameterSet.from_parameters(
            element_name=METRIC_TIME_ELEMENT_NAME,
            entity_links=(),
            time_granularity_name=TimeGranularity.WEEK.value,
            fields_to_compare=(
                ParameterSetField.ELEMENT_NAME,
                ParameterSetField.ENTITY_LINKS,
                ParameterSetField.TIME_GRANULARITY,
            ),
        )
    )

    assert tuple(pattern.match(specs)) == (MTD_SPEC_WEEK,)


def test_time_dimension_match_without_grain_specified(specs: Sequence[LinkableInstanceSpec]) -> None:  # noqa: D103
    pattern = EntityLinkPattern(
        SpecPatternParameterSet.from_parameters(
            element_name=METRIC_TIME_ELEMENT_NAME,
            entity_links=(),
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
        SpecPatternParameterSet.from_parameters(
            element_name="creation_time",
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
        SpecPatternParameterSet.from_parameters(
            element_name="creation_time",
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
            date_part=DatePart.YEAR,
        ),
    )
