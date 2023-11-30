from __future__ import annotations

import logging
from typing import Sequence

import pytest
from dbt_semantic_interfaces.call_parameter_sets import (
    DimensionCallParameterSet,
    EntityCallParameterSet,
    TimeDimensionCallParameterSet,
)
from dbt_semantic_interfaces.references import DimensionReference, EntityReference, TimeDimensionReference
from dbt_semantic_interfaces.type_enums import TimeGranularity
from dbt_semantic_interfaces.type_enums.date_part import DatePart

from metricflow.specs.patterns.typed_patterns import DimensionPattern, EntityPattern, TimeDimensionPattern
from metricflow.specs.specs import DimensionSpec, EntitySpec, LinkableInstanceSpec, TimeDimensionSpec

logger = logging.getLogger(__name__)


@pytest.fixture(scope="session")
def specs() -> Sequence[LinkableInstanceSpec]:  # noqa: D
    return (
        # Time dimensions
        TimeDimensionSpec(
            element_name="common_name",
            entity_links=(EntityReference("booking"), EntityReference("listing")),
            time_granularity=TimeGranularity.DAY,
            date_part=None,
        ),
        TimeDimensionSpec(
            element_name="common_name",
            entity_links=(EntityReference("booking"), EntityReference("listing")),
            time_granularity=TimeGranularity.DAY,
            date_part=DatePart.MONTH,
        ),
        # Dimensions
        DimensionSpec(
            element_name="common_name",
            entity_links=((EntityReference("booking"), EntityReference("listing"))),
        ),
        # Entities
        EntitySpec(
            element_name="common_name",
            entity_links=(EntityReference("booking"), EntityReference("listing")),
        ),
    )


def test_dimension_pattern(specs: Sequence[LinkableInstanceSpec]) -> None:  # noqa: D
    pattern = DimensionPattern.from_call_parameter_set(
        DimensionCallParameterSet(
            entity_path=(EntityReference("booking"), EntityReference("listing")),
            dimension_reference=DimensionReference(element_name="common_name"),
        )
    )

    assert tuple(pattern.match(specs)) == (
        DimensionSpec(
            element_name="common_name",
            entity_links=((EntityReference("booking"), EntityReference("listing"))),
        ),
        TimeDimensionSpec(
            element_name="common_name",
            entity_links=(EntityReference("booking"), EntityReference("listing")),
            time_granularity=TimeGranularity.DAY,
            date_part=None,
        ),
        TimeDimensionSpec(
            element_name="common_name",
            entity_links=(EntityReference("booking"), EntityReference("listing")),
            time_granularity=TimeGranularity.DAY,
            date_part=DatePart.MONTH,
        ),
    )


def test_time_dimension_pattern(specs: Sequence[LinkableInstanceSpec]) -> None:  # noqa: D
    pattern = TimeDimensionPattern.from_call_parameter_set(
        TimeDimensionCallParameterSet(
            entity_path=(EntityReference("booking"), EntityReference("listing")),
            time_dimension_reference=TimeDimensionReference(element_name="common_name"),
        )
    )

    assert tuple(pattern.match(specs)) == (
        TimeDimensionSpec(
            element_name="common_name",
            entity_links=(EntityReference("booking"), EntityReference("listing")),
            time_granularity=TimeGranularity.DAY,
            date_part=None,
        ),
    )


def test_time_dimension_pattern_with_date_part(specs: Sequence[LinkableInstanceSpec]) -> None:  # noqa: D
    pattern = TimeDimensionPattern.from_call_parameter_set(
        TimeDimensionCallParameterSet(
            entity_path=(EntityReference("booking"), EntityReference("listing")),
            time_dimension_reference=TimeDimensionReference(element_name="common_name"),
            date_part=DatePart.MONTH,
        )
    )

    assert tuple(pattern.match(specs)) == (
        TimeDimensionSpec(
            element_name="common_name",
            entity_links=(EntityReference("booking"), EntityReference("listing")),
            time_granularity=TimeGranularity.DAY,
            date_part=DatePart.MONTH,
        ),
    )


def test_entity_pattern(specs: Sequence[LinkableInstanceSpec]) -> None:  # noqa: D
    pattern = EntityPattern.from_call_parameter_set(
        EntityCallParameterSet(
            entity_path=(EntityReference("booking"), EntityReference("listing")),
            entity_reference=EntityReference(element_name="common_name"),
        )
    )

    assert tuple(pattern.match(specs)) == (
        EntitySpec(
            element_name="common_name",
            entity_links=(EntityReference("booking"), EntityReference("listing")),
        ),
    )
