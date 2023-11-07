from __future__ import annotations

import logging

from dbt_semantic_interfaces.references import EntityReference
from dbt_semantic_interfaces.type_enums import TimeGranularity

from metricflow.specs.specs import TimeDimensionSpec, TimeDimensionSpecField

logger = logging.getLogger(__name__)


def test_comparison_key_excluding_time_grain() -> None:  # noqa: D
    spec0 = TimeDimensionSpec(
        element_name="element0",
        entity_links=(EntityReference("entity0"),),
        time_granularity=TimeGranularity.DAY,
    )

    spec1 = TimeDimensionSpec(
        element_name="element0",
        entity_links=(EntityReference("entity0"),),
        time_granularity=TimeGranularity.MONTH,
    )
    assert spec0.comparison_key(exclude_fields=[]) != spec1.comparison_key(exclude_fields=[])
    assert spec0.comparison_key(exclude_fields=[]) != spec1.comparison_key((TimeDimensionSpecField.TIME_GRANULARITY,))
    assert spec0.comparison_key(exclude_fields=[TimeDimensionSpecField.TIME_GRANULARITY]) == spec1.comparison_key(
        exclude_fields=[TimeDimensionSpecField.TIME_GRANULARITY]
    )
    assert hash(spec0.comparison_key(exclude_fields=[TimeDimensionSpecField.TIME_GRANULARITY])) == hash(
        spec1.comparison_key(exclude_fields=[TimeDimensionSpecField.TIME_GRANULARITY])
    )

    assert spec0.with_grain(TimeGranularity.MONTH).comparison_key() == spec1.comparison_key()
