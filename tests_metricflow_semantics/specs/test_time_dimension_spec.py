from __future__ import annotations

import logging

from dbt_semantic_interfaces.references import EntityReference
from dbt_semantic_interfaces.type_enums import TimeGranularity
from metricflow_semantics.specs.time_dimension_spec import TimeDimensionSpec, TimeDimensionSpecField
from metricflow_semantics.time.granularity import ExpandedTimeGranularity

logger = logging.getLogger(__name__)


def test_comparison_key_excluding_time_grain() -> None:  # noqa: D103
    spec0 = TimeDimensionSpec(
        element_name="element0",
        entity_links=(EntityReference("entity0"),),
        time_granularity=ExpandedTimeGranularity.from_time_granularity(TimeGranularity.DAY),
    )

    spec1 = TimeDimensionSpec(
        element_name="element0",
        entity_links=(EntityReference("entity0"),),
        time_granularity=ExpandedTimeGranularity.from_time_granularity(TimeGranularity.MONTH),
    )
    assert spec0.comparison_key(exclude_fields=[]) != spec1.comparison_key(exclude_fields=[])
    assert spec0.comparison_key(exclude_fields=[]) != spec1.comparison_key((TimeDimensionSpecField.TIME_GRANULARITY,))
    assert spec0.comparison_key(exclude_fields=[TimeDimensionSpecField.TIME_GRANULARITY]) == spec1.comparison_key(
        exclude_fields=[TimeDimensionSpecField.TIME_GRANULARITY]
    )
    assert hash(spec0.comparison_key(exclude_fields=[TimeDimensionSpecField.TIME_GRANULARITY])) == hash(
        spec1.comparison_key(exclude_fields=[TimeDimensionSpecField.TIME_GRANULARITY])
    )

    assert (
        spec0.with_grain(ExpandedTimeGranularity.from_time_granularity(TimeGranularity.MONTH)).comparison_key()
        == spec1.comparison_key()
    )
