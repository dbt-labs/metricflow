from __future__ import annotations

import logging
from typing import Sequence

import pytest
from dbt_semantic_interfaces.call_parameter_sets import (
    DimensionCallParameterSet,
    EntityCallParameterSet,
    MetricCallParameterSet,
    TimeDimensionCallParameterSet,
)
from dbt_semantic_interfaces.references import (
    DimensionReference,
    EntityReference,
    MetricReference,
    TimeDimensionReference,
)
from dbt_semantic_interfaces.type_enums import TimeGranularity
from dbt_semantic_interfaces.type_enums.date_part import DatePart
from metricflow_semantics.specs.dimension_spec import DimensionSpec
from metricflow_semantics.specs.entity_spec import EntitySpec
from metricflow_semantics.specs.group_by_metric_spec import GroupByMetricSpec
from metricflow_semantics.specs.instance_spec import LinkableInstanceSpec
from metricflow_semantics.specs.patterns.typed_patterns import (
    DimensionPattern,
    EntityPattern,
    GroupByMetricPattern,
    TimeDimensionPattern,
)
from metricflow_semantics.specs.time_dimension_spec import TimeDimensionSpec
from metricflow_semantics.time.granularity import ExpandedTimeGranularity

logger = logging.getLogger(__name__)


@pytest.fixture(scope="session")
def specs() -> Sequence[LinkableInstanceSpec]:  # noqa: D103
    return (
        # Time dimensions
        TimeDimensionSpec(
            element_name="common_name",
            entity_links=(EntityReference("booking"), EntityReference("listing")),
            time_granularity=ExpandedTimeGranularity.from_time_granularity(TimeGranularity.DAY),
        ),
        TimeDimensionSpec(
            element_name="common_name",
            entity_links=(EntityReference("booking"), EntityReference("listing")),
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
        # Group by metrics
        GroupByMetricSpec(
            element_name="bookings",
            entity_links=(EntityReference(element_name="listing"),),
            metric_subquery_entity_links=(EntityReference("listing"),),
        ),
    )


def test_dimension_pattern(specs: Sequence[LinkableInstanceSpec]) -> None:  # noqa: D103
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
            time_granularity=ExpandedTimeGranularity.from_time_granularity(TimeGranularity.DAY),
        ),
    )


def test_time_dimension_pattern(specs: Sequence[LinkableInstanceSpec]) -> None:  # noqa: D103
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
            time_granularity=ExpandedTimeGranularity.from_time_granularity(TimeGranularity.DAY),
        ),
    )


def test_time_dimension_pattern_with_date_part(specs: Sequence[LinkableInstanceSpec]) -> None:  # noqa: D103
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
            date_part=DatePart.MONTH,
        ),
    )


def test_entity_pattern(specs: Sequence[LinkableInstanceSpec]) -> None:  # noqa: D103
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


def test_group_by_metric_pattern(specs: Sequence[LinkableInstanceSpec]) -> None:  # noqa: D103
    pattern = GroupByMetricPattern.from_call_parameter_set(
        MetricCallParameterSet(
            group_by=(EntityReference("listing"),),
            metric_reference=MetricReference(element_name="bookings"),
        )
    )

    assert tuple(pattern.match(specs)) == (
        GroupByMetricSpec(
            element_name="bookings",
            entity_links=(EntityReference("listing"),),
            metric_subquery_entity_links=(EntityReference("listing"),),
        ),
    )
