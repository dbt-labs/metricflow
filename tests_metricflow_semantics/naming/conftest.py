from __future__ import annotations

from typing import Sequence

import pytest
from dbt_semantic_interfaces.references import EntityReference
from dbt_semantic_interfaces.type_enums.date_part import DatePart
from metricflow_semantics.specs.dimension_spec import DimensionSpec
from metricflow_semantics.specs.entity_spec import EntitySpec
from metricflow_semantics.specs.group_by_metric_spec import GroupByMetricSpec
from metricflow_semantics.specs.instance_spec import LinkableInstanceSpec
from metricflow_semantics.specs.time_dimension_spec import TimeDimensionSpec
from metricflow_semantics.test_helpers.metric_time_dimension import MTD_SPEC_MONTH, MTD_SPEC_WEEK, MTD_SPEC_YEAR


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
            date_part=DatePart.DAY,
        ),
        # Dimensions
        DimensionSpec(
            element_name="country",
            entity_links=(
                EntityReference(element_name="listing"),
                EntityReference(element_name="user"),
            ),
        ),
        DimensionSpec(
            element_name="country",
            entity_links=(
                EntityReference(element_name="booking"),
                EntityReference(element_name="listing"),
            ),
        ),
        DimensionSpec(element_name="is_instant", entity_links=(EntityReference(element_name="booking"),)),
        # Entities
        EntitySpec(
            element_name="listing",
            entity_links=(EntityReference(element_name="booking"),),
        ),
        EntitySpec(
            element_name="user",
            entity_links=(EntityReference(element_name="booking"), EntityReference(element_name="listing")),
        ),
        # GroupByMetrics
        GroupByMetricSpec(
            element_name="bookings",
            entity_links=(EntityReference(element_name="listing"),),
            metric_subquery_entity_links=(EntityReference(element_name="listing"),),
        ),
    )
