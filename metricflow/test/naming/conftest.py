from __future__ import annotations

from typing import Sequence

import pytest
from dbt_semantic_interfaces.references import EntityReference
from dbt_semantic_interfaces.type_enums import TimeGranularity
from dbt_semantic_interfaces.type_enums.date_part import DatePart

from metricflow.specs.specs import DimensionSpec, EntitySpec, LinkableInstanceSpec, TimeDimensionSpec
from metricflow.test.time.metric_time_dimension import MTD_SPEC_MONTH, MTD_SPEC_WEEK, MTD_SPEC_YEAR


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
    )
