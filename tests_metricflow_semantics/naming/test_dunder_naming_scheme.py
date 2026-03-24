from __future__ import annotations

from typing import Sequence

import pytest
from dbt_semantic_interfaces.references import EntityReference
from dbt_semantic_interfaces.type_enums import TimeGranularity
from dbt_semantic_interfaces.type_enums.date_part import DatePart
from metricflow_semantics.model.semantic_manifest_lookup import SemanticManifestLookup
from metricflow_semantics.naming.dunder_scheme import DunderNamingScheme
from metricflow_semantics.specs.dimension_spec import DimensionSpec
from metricflow_semantics.specs.entity_spec import EntitySpec
from metricflow_semantics.specs.instance_spec import LinkableInstanceSpec
from metricflow_semantics.specs.time_dimension_spec import TimeDimensionSpec
from metricflow_semantics.test_helpers.metric_time_dimension import MTD_SPEC_MONTH, MTD_SPEC_WEEK, MTD_SPEC_YEAR
from metricflow_semantics.time.granularity import ExpandedTimeGranularity


@pytest.fixture(scope="session")
def dunder_naming_scheme() -> DunderNamingScheme:  # noqa: D103
    return DunderNamingScheme()


def test_input_str(dunder_naming_scheme: DunderNamingScheme) -> None:  # noqa: D103
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
                time_granularity=ExpandedTimeGranularity.from_time_granularity(TimeGranularity.MONTH),
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


def test_input_follows_scheme(  # noqa: D103
    dunder_naming_scheme: DunderNamingScheme,
    simple_semantic_manifest_lookup: SemanticManifestLookup,
) -> None:
    assert dunder_naming_scheme.input_str_follows_scheme(
        "listing__country", semantic_manifest_lookup=simple_semantic_manifest_lookup
    )
    assert dunder_naming_scheme.input_str_follows_scheme(
        "listing__creation_time__month", semantic_manifest_lookup=simple_semantic_manifest_lookup
    )
    assert dunder_naming_scheme.input_str_follows_scheme(
        "booking__listing", semantic_manifest_lookup=simple_semantic_manifest_lookup
    )
    assert not dunder_naming_scheme.input_str_follows_scheme(
        "listing__creation_time__extract_month", semantic_manifest_lookup=simple_semantic_manifest_lookup
    )
    assert not dunder_naming_scheme.input_str_follows_scheme(
        "123", semantic_manifest_lookup=simple_semantic_manifest_lookup
    )
    assert not dunder_naming_scheme.input_str_follows_scheme(
        "TimeDimension('metric_time')", semantic_manifest_lookup=simple_semantic_manifest_lookup
    )


def test_spec_pattern(  # noqa: D103
    dunder_naming_scheme: DunderNamingScheme,
    specs: Sequence[LinkableInstanceSpec],
    simple_semantic_manifest_lookup: SemanticManifestLookup,
) -> None:  # noqa: D103
    assert tuple(
        dunder_naming_scheme.spec_pattern(
            "listing__user__country", semantic_manifest_lookup=simple_semantic_manifest_lookup
        ).match(specs)
    ) == (
        DimensionSpec(
            element_name="country",
            entity_links=(
                EntityReference(element_name="listing"),
                EntityReference(element_name="user"),
            ),
        ),
    )

    assert tuple(
        dunder_naming_scheme.spec_pattern(
            "metric_time", semantic_manifest_lookup=simple_semantic_manifest_lookup
        ).match(specs)
    ) == (
        MTD_SPEC_WEEK,
        MTD_SPEC_MONTH,
        MTD_SPEC_YEAR,
    )

    assert tuple(
        dunder_naming_scheme.spec_pattern(
            "booking__listing__user", semantic_manifest_lookup=simple_semantic_manifest_lookup
        ).match(specs)
    ) == (
        EntitySpec(
            element_name="user",
            entity_links=(
                EntityReference(element_name="booking"),
                EntityReference(element_name="listing"),
            ),
        ),
    )

    assert tuple(
        dunder_naming_scheme.spec_pattern(
            "metric_time__month", semantic_manifest_lookup=simple_semantic_manifest_lookup
        ).match(specs)
    ) == (MTD_SPEC_MONTH,)
