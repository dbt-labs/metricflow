from __future__ import annotations

from typing import Sequence

import pytest
from dbt_semantic_interfaces.type_enums.time_granularity import TimeGranularity

from metricflow.specs.specs import (
    DimensionSpec,
    EntityReference,
    EntitySpec,
    InstanceSpec,
    InstanceSpecSet,
    LinkableInstanceSpec,
    LinklessEntitySpec,
    MeasureSpec,
    MetricSpec,
    TimeDimensionSpec,
)


@pytest.fixture
def dimension_spec() -> DimensionSpec:  # noqa: D
    return DimensionSpec(
        element_name="platform",
        entity_links=(
            EntityReference(element_name="user_id"),
            EntityReference(element_name="device_id"),
        ),
    )


@pytest.fixture
def time_dimension_spec() -> TimeDimensionSpec:  # noqa: D
    return TimeDimensionSpec(
        element_name="signup_ts",
        entity_links=(EntityReference(element_name="user_id"),),
        time_granularity=TimeGranularity.DAY,
    )


@pytest.fixture
def entity_spec() -> EntitySpec:  # noqa: D
    return EntitySpec(
        element_name="user_id",
        entity_links=(EntityReference(element_name="listing_id"),),
    )


def test_merge_specs(dimension_spec: DimensionSpec, entity_spec: EntitySpec) -> None:
    """Tests InstanceSpec.merge()."""
    assert InstanceSpec.merge([dimension_spec], [entity_spec]) == [dimension_spec, entity_spec]


def test_dimension_without_first_entity_link(dimension_spec: DimensionSpec) -> None:  # noqa: D
    assert dimension_spec.without_first_entity_link == DimensionSpec(
        element_name="platform", entity_links=(EntityReference(element_name="device_id"),)
    )


def test_dimension_without_entity_links(dimension_spec: DimensionSpec) -> None:  # noqa: D
    assert dimension_spec.without_entity_links == DimensionSpec(element_name="platform", entity_links=())


def test_time_dimension_without_first_entity_link(time_dimension_spec: TimeDimensionSpec) -> None:  # noqa: D
    assert time_dimension_spec.without_first_entity_link == TimeDimensionSpec(
        element_name="signup_ts",
        entity_links=(),
        time_granularity=TimeGranularity.DAY,
    )


def test_time_dimension_without_entity_links(time_dimension_spec: TimeDimensionSpec) -> None:  # noqa: D
    assert time_dimension_spec.without_entity_links == TimeDimensionSpec(
        element_name="signup_ts",
        entity_links=(),
        time_granularity=time_dimension_spec.time_granularity,
    )


def test_entity_without_first_entity_link(entity_spec: EntitySpec) -> None:  # noqa: D
    assert entity_spec.without_first_entity_link == EntitySpec(
        element_name="user_id",
        entity_links=(),
    )


def test_entity_without_entity_links(entity_spec: EntitySpec) -> None:  # noqa: D
    assert entity_spec.without_entity_links == EntitySpec(
        element_name="user_id",
        entity_links=(),
    )


def test_merge_linkable_specs(dimension_spec: DimensionSpec, entity_spec: EntitySpec) -> None:  # noqa: D
    linkable_specs: Sequence[LinkableInstanceSpec] = [dimension_spec, entity_spec]

    assert LinkableInstanceSpec.merge_linkable_specs([dimension_spec], [entity_spec]) == linkable_specs


def test_qualified_name() -> None:  # noqa: D
    assert (
        DimensionSpec(element_name="country", entity_links=(EntityReference("listing_id"),)).qualified_name
        == "listing_id__country"
    )


def test_merge_spec_set() -> None:  # noqa: D
    spec_set1 = InstanceSpecSet(metric_specs=(MetricSpec(element_name="bookings"),))
    spec_set2 = InstanceSpecSet(
        dimension_specs=(DimensionSpec(element_name="is_instant", entity_links=(EntityReference("booking"),)),)
    )

    assert InstanceSpecSet.merge((spec_set1, spec_set2)) == InstanceSpecSet(
        metric_specs=(MetricSpec(element_name="bookings"),),
        dimension_specs=(DimensionSpec(element_name="is_instant", entity_links=(EntityReference("booking"),)),),
    )


@pytest.fixture
def spec_set() -> InstanceSpecSet:  # noqa: D
    return InstanceSpecSet(
        metric_specs=(MetricSpec(element_name="bookings"),),
        measure_specs=(
            MeasureSpec(
                element_name="bookings",
            ),
        ),
        dimension_specs=(DimensionSpec(element_name="is_instant", entity_links=(EntityReference("booking"),)),),
        time_dimension_specs=(
            TimeDimensionSpec(
                element_name="ds",
                entity_links=(),
                time_granularity=TimeGranularity.DAY,
            ),
        ),
        entity_specs=(
            EntitySpec(
                element_name="user_id",
                entity_links=(EntityReference(element_name="listing_id"),),
            ),
        ),
    )


def test_spec_set_linkable_specs(spec_set: InstanceSpecSet) -> None:  # noqa: D
    assert set(spec_set.linkable_specs) == {
        DimensionSpec(element_name="is_instant", entity_links=(EntityReference("booking"),)),
        TimeDimensionSpec(
            element_name="ds",
            entity_links=(),
            time_granularity=TimeGranularity.DAY,
        ),
        EntitySpec(
            element_name="user_id",
            entity_links=(EntityReference(element_name="listing_id"),),
        ),
    }


def test_spec_set_all_specs(spec_set: InstanceSpecSet) -> None:  # noqa: D
    assert set(spec_set.all_specs) == {
        MetricSpec(element_name="bookings"),
        MeasureSpec(
            element_name="bookings",
        ),
        DimensionSpec(element_name="is_instant", entity_links=(EntityReference("booking"),)),
        TimeDimensionSpec(
            element_name="ds",
            entity_links=(),
            time_granularity=TimeGranularity.DAY,
        ),
        EntitySpec(
            element_name="user_id",
            entity_links=(EntityReference(element_name="listing_id"),),
        ),
    }


def test_linkless_entity() -> None:  # noqa: D
    """Check that equals and hash works as expected for the LinklessEntitySpec / EntitySpec."""
    entity_spec = EntitySpec(element_name="user_id", entity_links=())
    linkless_entity_spec = LinklessEntitySpec.from_element_name("user_id")

    # Check equality between the two.
    assert entity_spec == entity_spec
    assert linkless_entity_spec == linkless_entity_spec
    assert entity_spec == linkless_entity_spec

    # Check that they are treated equivalently in sets.
    set_with_entity_spec = {entity_spec}
    assert entity_spec in set_with_entity_spec
    assert linkless_entity_spec in set_with_entity_spec

    set_with_linkless_entity_spec = {linkless_entity_spec}
    assert entity_spec in set_with_linkless_entity_spec
    assert linkless_entity_spec in set_with_linkless_entity_spec

    set_with_entity_spec.add(linkless_entity_spec)
    assert len(set_with_entity_spec) == 1
