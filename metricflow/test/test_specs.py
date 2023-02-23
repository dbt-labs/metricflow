from typing import Sequence

import pytest

from metricflow.specs import (
    DimensionSpec,
    IdentifierSpec,
    InstanceSpec,
    LinkableInstanceSpec,
    TimeDimensionSpec,
    InstanceSpecSet,
    MetricSpec,
    MeasureSpec,
    LinklessIdentifierSpec,
    IdentifierReference,
)
from dbt.semantic.time import TimeGranularity


@pytest.fixture
def dimension_spec() -> DimensionSpec:  # noqa: D
    return DimensionSpec(
        name="platform",
        identifier_links=(
            IdentifierReference(name="user_id"),
            IdentifierReference(name="device_id"),
        ),
    )


@pytest.fixture
def time_dimension_spec() -> TimeDimensionSpec:  # noqa: D
    return TimeDimensionSpec(
        name="signup_ts",
        identifier_links=(IdentifierReference(name="user_id"),),
        time_granularity=TimeGranularity.DAY,
    )


@pytest.fixture
def identifier_spec() -> IdentifierSpec:  # noqa: D
    return IdentifierSpec(
        name="user_id",
        identifier_links=(IdentifierReference(name="listing_id"),),
    )


def test_merge_specs(dimension_spec: DimensionSpec, identifier_spec: IdentifierSpec) -> None:
    """Tests InstanceSpec.merge()"""
    assert InstanceSpec.merge([dimension_spec], [identifier_spec]) == [dimension_spec, identifier_spec]


def test_dimension_without_first_identifier_link(dimension_spec: DimensionSpec) -> None:  # noqa: D
    assert dimension_spec.without_first_identifier_link == DimensionSpec(
        name="platform", identifier_links=(IdentifierReference(name="device_id"),)
    )


def test_dimension_without_identifier_links(dimension_spec: DimensionSpec) -> None:  # noqa: D
    assert dimension_spec.without_identifier_links == DimensionSpec(name="platform", identifier_links=())


def test_time_dimension_without_first_identifier_link(time_dimension_spec: TimeDimensionSpec) -> None:  # noqa: D
    assert time_dimension_spec.without_first_identifier_link == TimeDimensionSpec(
        name="signup_ts",
        identifier_links=(),
        time_granularity=TimeGranularity.DAY,
    )


def test_time_dimension_without_identifier_links(time_dimension_spec: TimeDimensionSpec) -> None:  # noqa: D
    assert time_dimension_spec.without_identifier_links == TimeDimensionSpec(
        name="signup_ts",
        identifier_links=(),
        time_granularity=time_dimension_spec.time_granularity,
    )


def test_identifier_without_first_identifier_link(identifier_spec: IdentifierSpec) -> None:  # noqa: D
    assert identifier_spec.without_first_identifier_link == IdentifierSpec(
        name="user_id",
        identifier_links=(),
    )


def test_identifier_without_identifier_links(identifier_spec: IdentifierSpec) -> None:  # noqa: D
    assert identifier_spec.without_identifier_links == IdentifierSpec(
        name="user_id",
        identifier_links=(),
    )


def test_merge_linkable_specs(dimension_spec: DimensionSpec, identifier_spec: IdentifierSpec) -> None:  # noqa: D
    linkable_specs: Sequence[LinkableInstanceSpec] = [dimension_spec, identifier_spec]

    assert LinkableInstanceSpec.merge_linkable_specs([dimension_spec], [identifier_spec]) == linkable_specs


def test_qualified_name() -> None:  # noqa: D
    assert (
        DimensionSpec(name="country", identifier_links=(IdentifierReference("listing_id"),)).qualified_name
        == "listing_id__country"
    )


def test_merge_spec_set() -> None:  # noqa: D
    spec_set1 = InstanceSpecSet(metric_specs=(MetricSpec(name="bookings"),))
    spec_set2 = InstanceSpecSet(dimension_specs=(DimensionSpec(name="is_instant", identifier_links=()),))

    assert InstanceSpecSet.merge((spec_set1, spec_set2)) == InstanceSpecSet(
        metric_specs=(MetricSpec(name="bookings"),),
        dimension_specs=(DimensionSpec(name="is_instant", identifier_links=()),),
    )


@pytest.fixture
def spec_set() -> InstanceSpecSet:  # noqa: D
    return InstanceSpecSet(
        metric_specs=(MetricSpec(name="bookings"),),
        measure_specs=(
            MeasureSpec(
                name="bookings",
            ),
        ),
        dimension_specs=(DimensionSpec(name="is_instant", identifier_links=()),),
        time_dimension_specs=(
            TimeDimensionSpec(
                name="ds",
                identifier_links=(),
                time_granularity=TimeGranularity.DAY,
            ),
        ),
        identifier_specs=(
            IdentifierSpec(
                name="user_id",
                identifier_links=(IdentifierReference(name="listing_id"),),
            ),
        ),
    )


def test_spec_set_linkable_specs(spec_set: InstanceSpecSet) -> None:  # noqa: D
    assert set(spec_set.linkable_specs) == {
        DimensionSpec(name="is_instant", identifier_links=()),
        TimeDimensionSpec(
            name="ds",
            identifier_links=(),
            time_granularity=TimeGranularity.DAY,
        ),
        IdentifierSpec(
            name="user_id",
            identifier_links=(IdentifierReference(name="listing_id"),),
        ),
    }


def test_spec_set_all_specs(spec_set: InstanceSpecSet) -> None:  # noqa: D
    assert set(spec_set.all_specs) == {
        MetricSpec(name="bookings"),
        MeasureSpec(
            name="bookings",
        ),
        DimensionSpec(name="is_instant", identifier_links=()),
        TimeDimensionSpec(
            name="ds",
            identifier_links=(),
            time_granularity=TimeGranularity.DAY,
        ),
        IdentifierSpec(
            name="user_id",
            identifier_links=(IdentifierReference(name="listing_id"),),
        ),
    }


def test_linkless_identifier() -> None:  # noqa: D
    """Check that equals and hash works as expected for the LinklessIdentifierSpec / IdentifierSpec"""
    identifier_spec = IdentifierSpec(name="user_id", identifier_links=())
    linkless_identifier_spec = LinklessIdentifierSpec.from_name("user_id")

    # Check equality between the two.
    assert identifier_spec == identifier_spec
    assert linkless_identifier_spec == linkless_identifier_spec
    assert identifier_spec == linkless_identifier_spec

    # Check that they are treated equivalently in sets.
    set_with_identifier_spec = {identifier_spec}
    assert identifier_spec in set_with_identifier_spec
    assert linkless_identifier_spec in set_with_identifier_spec

    set_with_linkless_identifier_spec = {linkless_identifier_spec}
    assert identifier_spec in set_with_linkless_identifier_spec
    assert linkless_identifier_spec in set_with_linkless_identifier_spec

    set_with_identifier_spec.add(linkless_identifier_spec)
    assert len(set_with_identifier_spec) == 1
