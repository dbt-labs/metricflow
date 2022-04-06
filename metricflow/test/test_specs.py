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
)
from metricflow.time.time_granularity import TimeGranularity


@pytest.fixture
def dimension_spec() -> DimensionSpec:  # noqa: D
    return DimensionSpec(
        element_name="platform",
        identifier_links=(
            LinklessIdentifierSpec.from_element_name(element_name="user_id"),
            LinklessIdentifierSpec.from_element_name(element_name="device_id"),
        ),
    )


@pytest.fixture
def time_dimension_spec() -> TimeDimensionSpec:  # noqa: D
    return TimeDimensionSpec(
        element_name="signup_ts",
        identifier_links=(LinklessIdentifierSpec.from_element_name(element_name="user_id"),),
        time_granularity=TimeGranularity.DAY,
    )


@pytest.fixture
def identifier_spec() -> IdentifierSpec:  # noqa: D
    return IdentifierSpec(
        element_name="user_id",
        identifier_links=(LinklessIdentifierSpec.from_element_name(element_name="listing_id"),),
    )


def test_merge_specs(dimension_spec: DimensionSpec, identifier_spec: IdentifierSpec) -> None:
    """Tests InstanceSpec.merge()"""
    assert InstanceSpec.merge([dimension_spec], [identifier_spec]) == [dimension_spec, identifier_spec]


def test_dimension_without_first_identifier_link(dimension_spec: DimensionSpec) -> None:  # noqa: D
    assert dimension_spec.without_first_identifier_link() == DimensionSpec(
        element_name="platform", identifier_links=(LinklessIdentifierSpec.from_element_name(element_name="device_id"),)
    )


def test_dimension_without_identifier_links(dimension_spec: DimensionSpec) -> None:  # noqa: D
    assert dimension_spec.without_identifier_links() == DimensionSpec(element_name="platform", identifier_links=())


def test_time_dimension_without_first_identifier_link(time_dimension_spec: TimeDimensionSpec) -> None:  # noqa: D
    assert time_dimension_spec.without_first_identifier_link() == TimeDimensionSpec(
        element_name="signup_ts",
        identifier_links=(),
        time_granularity=TimeGranularity.DAY,
    )


def test_time_dimension_without_identifier_links(time_dimension_spec: TimeDimensionSpec) -> None:  # noqa: D
    assert time_dimension_spec.without_identifier_links() == LinklessIdentifierSpec(
        element_name="signup_ts",
        identifier_links=(),
    )


def test_identifier_without_first_identifier_link(identifier_spec: IdentifierSpec) -> None:  # noqa: D
    assert identifier_spec.without_first_identifier_link() == IdentifierSpec(
        element_name="user_id",
        identifier_links=(),
    )


def test_identifier_without_identifier_links(identifier_spec: IdentifierSpec) -> None:  # noqa: D
    assert identifier_spec.without_identifier_links() == IdentifierSpec(
        element_name="user_id",
        identifier_links=(),
    )


def test_merge_linkable_specs(dimension_spec: DimensionSpec, identifier_spec: IdentifierSpec) -> None:  # noqa: D
    linkable_specs: Sequence[LinkableInstanceSpec] = [dimension_spec, identifier_spec]

    assert LinkableInstanceSpec.merge_linkable_specs([dimension_spec], [identifier_spec]) == linkable_specs


def test_qualified_name() -> None:  # noqa: D
    assert (
        DimensionSpec(
            element_name="country", identifier_links=(LinklessIdentifierSpec.from_element_name("listing_id"),)
        ).qualified_name
        == "listing_id__country"
    )


def test_merge_spec_set() -> None:  # noqa: D
    spec_set1 = InstanceSpecSet(metric_specs=(MetricSpec(element_name="bookings"),))
    spec_set2 = InstanceSpecSet(dimension_specs=(DimensionSpec(element_name="is_instant", identifier_links=()),))

    assert spec_set1.merge([spec_set2]) == InstanceSpecSet(
        metric_specs=(MetricSpec(element_name="bookings"),),
        dimension_specs=(DimensionSpec(element_name="is_instant", identifier_links=()),),
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
        dimension_specs=(DimensionSpec(element_name="is_instant", identifier_links=()),),
        time_dimension_specs=(
            TimeDimensionSpec(
                element_name="ds",
                identifier_links=(),
                time_granularity=TimeGranularity.DAY,
            ),
        ),
        identifier_specs=(
            IdentifierSpec(
                element_name="user_id",
                identifier_links=(LinklessIdentifierSpec.from_element_name(element_name="listing_id"),),
            ),
        ),
    )


def test_spec_set_linkable_specs(spec_set: InstanceSpecSet) -> None:  # noqa: D
    assert set(spec_set.linkable_specs) == {
        DimensionSpec(element_name="is_instant", identifier_links=()),
        TimeDimensionSpec(
            element_name="ds",
            identifier_links=(),
            time_granularity=TimeGranularity.DAY,
        ),
        IdentifierSpec(
            element_name="user_id",
            identifier_links=(LinklessIdentifierSpec.from_element_name(element_name="listing_id"),),
        ),
    }


def test_spec_set_all_specs(spec_set: InstanceSpecSet) -> None:  # noqa: D
    assert set(spec_set.all_specs) == {
        MetricSpec(element_name="bookings"),
        MeasureSpec(
            element_name="bookings",
        ),
        DimensionSpec(element_name="is_instant", identifier_links=()),
        TimeDimensionSpec(
            element_name="ds",
            identifier_links=(),
            time_granularity=TimeGranularity.DAY,
        ),
        IdentifierSpec(
            element_name="user_id",
            identifier_links=(LinklessIdentifierSpec.from_element_name(element_name="listing_id"),),
        ),
    }


def test_linkless_identifier() -> None:  # noqa: D
    """Check that equals and hash works as expected for the LinklessIdentifierSpec / IdentifierSpec"""
    identifier_spec = IdentifierSpec(element_name="user_id", identifier_links=())
    linkless_identifier_spec = LinklessIdentifierSpec.from_element_name("user_id")

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
