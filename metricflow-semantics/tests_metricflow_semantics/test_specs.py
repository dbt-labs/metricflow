from __future__ import annotations

from typing import Sequence

import pytest
from dbt_semantic_interfaces.references import EntityReference
from dbt_semantic_interfaces.type_enums.time_granularity import TimeGranularity
from metricflow_semantics.semantic_graph.attribute_resolution.group_by_item_set import (
    GroupByItemSet,
)
from metricflow_semantics.specs.dimension_spec import DimensionSpec
from metricflow_semantics.specs.entity_spec import EntitySpec, LinklessEntitySpec
from metricflow_semantics.specs.group_by_metric_spec import GroupByMetricSpec
from metricflow_semantics.specs.instance_spec import InstanceSpec, LinkableInstanceSpec
from metricflow_semantics.specs.metric_spec import MetricSpec
from metricflow_semantics.specs.simple_metric_input_spec import SimpleMetricInputSpec
from metricflow_semantics.specs.spec_set import InstanceSpecSet
from metricflow_semantics.specs.time_dimension_spec import TimeDimensionSpec
from metricflow_semantics.specs.where_filter.where_filter_spec import WhereFilterSpec
from metricflow_semantics.specs.where_filter.where_filter_spec_set import WhereFilterSpecSet
from metricflow_semantics.sql.sql_bind_parameters import SqlBindParameterSet
from metricflow_semantics.time.granularity import ExpandedTimeGranularity


@pytest.fixture
def dimension_spec() -> DimensionSpec:  # noqa: D103
    return DimensionSpec(
        element_name="platform",
        entity_links=(
            EntityReference(element_name="user_id"),
            EntityReference(element_name="device_id"),
        ),
    )


@pytest.fixture
def time_dimension_spec() -> TimeDimensionSpec:  # noqa: D103
    return TimeDimensionSpec(
        element_name="signup_ts",
        entity_links=(EntityReference(element_name="user_id"),),
        time_granularity=ExpandedTimeGranularity.from_time_granularity(TimeGranularity.DAY),
    )


@pytest.fixture
def entity_spec() -> EntitySpec:  # noqa: D103
    return EntitySpec(
        element_name="user_id",
        entity_links=(EntityReference(element_name="listing_id"),),
    )


def test_merge_specs(dimension_spec: DimensionSpec, entity_spec: EntitySpec) -> None:
    """Tests InstanceSpec.merge()."""
    assert InstanceSpec.merge([dimension_spec], [entity_spec]) == [dimension_spec, entity_spec]


def test_dimension_without_first_entity_link(dimension_spec: DimensionSpec) -> None:  # noqa: D103
    assert dimension_spec.without_first_entity_link == DimensionSpec(
        element_name="platform", entity_links=(EntityReference(element_name="device_id"),)
    )


def test_dimension_without_entity_links(dimension_spec: DimensionSpec) -> None:  # noqa: D103
    assert dimension_spec.without_entity_links == DimensionSpec(element_name="platform", entity_links=())


def test_time_dimension_without_first_entity_link(time_dimension_spec: TimeDimensionSpec) -> None:  # noqa: D103
    assert time_dimension_spec.without_first_entity_link == TimeDimensionSpec(
        element_name="signup_ts",
        entity_links=(),
        time_granularity=ExpandedTimeGranularity.from_time_granularity(TimeGranularity.DAY),
    )


def test_time_dimension_without_entity_links(time_dimension_spec: TimeDimensionSpec) -> None:  # noqa: D103
    assert time_dimension_spec.without_entity_links == TimeDimensionSpec(
        element_name="signup_ts",
        entity_links=(),
        time_granularity=time_dimension_spec.time_granularity,
    )


def test_entity_without_first_entity_link(entity_spec: EntitySpec) -> None:  # noqa: D103
    assert entity_spec.without_first_entity_link == EntitySpec(
        element_name="user_id",
        entity_links=(),
    )


def test_entity_without_entity_links(entity_spec: EntitySpec) -> None:  # noqa: D103
    assert entity_spec.without_entity_links == EntitySpec(
        element_name="user_id",
        entity_links=(),
    )


def test_merge_linkable_specs(dimension_spec: DimensionSpec, entity_spec: EntitySpec) -> None:  # noqa: D103
    linkable_specs: Sequence[LinkableInstanceSpec] = [dimension_spec, entity_spec]

    assert LinkableInstanceSpec.merge_linkable_specs([dimension_spec], [entity_spec]) == linkable_specs


def test_dunder_name() -> None:  # noqa: D103
    assert (
        DimensionSpec(element_name="country", entity_links=(EntityReference("listing_id"),)).dunder_name
        == "listing_id__country"
    )


def test_merge_spec_set() -> None:  # noqa: D103
    spec_set1 = InstanceSpecSet(metric_specs=(MetricSpec(element_name="bookings"),))
    spec_set2 = InstanceSpecSet(
        dimension_specs=(DimensionSpec(element_name="is_instant", entity_links=(EntityReference("booking"),)),)
    )

    assert spec_set1.merge(spec_set2) == InstanceSpecSet(
        metric_specs=(MetricSpec(element_name="bookings"),),
        dimension_specs=(DimensionSpec(element_name="is_instant", entity_links=(EntityReference("booking"),)),),
    )


@pytest.fixture
def spec_set() -> InstanceSpecSet:  # noqa: D103
    return InstanceSpecSet(
        metric_specs=(MetricSpec(element_name="bookings"),),
        simple_metric_input_specs=(
            SimpleMetricInputSpec(
                element_name="bookings",
            ),
        ),
        dimension_specs=(DimensionSpec(element_name="is_instant", entity_links=(EntityReference("booking"),)),),
        time_dimension_specs=(
            TimeDimensionSpec(
                element_name="ds",
                entity_links=(),
                time_granularity=ExpandedTimeGranularity.from_time_granularity(TimeGranularity.DAY),
            ),
        ),
        entity_specs=(
            EntitySpec(
                element_name="user_id",
                entity_links=(EntityReference(element_name="listing_id"),),
            ),
        ),
        group_by_metric_specs=(
            GroupByMetricSpec(
                element_name="bookings",
                entity_links=(EntityReference(element_name="listing_id"),),
                metric_subquery_entity_links=(EntityReference(element_name="listing_id"),),
            ),
        ),
    )


def test_spec_set_linkable_specs(spec_set: InstanceSpecSet) -> None:  # noqa: D103
    assert set(spec_set.linkable_specs) == {
        DimensionSpec(element_name="is_instant", entity_links=(EntityReference("booking"),)),
        TimeDimensionSpec(
            element_name="ds",
            entity_links=(),
            time_granularity=ExpandedTimeGranularity.from_time_granularity(TimeGranularity.DAY),
        ),
        EntitySpec(
            element_name="user_id",
            entity_links=(EntityReference(element_name="listing_id"),),
        ),
        GroupByMetricSpec(
            element_name="bookings",
            entity_links=(EntityReference(element_name="listing_id"),),
            metric_subquery_entity_links=(EntityReference(element_name="listing_id"),),
        ),
    }


def test_spec_set_all_specs(spec_set: InstanceSpecSet) -> None:  # noqa: D103
    assert set(spec_set.all_specs) == {
        MetricSpec(element_name="bookings"),
        SimpleMetricInputSpec(
            element_name="bookings",
        ),
        DimensionSpec(element_name="is_instant", entity_links=(EntityReference("booking"),)),
        TimeDimensionSpec(
            element_name="ds",
            entity_links=(),
            time_granularity=ExpandedTimeGranularity.from_time_granularity(TimeGranularity.DAY),
        ),
        EntitySpec(
            element_name="user_id",
            entity_links=(EntityReference(element_name="listing_id"),),
        ),
        GroupByMetricSpec(
            element_name="bookings",
            entity_links=(EntityReference(element_name="listing_id"),),
            metric_subquery_entity_links=(EntityReference(element_name="listing_id"),),
        ),
    }


def test_linkless_entity() -> None:
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


@pytest.fixture
def where_filter_spec_set() -> WhereFilterSpecSet:  # noqa: D103
    return WhereFilterSpecSet(
        metric_level_filter_specs=(
            WhereFilterSpec(
                where_sql="metric is true",
                bind_parameters=SqlBindParameterSet(),
                element_set=GroupByItemSet(),
            ),
        ),
        query_level_filter_specs=(
            WhereFilterSpec(
                where_sql="query is true",
                bind_parameters=SqlBindParameterSet(),
                element_set=GroupByItemSet(),
            ),
        ),
    )


def test_where_filter_spec_set_all_specs(where_filter_spec_set: WhereFilterSpecSet) -> None:  # noqa: D103
    assert set(where_filter_spec_set.all_filter_specs) == {
        WhereFilterSpec(
            where_sql="metric is true",
            bind_parameters=SqlBindParameterSet(),
            element_set=GroupByItemSet(),
        ),
        WhereFilterSpec(
            where_sql="query is true",
            bind_parameters=SqlBindParameterSet(),
            element_set=GroupByItemSet(),
        ),
    }


def test_where_filter_spec_set_merge(where_filter_spec_set: WhereFilterSpecSet) -> None:  # noqa: D103
    metric_level_filter = WhereFilterSpec(
        where_sql="metric is true",
        bind_parameters=SqlBindParameterSet(),
        element_set=GroupByItemSet(),
    )
    query_level_filter = WhereFilterSpec(
        where_sql="query is true",
        bind_parameters=SqlBindParameterSet(),
        element_set=GroupByItemSet(),
    )

    spec_set1 = WhereFilterSpecSet(
        metric_level_filter_specs=(metric_level_filter,),
    )
    spec_set2 = WhereFilterSpecSet(query_level_filter_specs=(query_level_filter,))

    assert spec_set1.merge(spec_set2) == WhereFilterSpecSet(
        metric_level_filter_specs=(metric_level_filter,), query_level_filter_specs=(query_level_filter,)
    )
