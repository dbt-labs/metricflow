from __future__ import annotations

from datetime import datetime

import pytest
from _pytest.fixtures import FixtureRequest
from dbt_semantic_interfaces.implementations.filters.where_filter import PydanticWhereFilter
from metricflow_semantics.filters.time_constraint import TimeRangeConstraint
from metricflow_semantics.query.query_parser import MetricFlowQueryParser
from metricflow_semantics.specs.linkable_spec_set import LinkableSpecSet
from metricflow_semantics.specs.query_spec import MetricFlowQuerySpec
from metricflow_semantics.specs.where_filter.where_filter_spec import WhereFilterSpec
from metricflow_semantics.sql.sql_bind_parameters import SqlBindParameterSet
from metricflow_semantics.test_helpers.config_helpers import MetricFlowTestConfiguration
from metricflow_semantics.test_helpers.snapshot_helpers import assert_plan_snapshot_text_equal

from metricflow.dataflow.builder.dataflow_plan_builder import DataflowPlanBuilder
from metricflow.dataflow.optimizer.predicate_pushdown_optimizer import (
    PredicatePushdownBranchStateTracker,
    PredicatePushdownOptimizer,
)
from metricflow.plan_conversion.node_processor import PredicateInputType, PredicatePushdownState


@pytest.fixture
def fully_enabled_pushdown_state() -> PredicatePushdownState:
    """Provides a valid configuration with all predicate properties set and pushdown fully enabled."""
    params = PredicatePushdownState(time_range_constraint=TimeRangeConstraint.all_time(), where_filter_specs=tuple())
    return params


@pytest.fixture
def branch_state_tracker(fully_enabled_pushdown_state: PredicatePushdownState) -> PredicatePushdownBranchStateTracker:
    """Provides a branch state tracker for direct testing of update mechanics."""
    return PredicatePushdownBranchStateTracker(initial_state=fully_enabled_pushdown_state)


def test_time_range_pushdown_enabled_states(fully_enabled_pushdown_state: PredicatePushdownState) -> None:
    """Tests pushdown enabled check for time range pushdown operations."""
    time_range_only_state = PredicatePushdownState(
        time_range_constraint=TimeRangeConstraint.all_time(),
        pushdown_enabled_types=frozenset([PredicateInputType.TIME_RANGE_CONSTRAINT]),
        where_filter_specs=tuple(),
    )

    enabled_states = {
        "fully enabled": fully_enabled_pushdown_state.has_time_range_constraint_to_push_down,
        "enabled for time range only": time_range_only_state.has_time_range_constraint_to_push_down,
    }

    assert all(list(enabled_states.values())), (
        "Expected pushdown to be enabled for pushdown state with time range constraint and global pushdown enabled, "
        "but some states returned False for has_time_range_constraint_to_push_down.\n"
        f"Pushdown enabled states: {enabled_states}\n"
        f"Fully enabled state: {fully_enabled_pushdown_state}\n"
        f"Time range only state: {time_range_only_state}"
    )


def test_invalid_disabled_pushdown_state() -> None:
    """Tests checks for invalid param configuration on disabled pushdown parameters."""
    with pytest.raises(AssertionError, match="Disabled pushdown state objects cannot have properties set"):
        PredicatePushdownState(
            time_range_constraint=TimeRangeConstraint.all_time(),
            pushdown_enabled_types=frozenset(),
            where_filter_specs=tuple(),
        )


def test_branch_state_propagation(branch_state_tracker: PredicatePushdownBranchStateTracker) -> None:
    """Tests forward propagation of predicate pushdown branch state.

    This asserts against expected results on entry and exit of a three-hop nested propagation.
    """
    base_state = branch_state_tracker.last_pushdown_state
    where_state = PredicatePushdownState.with_where_filter_specs(
        original_pushdown_state=base_state,
        where_filter_specs=(
            WhereFilterSpec(
                where_sql="x is true",
                bind_parameters=SqlBindParameterSet(),
                linkable_element_unions=(),
                linkable_spec_set=LinkableSpecSet(),
            ),
        ),
    )
    time_state = PredicatePushdownState.with_time_range_constraint(
        original_pushdown_state=base_state,
        time_range_constraint=TimeRangeConstraint(datetime(2024, 1, 1), datetime(2024, 1, 1)),
    )
    state_updates = (time_state, where_state, time_state)
    with branch_state_tracker.track_pushdown_state(state_updates[0]):
        assert branch_state_tracker.last_pushdown_state == state_updates[0], "Failed to track first state update!"
        with branch_state_tracker.track_pushdown_state(state_updates[1]):
            assert branch_state_tracker.last_pushdown_state == state_updates[1], "Failed to track second state update!"
            with branch_state_tracker.track_pushdown_state(state_updates[2]):
                assert (
                    branch_state_tracker.last_pushdown_state == state_updates[2]
                ), "Failed to track third state update!"

            assert branch_state_tracker.last_pushdown_state == state_updates[1], "Failed to remove third state update!"

        assert branch_state_tracker.last_pushdown_state == state_updates[0], "Failed to remove second state update!"

    assert branch_state_tracker.last_pushdown_state == base_state, "Failed to remove first state update!"


def test_applied_filter_back_propagation(branch_state_tracker: PredicatePushdownBranchStateTracker) -> None:
    """Tests backwards propagation of applied where filter annotations.

    This asserts that propagation on entry remains unaffected while the applied where filter annotations are
    back-propagated as expected after exit, both for cases where an update was applied on entry to the
    context manager and where the value was overridden just prior to exit from the context manager.
    """
    base_state = branch_state_tracker.last_pushdown_state
    where_spec_x_is_true = WhereFilterSpec(
        where_sql="x is true",
        bind_parameters=SqlBindParameterSet(),
        linkable_element_unions=(),
        linkable_spec_set=LinkableSpecSet(),
    )
    where_spec_y_is_null = WhereFilterSpec(
        where_sql="y is null",
        bind_parameters=SqlBindParameterSet(),
        linkable_element_unions=(),
        linkable_spec_set=LinkableSpecSet(),
    )

    where_state = PredicatePushdownState.with_where_filter_specs(
        original_pushdown_state=base_state, where_filter_specs=(where_spec_x_is_true, where_spec_y_is_null)
    )
    x_applied_state = PredicatePushdownState.with_pushdown_applied_where_filter_specs(
        original_pushdown_state=where_state, pushdown_applied_where_filter_specs=frozenset((where_spec_x_is_true,))
    )
    both_applied_state = PredicatePushdownState.with_pushdown_applied_where_filter_specs(
        original_pushdown_state=base_state,
        pushdown_applied_where_filter_specs=frozenset((where_spec_x_is_true, where_spec_y_is_null)),
    )

    with branch_state_tracker.track_pushdown_state(base_state):
        assert (
            branch_state_tracker.last_pushdown_state == base_state
        ), "Initial condition AND initial tracking mis-configured!"
        with branch_state_tracker.track_pushdown_state(where_state):
            assert branch_state_tracker.last_pushdown_state == where_state, "Failed to track where state!"
            with branch_state_tracker.track_pushdown_state(x_applied_state):
                assert (
                    branch_state_tracker.last_pushdown_state == x_applied_state
                ), "Failed to track applied filter state!"

            assert (
                branch_state_tracker.last_pushdown_state.applied_where_filter_specs
                == x_applied_state.applied_where_filter_specs
            ), "Failed to back-propagate applied filter state!"
            # Update internally from where state
            branch_state_tracker.override_last_pushdown_state(
                PredicatePushdownState.with_pushdown_applied_where_filter_specs(
                    original_pushdown_state=branch_state_tracker.last_pushdown_state,
                    pushdown_applied_where_filter_specs=frozenset((where_spec_x_is_true, where_spec_y_is_null)),
                )
            )

        assert not branch_state_tracker.last_pushdown_state.has_where_filters_to_push_down, (
            f"Failed to remove where filter state update! Should be {base_state} but got "
            f"{branch_state_tracker.last_pushdown_state}!"
        )
        assert branch_state_tracker.last_pushdown_state == both_applied_state

    # We expect to propagate back to the initial entry since we only ever want to apply a filter once within a branch
    assert branch_state_tracker.last_pushdown_state == both_applied_state


def _check_optimization(
    request: FixtureRequest,
    mf_test_configuration: MetricFlowTestConfiguration,
    dataflow_plan_builder: DataflowPlanBuilder,
    query_spec: MetricFlowQuerySpec,
    expected_additional_constraint_nodes_in_optimized: int,
) -> None:
    dataflow_plan = dataflow_plan_builder.build_plan(query_spec=query_spec)
    optimizer = PredicatePushdownOptimizer(node_data_set_resolver=dataflow_plan_builder._node_data_set_resolver)
    optimized_plan = optimizer.optimize(dataflow_plan=dataflow_plan)

    for plan in (dataflow_plan, optimized_plan):
        assert_plan_snapshot_text_equal(
            request=request,
            mf_test_configuration=mf_test_configuration,
            plan=plan,
            plan_snapshot_text=plan.structure_text(),
        )

    assert dataflow_plan.node_count + expected_additional_constraint_nodes_in_optimized == optimized_plan.node_count, (
        f"Did not get the expected number ({expected_additional_constraint_nodes_in_optimized}) of additional "
        f"constraint nodes in the optimized plan, found {optimized_plan.node_count - dataflow_plan.node_count} added "
        "nodes. Check snapshot output for details."
    )


def test_simple_join_categorical_pushdown(
    request: FixtureRequest,
    mf_test_configuration: MetricFlowTestConfiguration,
    query_parser: MetricFlowQueryParser,
    dataflow_plan_builder: DataflowPlanBuilder,
) -> None:
    """Tests pushdown optimization for a simple predicate through a single join.

    In this case the entire constraint should be moved inside of the join.
    """
    query_spec = query_parser.parse_and_validate_query(
        metric_names=("bookings",),
        group_by_names=("listing__country_latest",),
        where_constraints=[PydanticWhereFilter(where_sql_template="{{ Dimension('booking__is_instant') }}")],
    ).query_spec
    _check_optimization(
        request=request,
        mf_test_configuration=mf_test_configuration,
        dataflow_plan_builder=dataflow_plan_builder,
        query_spec=query_spec,
        expected_additional_constraint_nodes_in_optimized=0,
    )


def test_simple_join_metric_time_pushdown_with_two_targets(
    request: FixtureRequest,
    mf_test_configuration: MetricFlowTestConfiguration,
    query_parser: MetricFlowQueryParser,
    dataflow_plan_builder: DataflowPlanBuilder,
) -> None:
    """Tests pushdown optimization for a simple metric time predicate through a single join.

    This includes a scenario where the dimension source is also a metric time node, but we do NOT want the metric_time
    filter applied to it since it is a _current style dimension table at its core.

    Note this optimizer will not push the predicate down until metric_time pushdown is supported.
    """
    query_spec = query_parser.parse_and_validate_query(
        metric_names=("bookings",),
        group_by_names=("listing__country_latest",),
        where_constraints=[PydanticWhereFilter(where_sql_template="{{ TimeDimension('metric_time') }} = '2024-01-01'")],
    ).query_spec
    _check_optimization(
        request=request,
        mf_test_configuration=mf_test_configuration,
        dataflow_plan_builder=dataflow_plan_builder,
        query_spec=query_spec,
        expected_additional_constraint_nodes_in_optimized=0,  # TODO: Add support for time dimension pushdown
    )


def test_conversion_metric_predicate_pushdown(
    request: FixtureRequest,
    mf_test_configuration: MetricFlowTestConfiguration,
    query_parser: MetricFlowQueryParser,
    dataflow_plan_builder: DataflowPlanBuilder,
) -> None:
    """Tests pushdown optimizer behavior for a simple predicate on a conversion metric.

    As of this time the pushdown should NOT move past the conversion metric node.
    """
    query_spec = query_parser.parse_and_validate_query(
        metric_names=("visit_buy_conversion_rate_7days",),
        group_by_names=("metric_time", "user__home_state_latest"),
        where_constraints=[PydanticWhereFilter(where_sql_template="{{ Dimension('visit__referrer_id') }} = '123456'")],
    ).query_spec
    _check_optimization(
        request=request,
        mf_test_configuration=mf_test_configuration,
        dataflow_plan_builder=dataflow_plan_builder,
        query_spec=query_spec,
        expected_additional_constraint_nodes_in_optimized=0,
    )


def test_cumulative_metric_predicate_pushdown(
    request: FixtureRequest,
    mf_test_configuration: MetricFlowTestConfiguration,
    query_parser: MetricFlowQueryParser,
    dataflow_plan_builder: DataflowPlanBuilder,
) -> None:
    """Tests pushdown optimizer behavior for a query against a cumulative metric.

    At this time categorical dimension predicates should be pushed down, but metric_time predicates should not be,
    since supporting time filter pushdown for cumulative metrics requires filter expansion to ensure we capture the
    full set of inputs for the initial cumulative window.

    For the query listed here the entire constraint will be moved past the dimension join.

    TODO: Add metric time filters
    """
    query_spec = query_parser.parse_and_validate_query(
        metric_names=("every_two_days_bookers",),
        group_by_names=("listing__country_latest", "metric_time"),
        where_constraints=[PydanticWhereFilter(where_sql_template="{{ Dimension('booking__is_instant') }}")],
    ).query_spec
    _check_optimization(
        request=request,
        mf_test_configuration=mf_test_configuration,
        dataflow_plan_builder=dataflow_plan_builder,
        query_spec=query_spec,
        expected_additional_constraint_nodes_in_optimized=0,
    )


@pytest.mark.skip("plan output has non-deterministic ordering")
def test_aggregate_output_join_metric_predicate_pushdown(
    request: FixtureRequest,
    mf_test_configuration: MetricFlowTestConfiguration,
    query_parser: MetricFlowQueryParser,
    dataflow_plan_builder: DataflowPlanBuilder,
) -> None:
    """Tests pushdown optimizer behavior when a metric does an aggregate output metric join.

    In this case we expect filters to not be pushed down, since they are outside of a full outer join.
    """
    query_spec = query_parser.parse_and_validate_query(
        metric_names=("views_times_booking_value",),
        where_constraints=[PydanticWhereFilter(where_sql_template="{{ Dimension('listing__is_lux_latest') }}")],
    ).query_spec
    _check_optimization(
        request=request,
        mf_test_configuration=mf_test_configuration,
        dataflow_plan_builder=dataflow_plan_builder,
        query_spec=query_spec,
        expected_additional_constraint_nodes_in_optimized=0,
    )


@pytest.mark.skip("Predicate pushdown is not implemented for some of the nodes in this plan")
def test_offset_metric_predicate_pushdown(
    request: FixtureRequest,
    mf_test_configuration: MetricFlowTestConfiguration,
    query_parser: MetricFlowQueryParser,
    dataflow_plan_builder: DataflowPlanBuilder,
) -> None:
    """Tests pushdown optimizer behavior for a query against a derived offset metric.

    As with cumulative metrics, at this time categorical dimension predicates may be pushed down, but metric_time
    predicates should not be, since we need to capture the union of the filter window and the offset span.

    For the query listed here the entire constraint will be moved past the dimension join.

    TODO: Add metric time filters
    """
    query_spec = query_parser.parse_and_validate_query(
        metric_names=("bookings_growth_2_weeks",),
        group_by_names=("listing__country_latest", "metric_time"),
        where_constraints=[PydanticWhereFilter(where_sql_template="{{ Dimension('booking__is_instant') }}")],
    ).query_spec
    _check_optimization(
        request=request,
        mf_test_configuration=mf_test_configuration,
        dataflow_plan_builder=dataflow_plan_builder,
        query_spec=query_spec,
        expected_additional_constraint_nodes_in_optimized=0,
    )


@pytest.mark.skip("Predicate pushdown is not implemented for some of the nodes in this plan")
def test_fill_nulls_time_spine_metric_predicate_pushdown(
    request: FixtureRequest,
    mf_test_configuration: MetricFlowTestConfiguration,
    query_parser: MetricFlowQueryParser,
    dataflow_plan_builder: DataflowPlanBuilder,
) -> None:
    """Tests pushdown optimizer behavior for a metric with a time spine and fill_nulls_with enabled.

    Until time dimension pushdown is supported we will only see the categorical dimension entry pushed down here.

    For the query listed here the entire constraint will be moved past the dimension join.

    TODO: Add metric time filters
    """
    query_spec = query_parser.parse_and_validate_query(
        metric_names=("bookings_growth_2_weeks_fill_nulls_with_0",),
        group_by_names=("listing__country_latest", "metric_time"),
        where_constraints=[PydanticWhereFilter(where_sql_template="{{ Dimension('booking__is_instant') }}")],
    ).query_spec
    _check_optimization(
        request=request,
        mf_test_configuration=mf_test_configuration,
        dataflow_plan_builder=dataflow_plan_builder,
        query_spec=query_spec,
        expected_additional_constraint_nodes_in_optimized=0,
    )


@pytest.mark.skip("Predicate pushdown is not implemented for some of the nodes in this plan")
def test_fill_nulls_time_spine_metric_with_post_agg_join_predicate_pushdown(
    request: FixtureRequest,
    mf_test_configuration: MetricFlowTestConfiguration,
    query_parser: MetricFlowQueryParser,
    dataflow_plan_builder: DataflowPlanBuilder,
) -> None:
    """Tests pushdown optimizer behavior for a metric with a time spine and fill_nulls_with and a post-agg join.

    When querying a metric like this with a group by on all filter specs we do a post-aggregation outer join
    against the time spine, which should preclude predicate pushdown for query-time filters at that state, but
    will allow for pushdown within the JoinToTimeSpine operation. This will still do predicate pushdown as before,
    but only exactly as before - the added constraint outside of the JoinToTimeSpine operation must still be
    applied in its entirety, and so we expect 0 additional constraint nodes. If we failed to account for the
    repeated constraint outside of the JoinToTimeSpine in our pushdown handling this would remove one of the
    WhereConstraintNodes from the original query altogether.

    Until time dimension pushdown is supported we will only see the categorical dimension entry pushed down here.

    TODO: Add metric time filters
    """
    query_spec = query_parser.parse_and_validate_query(
        metric_names=("bookings_growth_2_weeks_fill_nulls_with_0",),
        group_by_names=("listing__country_latest", "booking__is_instant", "metric_time"),
        where_constraints=[PydanticWhereFilter(where_sql_template="{{ Dimension('booking__is_instant') }}")],
    ).query_spec
    _check_optimization(
        request=request,
        mf_test_configuration=mf_test_configuration,
        dataflow_plan_builder=dataflow_plan_builder,
        query_spec=query_spec,
        expected_additional_constraint_nodes_in_optimized=0,
    )
