from __future__ import annotations

from datetime import datetime

import pytest
from metricflow_semantics.filters.time_constraint import TimeRangeConstraint
from metricflow_semantics.specs.spec_classes import WhereFilterSpec
from metricflow_semantics.sql.sql_bind_parameters import SqlBindParameters

from metricflow.dataflow.optimizer.predicate_pushdown_optimizer import PredicatePushdownBranchStateTracker
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
                where_sql="x is true", bind_parameters=SqlBindParameters(), linkable_elements=(), linkable_specs=()
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
        where_sql="x is true", bind_parameters=SqlBindParameters(), linkable_elements=(), linkable_specs=()
    )
    where_spec_y_is_null = WhereFilterSpec(
        where_sql="y is null", bind_parameters=SqlBindParameters(), linkable_elements=(), linkable_specs=()
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
