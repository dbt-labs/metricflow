from __future__ import annotations

import pytest
from metricflow_semantics.filters.time_constraint import TimeRangeConstraint

from metricflow.plan_conversion.node_processor import PredicateInputType, PredicatePushdownState


@pytest.fixture
def fully_enabled_pushdown_state() -> PredicatePushdownState:
    """Tests a valid configuration with all predicate properties set and pushdown fully enabled."""
    params = PredicatePushdownState(
        time_range_constraint=TimeRangeConstraint.all_time(),
    )
    return params


def test_time_range_pushdown_enabled_states(fully_enabled_pushdown_state: PredicatePushdownState) -> None:
    """Tests pushdown enabled check for time range pushdown operations."""
    time_range_only_state = PredicatePushdownState(
        time_range_constraint=TimeRangeConstraint.all_time(),
        pushdown_enabled_types=frozenset([PredicateInputType.TIME_RANGE_CONSTRAINT]),
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
        PredicatePushdownState(time_range_constraint=TimeRangeConstraint.all_time(), pushdown_enabled_types=frozenset())
