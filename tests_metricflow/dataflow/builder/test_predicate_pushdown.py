from __future__ import annotations

import pytest
from metricflow_semantics.filters.time_constraint import TimeRangeConstraint

from metricflow.plan_conversion.node_processor import PredicateInputType, PredicatePushdownParameters


@pytest.fixture
def all_pushdown_params() -> PredicatePushdownParameters:
    """Tests a valid configuration with all predicate properties set and pushdown fully enabled."""
    params = PredicatePushdownParameters(
        time_range_constraint=TimeRangeConstraint.all_time(),
    )
    return params


def test_time_range_pushdown_enabled_states(all_pushdown_params: PredicatePushdownParameters) -> None:
    """Tests pushdown enabled check for time range pushdown operations."""
    time_range_only_params = PredicatePushdownParameters(
        time_range_constraint=TimeRangeConstraint.all_time(),
        pushdown_enabled_types=frozenset([PredicateInputType.TIME_RANGE_CONSTRAINT]),
    )

    enabled_states = {
        "fully enabled": all_pushdown_params.is_pushdown_enabled_for_time_range_constraint,
        "enabled for time range only": time_range_only_params.is_pushdown_enabled_for_time_range_constraint,
    }

    assert all(list(enabled_states.values())), (
        "Expected pushdown to be enabled for pushdown params with time range constraint and global pushdown enabled, "
        f"but some params returned False for is_pushdown_enabled.\nPushdown enabled states: {enabled_states}\n"
        f"All params: {all_pushdown_params}\nTime range only params: {time_range_only_params}"
    )


def test_invalid_disabled_pushdown_params() -> None:
    """Tests checks for invalid param configuration on disabled pushdown parameters."""
    with pytest.raises(AssertionError, match="Disabled pushdown parameters cannot have properties set"):
        PredicatePushdownParameters(
            time_range_constraint=TimeRangeConstraint.all_time(), pushdown_enabled_types=frozenset()
        )
