from __future__ import annotations

import dataclasses

import pytest
from metricflow_semantics.filters.time_constraint import TimeRangeConstraint

from metricflow.plan_conversion.node_processor import PredicatePushdownParameters, PredicatePushdownState


@pytest.fixture
def all_pushdown_params() -> PredicatePushdownParameters:
    """Tests a valid configuration with all predicate properties set and pushdown fully enabled."""
    params = PredicatePushdownParameters(
        time_range_constraint=TimeRangeConstraint.all_time(), pushdown_state=PredicatePushdownState.FULLY_ENABLED
    )
    predicate_property_names = {
        field.name for field in dataclasses.fields(params) if field.metadata.get(params._PREDICATE_METADATA_KEY)
    }
    predicate_properties = {
        name: value for name, value in dataclasses.asdict(params).items() if name in predicate_property_names
    }
    assert all(value is not None for value in predicate_properties.values()), (
        "All predicate properties in this pushdown param instance should be set to something other than None. Found "
        f"one or more None values in property map: {predicate_properties}"
    )
    return params


def test_time_range_pushdown_enabled_states(all_pushdown_params: PredicatePushdownParameters) -> None:
    """Tests pushdown enabled check for time range pushdown operations."""
    time_range_only_params = PredicatePushdownParameters(
        time_range_constraint=TimeRangeConstraint.all_time(),
        pushdown_state=PredicatePushdownState.ENABLED_FOR_TIME_RANGE_ONLY,
    )

    enabled_states = {
        "fully enabled": all_pushdown_params.is_pushdown_enabled,
        "enabled for time range only": time_range_only_params.is_pushdown_enabled,
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
            pushdown_state=PredicatePushdownState.DISABLED, time_range_constraint=TimeRangeConstraint.all_time()
        )
