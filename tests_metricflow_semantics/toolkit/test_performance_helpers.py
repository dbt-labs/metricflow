from __future__ import annotations

import logging
import re
from typing import Sequence

import pytest
from _pytest.fixtures import FixtureRequest
from metricflow_semantics.test_helpers.config_helpers import MetricFlowTestConfiguration
from metricflow_semantics.test_helpers.snapshot_helpers import assert_str_snapshot_equal
from metricflow_semantics.toolkit import performance_helpers
from metricflow_semantics.toolkit.performance_helpers import mf_log_duration

logger = logging.getLogger(__name__)

# Matches the elapsed duration at the end of timer log lines so snapshots do not depend on wall-clock timing.
# e.g. `[END D0] Outer timer [0.01 s]` becomes `[END D0] Outer timer [<duration>]`.
_DURATION_PATTERN = re.compile(r"\[\d+\.\d+ s\]")


def _log_messages_with_normalized_durations(messages: Sequence[str]) -> str:
    """Return log messages with elapsed durations normalized for snapshot stability."""
    return "\n".join(_DURATION_PATTERN.sub("[<duration>]", message) for message in messages)


def test_nested_execution_timers(
    request: FixtureRequest,
    mf_test_configuration: MetricFlowTestConfiguration,
    caplog: pytest.LogCaptureFixture,
) -> None:
    """Snapshot the log lines emitted by nested `ExecutionTimer` contexts."""
    with caplog.at_level(logging.INFO, logger=performance_helpers.__name__):
        with performance_helpers.ExecutionTimer("Outer timer", duration_warning_threshold=None):
            with performance_helpers.ExecutionTimer("Inner timer", duration_warning_threshold=None):
                pass

    assert_str_snapshot_equal(
        request=request,
        snapshot_configuration=mf_test_configuration,
        snapshot_str=_log_messages_with_normalized_durations(caplog.messages),
    )


@mf_log_duration(duration_warning_threshold=None)
def _outer_function() -> str:
    return _inner_function()


@mf_log_duration(duration_warning_threshold=None)
def _inner_function() -> str:
    return "result"


def test_nested_mf_log_duration(
    request: FixtureRequest,
    mf_test_configuration: MetricFlowTestConfiguration,
    caplog: pytest.LogCaptureFixture,
) -> None:
    """Snapshot the log lines emitted by nested functions decorated with `mf_log_duration`."""
    with caplog.at_level(logging.INFO, logger=performance_helpers.__name__):
        assert _outer_function() == "result"

    assert_str_snapshot_equal(
        request=request,
        snapshot_configuration=mf_test_configuration,
        snapshot_str=_log_messages_with_normalized_durations(caplog.messages),
    )
