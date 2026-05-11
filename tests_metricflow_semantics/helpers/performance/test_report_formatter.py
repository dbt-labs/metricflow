from __future__ import annotations

import logging
import re

from _pytest.fixtures import FixtureRequest
from metricflow_semantics.test_helpers.config_helpers import MetricFlowTestConfiguration
from metricflow_semantics.test_helpers.performance.profiling import PerformanceTracker
from metricflow_semantics.test_helpers.performance.report_formatter import TableTextFormatter
from metricflow_semantics.test_helpers.snapshot_helpers import assert_str_snapshot_equal
from metricflow_semantics.toolkit.mf_logging.lazy_formattable import LazyFormat

logger = logging.getLogger(__name__)

_TEST_MAX_ATTEMPT_COUNT = 3


def test_format_report_to_text_table(
    request: FixtureRequest,
    mf_test_configuration: MetricFlowTestConfiguration,
) -> None:
    """Tests formatting a performance report to a text table."""

    def _recurse_n_times(depth: int) -> None:
        if depth > 0:
            _recurse_n_times(depth=depth - 1)
        return

    for attempt_index in range(_TEST_MAX_ATTEMPT_COUNT):
        performance_tracker = PerformanceTracker()
        with performance_tracker.session("Profile `_recurse_n_times(2)`"):
            _recurse_n_times(2)

        text_table = performance_tracker.last_session_report.text_format(TableTextFormatter(row_limit=1))
        # Replace line numbers that come after a `:` with `*`.
        text_table = re.sub(pattern=r":(\d+)", repl=lambda match: ":" + "*" * len(match.group(1)), string=text_table)

        try:
            assert_str_snapshot_equal(
                request=request, snapshot_configuration=mf_test_configuration, snapshot_str=text_table
            )
            return
        except Exception as e:
            if attempt_index == _TEST_MAX_ATTEMPT_COUNT - 1:
                raise
            logger.warning(
                LazyFormat(
                    "Got a snapshot error. Retrying as slight variations in runtime will cause snapshot differences.",
                    exc_info=e,
                )
            )
