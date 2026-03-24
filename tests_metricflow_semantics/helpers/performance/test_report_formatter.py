from __future__ import annotations

import logging
import re

from _pytest.fixtures import FixtureRequest
from metricflow_semantics.test_helpers.config_helpers import MetricFlowTestConfiguration
from metricflow_semantics.test_helpers.performance.profiling import PerformanceTracker
from metricflow_semantics.test_helpers.performance.report_formatter import TableTextFormatter
from metricflow_semantics.test_helpers.snapshot_helpers import assert_str_snapshot_equal

logger = logging.getLogger(__name__)


def test_format_report_to_text_table(
    request: FixtureRequest,
    mf_test_configuration: MetricFlowTestConfiguration,
) -> None:
    """Tests formatting a performance report to a text table."""

    def _recurse_n_times(depth: int) -> None:
        if depth > 0:
            _recurse_n_times(depth=depth - 1)
        return

    performance_tracker = PerformanceTracker()
    with performance_tracker.session("Profile `_recurse_n_times(2)`"):
        _recurse_n_times(2)

    text_table = performance_tracker.last_session_report.text_format(TableTextFormatter(row_limit=1))
    # Replace line numbers that come after a `:` with `*`.
    text_table = re.sub(pattern=r":(\d+)", repl=lambda match: ":" + "*" * len(match.group(1)), string=text_table)

    assert_str_snapshot_equal(request=request, snapshot_configuration=mf_test_configuration, snapshot_str=text_table)
