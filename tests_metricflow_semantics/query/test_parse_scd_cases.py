from __future__ import annotations

import logging

from tests_metricflow_semantics.query.parser_helpers import QueryParserTester

logger = logging.getLogger(__name__)


def test_join_scd_model_to_non_scd_model(parser_tester_for_scd_manifest: QueryParserTester) -> None:
    """Check that a join from a measure model -> SCD model -> non-SCD model is allowed."""
    parser_tester_for_scd_manifest.assert_result_snapshot(
        metric_names=["bookings"],
        group_by_names=["metric_time__day", "listing__user__home_state_latest"],
    )
