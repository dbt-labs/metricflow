"""Test cases that check error messages raised during query parsing.

TODO: These tests may be better parameterized.
"""
from __future__ import annotations

from tests_metricflow_semantics.query.parser_helpers import QueryParserTester


def test_empty_query(parser_tester_for_bookings_manifest: QueryParserTester) -> None:
    """Test the error message for an empty query."""
    parser_tester_for_bookings_manifest.assert_error_snapshot()


def test_invalid_where_filter(parser_tester_for_bookings_manifest: QueryParserTester) -> None:
    """Test the error message for an invalid where-filter."""
    parser_tester_for_bookings_manifest.assert_error_snapshot(
        metric_names=["bookings"],
        group_by_names=["metric_time__day"],
        where_constraint_strs=["{{ Dimension('booking__invalid_dim') }} = '1'"],
    )


def test_long_invalid_metric(parser_tester_for_bookings_manifest: QueryParserTester) -> None:
    """Test the error message for an empty query."""
    parser_tester_for_bookings_manifest.assert_error_snapshot(
        metric_names=["bookings", "long_invalid_metric_name" + "_" * 50],
        group_by_names=["metric_time__day"],
        where_constraint_strs=["{{ Dimension('booking__invalid_dim') }} = '1'"],
    )


def test_join_scd_model_to_scd_model(parser_tester_for_scd_manifest: QueryParserTester) -> None:
    """Check that a join from a measure model -> SCD model -> SCD model raises an error."""
    parser_tester_for_scd_manifest.assert_error_snapshot(
        metric_names=["bookings"],
        group_by_names=["metric_time__day", "listing__user__account_type"],
    )
