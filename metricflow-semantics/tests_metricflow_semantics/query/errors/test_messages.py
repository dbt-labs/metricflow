"""Test cases that check error messages raised during query parsing.

TODO: These tests may be better parameterized.
"""
from __future__ import annotations

from typing import Optional, Sequence

import pytest
from _pytest.fixtures import FixtureRequest
from metricflow_semantics.query.query_parser import MetricFlowQueryParser
from metricflow_semantics.test_helpers.config_helpers import MetricFlowTestConfiguration
from metricflow_semantics.test_helpers.snapshot_helpers import assert_str_snapshot_equal


def _check_error_message(
    request: FixtureRequest,
    mf_test_configuration: MetricFlowTestConfiguration,
    query_parser: MetricFlowQueryParser,
    metric_names: Optional[Sequence[str]] = None,
    group_by_names: Optional[Sequence[str]] = None,
    where_constraint_strs: Optional[Sequence[str]] = None,
) -> None:
    with pytest.raises(Exception) as e:
        query_parser.parse_and_validate_query(
            metric_names=metric_names,
            group_by_names=group_by_names,
            where_constraint_strs=where_constraint_strs,
        )
    assert_str_snapshot_equal(
        request=request,
        mf_test_configuration=mf_test_configuration,
        snapshot_id="result",
        snapshot_str=str(e.value),
    )


def test_empty_query(
    request: FixtureRequest,
    mf_test_configuration: MetricFlowTestConfiguration,
    bookings_query_parser: MetricFlowQueryParser,
) -> None:
    """Test the error message for an empty query."""
    _check_error_message(
        request=request,
        mf_test_configuration=mf_test_configuration,
        query_parser=bookings_query_parser,
    )


def test_invalid_where_filter(
    request: FixtureRequest,
    mf_test_configuration: MetricFlowTestConfiguration,
    bookings_query_parser: MetricFlowQueryParser,
) -> None:
    """Test the error message for an invalid where-filter."""
    _check_error_message(
        request=request,
        mf_test_configuration=mf_test_configuration,
        query_parser=bookings_query_parser,
        metric_names=["bookings"],
        group_by_names=["metric_time__day"],
        where_constraint_strs=["{{ Dimension('booking__invalid_dim') }} = '1'"],
    )


def test_long_invalid_metric(
    request: FixtureRequest,
    mf_test_configuration: MetricFlowTestConfiguration,
    bookings_query_parser: MetricFlowQueryParser,
) -> None:
    """Test the error message for an empty query."""
    _check_error_message(
        request=request,
        mf_test_configuration=mf_test_configuration,
        query_parser=bookings_query_parser,
        metric_names=["bookings", "long_invalid_metric_name" + "_" * 50],
        group_by_names=["metric_time__day"],
        where_constraint_strs=["{{ Dimension('booking__invalid_dim') }} = '1'"],
    )
