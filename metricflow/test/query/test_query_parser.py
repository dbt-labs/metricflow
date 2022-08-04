import logging

import pytest

from metricflow.constraints.time_constraint import TimeRangeConstraint
from metricflow.errors.errors import UnableToSatisfyQueryError
from metricflow.query.query_parser import MetricFlowQueryParser
from metricflow.specs import (
    MetricSpec,
    DimensionSpec,
    TimeDimensionSpec,
    IdentifierSpec,
    OrderBySpec,
    OutputColumnNameOverride,
)
from metricflow.test.test_utils import as_datetime
from metricflow.test.time.metric_time_dimension import MTD
from metricflow.time.time_granularity import TimeGranularity

logger = logging.getLogger(__name__)


def test_query_parser(query_parser: MetricFlowQueryParser) -> None:  # noqa: D
    query_spec = query_parser.parse_and_validate_query(
        metric_names=["bookings"], group_by_names=["is_instant", "listing", MTD], order=[MTD, "-bookings"]
    )

    assert query_spec.metric_specs == (MetricSpec(element_name="bookings"),)
    assert query_spec.dimension_specs == (DimensionSpec(element_name="is_instant", identifier_links=()),)
    assert query_spec.time_dimension_specs == (
        TimeDimensionSpec(element_name=MTD, identifier_links=(), time_granularity=TimeGranularity.DAY),
    )
    assert query_spec.identifier_specs == (IdentifierSpec(element_name="listing", identifier_links=()),)
    assert query_spec.order_by_specs == (
        OrderBySpec(
            item=TimeDimensionSpec(element_name=MTD, identifier_links=(), time_granularity=TimeGranularity.DAY),
            descending=False,
        ),
        OrderBySpec(
            item=MetricSpec(element_name="bookings"),
            descending=True,
        ),
    )


def test_order_by_granularity_conversion(query_parser: MetricFlowQueryParser) -> None:
    """Test that the granularity of the primary time dimension in the order by is returned appropriately.

    In the case where the primary time dimension is specified in the order by without a granularity suffix, the order
    by spec returned by the parser should have a granularity appropriate for the queried metrics.
    """

    # "bookings" has a granularity of DAY, "revenue" has a granularity of MONTH
    query_spec = query_parser.parse_and_validate_query(
        metric_names=["bookings", "revenue"], group_by_names=[MTD], order=[f"-{MTD}"]
    )

    # The lowest common granularity is MONTH, so we expect the PTD in the order by to have that granularity.
    assert (
        OrderBySpec(
            item=TimeDimensionSpec(element_name=MTD, identifier_links=(), time_granularity=TimeGranularity.DAY),
            descending=True,
        ),
    ) == query_spec.order_by_specs


def test_order_by_granularity_no_conversion(query_parser: MetricFlowQueryParser) -> None:  # noqa: D
    query_spec = query_parser.parse_and_validate_query(metric_names=["bookings"], group_by_names=[MTD], order=[MTD])

    # The only granularity is DAY, so we expect the PTD in the order by to have that granularity.
    assert (
        OrderBySpec(
            item=TimeDimensionSpec(element_name=MTD, identifier_links=(), time_granularity=TimeGranularity.DAY),
            descending=False,
        ),
    ) == query_spec.order_by_specs


def test_time_range_constraint_conversion(query_parser: MetricFlowQueryParser) -> None:
    """Test that the returned time constraint in the query spec is adjusted to match the granularity of the query."""

    # "bookings" has a granularity of DAY, "revenue" has a granularity of MONTH
    query_spec = query_parser.parse_and_validate_query(
        metric_names=["bookings", "revenue"],
        group_by_names=[MTD],
        time_constraint_start=as_datetime("2020-01-15"),
        time_constraint_end=as_datetime("2020-02-15"),
    )

    assert (
        TimeRangeConstraint(start_time=as_datetime("2020-01-15"), end_time=as_datetime("2020-02-15"))
    ) == query_spec.time_range_constraint


def test_column_override(query_parser: MetricFlowQueryParser) -> None:
    """Tests that the output column override is set.

    Should be set in cases where the metrics have a non-day granularity, but ds is specified.
    """

    # "revenue" has a granularity of MONTH
    query_spec = query_parser.parse_and_validate_query(
        metric_names=["revenue"],
        group_by_names=[MTD],
        time_constraint_start=as_datetime("2020-01-15"),
        time_constraint_end=as_datetime("2020-02-15"),
    )

    assert (
        OutputColumnNameOverride(
            time_dimension_spec=TimeDimensionSpec(
                element_name=MTD,
                identifier_links=(),
                time_granularity=TimeGranularity.DAY,
            ),
            output_column_name=MTD,
        ),
    ) == query_spec.output_column_name_overrides


def test_parse_and_validate_where_constraint_dims(query_parser: MetricFlowQueryParser) -> None:
    """Test that the returned time constraint in the query spec is adjusted to match the granularity of the query."""
    # check constraint on invalid_dim raises UnableToSatisfyQueryError
    with pytest.raises(UnableToSatisfyQueryError):
        query_parser.parse_and_validate_query(
            metric_names=["bookings"],
            group_by_names=[MTD],
            time_constraint_start=as_datetime("2020-01-15"),
            time_constraint_end=as_datetime("2020-02-15"),
            where_constraint_str="WHERE invalid_dim = '1'",
        )

    query_spec = query_parser.parse_and_validate_query(
        metric_names=["bookings"],
        group_by_names=[MTD],
        time_constraint_start=as_datetime("2020-01-15"),
        time_constraint_end=as_datetime("2020-02-15"),
        where_constraint_str="WHERE is_instant = '1'",
    )
    assert DimensionSpec(element_name="is_instant", identifier_links=()) not in query_spec.dimension_specs


def test_parse_and_validate_metric_constraint_dims(query_parser: MetricFlowQueryParser) -> None:
    """Test that the returned time constraint in the query spec is adjusted to match the granularity of the query."""

    # check constraint on invalid_dim raises UnableToSatisfyQueryError
    with pytest.raises(UnableToSatisfyQueryError):
        query_parser.parse_and_validate_query(
            metric_names=["metric_with_invalid_constraint"],
            group_by_names=[MTD],
            time_constraint_start=as_datetime("2020-01-15"),
            time_constraint_end=as_datetime("2020-02-15"),
        )
