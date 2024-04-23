"""Test module to codify and enforce input/output behavior for certain date and time operations.

Certain functions - particularly date functions - have variable runtime behavior by default.
For example, date_part(day_of_week) might follow ISO semantics or engine-specific semantics.
Similarly, date_trunc(week) might use ISO semantics (i.e., Monday) or engine-specific semantics
(typically Sunday).

This module exists as self-enforcing documentation around our expectations. Any SqlClient implemented
in this repo and subject to MetricFlow's integration test suite should, therefore, exhibit consistent
runtime behavior in terms of the input/output mechanisms for date/time transformations.

Note this test suite is not exhaustive - it is only intended to illustrate behaviors where engines
routinely diverge in the results of their time operations. This is most commonly found where day of
week and start of year are involved.
"""

from __future__ import annotations

import logging
from typing import Any

import pandas as pd
import pytest
from dbt_semantic_interfaces.type_enums.date_part import DatePart

from metricflow.protocols.sql_client import SqlClient
from metricflow.sql.sql_exprs import (
    SqlCastToTimestampExpression,
    SqlDateTruncExpression,
    SqlExtractExpression,
    SqlStringLiteralExpression,
)
from metricflow.time.time_granularity import TimeGranularity

logger = logging.getLogger(__name__)


def _extract_dataframe_value(df: pd.DataFrame) -> Any:  # type: ignore[misc]
    """Helper to assert that a query result has a single value, and return the value from the dataframe."""
    assert df.shape == (
        1,
        1,
    ), f"Invalid dataframe, expected exactly one Timestamp value but got {df.to_markdown(index=False)}!"
    for col in df.select_dtypes(["datetimetz"]):
        # Remove time zone information if the engine includes it by default, because weird stuff happens there.
        df[col] = df[col].dt.tz_localize(None)
    return df.iloc[0, 0]


def _build_date_trunc_expression(date_string: str, time_granularity: TimeGranularity) -> SqlDateTruncExpression:
    cast_expr = SqlCastToTimestampExpression(SqlStringLiteralExpression(literal_value=date_string))
    return SqlDateTruncExpression(time_granularity=time_granularity, arg=cast_expr)


def test_date_trunc_to_year(sql_client: SqlClient) -> None:
    """Tests date trunc behavior to verify that a date_trunc to YEAR outputs the first of January.

    This is needed because the ISO calendar chooses a year start equivalent to the Monday of the week containing
    the first Thursday of that year, which might, in fact, be in the previous calendar year. Therefore, we coerce
    to the more general calendar year standard of the first of January.
    """
    # The ISO year start for 2015 is 2014-12-29, but we should always get 2015-01-01
    ISO_DATE_STRING = "2015-06-15"
    expected = pd.Timestamp(year=2015, month=1, day=1)
    date_trunc_stmt = sql_client.sql_query_plan_renderer.expr_renderer.render_sql_expr(
        _build_date_trunc_expression(date_string=ISO_DATE_STRING, time_granularity=TimeGranularity.YEAR)
    ).sql

    df = sql_client.query(f"SELECT {date_trunc_stmt}")

    actual = pd.Timestamp(_extract_dataframe_value(df=df))
    assert expected == actual


@pytest.mark.parametrize(
    ("input", "expected"),
    (
        ("2015-02-22", pd.Timestamp(year=2015, month=1, day=1)),
        ("2015-06-30", pd.Timestamp(year=2015, month=4, day=1)),
        ("2015-07-01", pd.Timestamp(year=2015, month=7, day=1)),
        ("2015-11-17", pd.Timestamp(year=2015, month=10, day=1)),
    ),
)
def test_date_trunc_to_quarter(sql_client: SqlClient, input: str, expected: pd.Timestamp) -> None:
    """Tests default date trunc behavior to quarterly boundaries.

    This should generally pin to the first day of the "natural" yearly quarters, for example,
    2015-01-01, 2015-04-01, 2015-07-01, and 2015-10-01.
    """
    date_trunc_stmt = sql_client.sql_query_plan_renderer.expr_renderer.render_sql_expr(
        _build_date_trunc_expression(date_string=input, time_granularity=TimeGranularity.QUARTER)
    ).sql

    df = sql_client.query(f"SELECT {date_trunc_stmt}")

    actual = pd.Timestamp(_extract_dataframe_value(df=df))
    assert expected == actual


@pytest.mark.parametrize(
    ("input", "expected"),
    (
        ("2023-10-08", pd.Timestamp(year=2023, month=10, day=2)),  # Sunday -> preceding Monday
        ("2023-10-02", pd.Timestamp(year=2023, month=10, day=2)),  # Monday truncates to itself
        ("2023-09-01", pd.Timestamp(year=2023, month=8, day=28)),  # Weekday -> preceding Monday
    ),
)
def test_date_trunc_to_week(sql_client: SqlClient, input: str, expected: pd.Timestamp) -> None:
    """Tests date trunc behavior to verify that a date_trunc to WEEK returns the date for MONDAY.

    This is needed because some engines default to Monday and some default to Sunday for their week start.
    On a human level, both are reasonable, so we coerce to the ISO standard of Monday.
    """
    date_trunc_stmt = sql_client.sql_query_plan_renderer.expr_renderer.render_sql_expr(
        _build_date_trunc_expression(date_string=input, time_granularity=TimeGranularity.WEEK)
    ).sql

    df = sql_client.query(f"SELECT {date_trunc_stmt}")

    actual = pd.Timestamp(_extract_dataframe_value(df=df))
    assert expected == actual


def _build_extract_expression(date_string: str, date_part: DatePart) -> SqlExtractExpression:
    cast_expr = SqlCastToTimestampExpression(SqlStringLiteralExpression(literal_value=date_string))
    return SqlExtractExpression(date_part=date_part, arg=cast_expr)


def test_date_part_year(sql_client: SqlClient) -> None:
    """Tests date_part or extract behavior for year.

    This is necessary because ISOYEAR semantics might return the subsequent (or preceding) year depending
    on the date instead of the calendar year.
    """
    # The ISO year start for 2015 is 2014-12-29, so this should return 2015 for ISOYEAR
    ISO_DATE_STRING = "2014-12-30"
    # We, however, expect the calendar year
    expected = 2014
    extract_stmt = sql_client.sql_query_plan_renderer.expr_renderer.render_sql_expr(
        _build_extract_expression(date_string=ISO_DATE_STRING, date_part=DatePart.YEAR)
    ).sql

    df = sql_client.query(f"SELECT {extract_stmt}")

    actual = _extract_dataframe_value(df=df)
    assert expected == actual


@pytest.mark.parametrize(
    ("input", "expected"),
    (
        ("2015-02-22", 1),
        ("2015-06-30", 2),
        ("2015-07-01", 3),
        ("2015-11-17", 4),
    ),
)
def test_date_part_quarter(sql_client: SqlClient, input: str, expected: int) -> None:
    """Tests date_part or extract behavior for quarter.

    We expect the quarter boundaries to line up and results to be in [1, 4] corresponding to the natural
    4 calendar quarters.
    """
    extract_stmt = sql_client.sql_query_plan_renderer.expr_renderer.render_sql_expr(
        _build_extract_expression(date_string=input, date_part=DatePart.QUARTER)
    ).sql

    df = sql_client.query(f"SELECT {extract_stmt}")

    actual = _extract_dataframe_value(df=df)
    assert expected == actual


def test_date_part_day_of_year(sql_client: SqlClient) -> None:
    """Tests date_part or extract behavior for day of year.

    Ensures we are, in fact, starting our calendar year on January 1st.
    """
    # The ISO year start for 2015 is 2014-12-29, so this should return 2 if day of year is bound to ISO.
    ISO_DATE_STRING = "2014-12-30"
    # We, however, expect the calendar year, so it should return 364
    expected = 364
    extract_stmt = sql_client.sql_query_plan_renderer.expr_renderer.render_sql_expr(
        _build_extract_expression(date_string=ISO_DATE_STRING, date_part=DatePart.DOY)
    ).sql

    df = sql_client.query(f"SELECT {extract_stmt}")

    actual = _extract_dataframe_value(df=df)
    assert expected == actual


@pytest.mark.parametrize(
    ("input", "expected"),
    (
        ("2023-08-28", 1),  # Monday
        ("2023-08-29", 2),  # Tuesday
        ("2023-08-30", 3),  # Wednesday
        ("2023-08-31", 4),  # Thursday
        ("2023-09-01", 5),  # Friday
        ("2023-09-02", 6),  # Saturday
        ("2023-09-03", 7),  # Sunday
    ),
)
def test_date_part_day_of_week(sql_client: SqlClient, input: str, expected: int) -> None:
    """Tests date_part or extract behavior for day of week."""
    extract_stmt = sql_client.sql_query_plan_renderer.expr_renderer.render_sql_expr(
        _build_extract_expression(date_string=input, date_part=DatePart.DOW)
    ).sql

    df = sql_client.query(f"SELECT {extract_stmt}")

    actual = _extract_dataframe_value(df=df)
    assert expected == actual
