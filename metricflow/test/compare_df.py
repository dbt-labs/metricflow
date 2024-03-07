from __future__ import annotations

import datetime
import logging
import math
from typing import SupportsFloat

import pandas as pd

logger = logging.getLogger(__name__)


def _dataframes_contain_same_data(
    actual: pd.DataFrame,
    expected: pd.DataFrame,
) -> bool:
    """Compare all elements of the dataframe one by one for equality.

    We use math.isclose() to avoid issues with float comparisons.
    """
    if expected.shape != actual.shape:
        return False

    for c in range(expected.shape[0]):
        for r in range(expected.shape[1]):
            expected_value = expected.iloc[c, r]
            actual_value = actual.iloc[c, r]

            # NaNs can't be compared for equality.
            if pd.isna(expected_value) and pd.isna(actual_value):
                continue

            if (
                isinstance(expected_value, SupportsFloat)
                and isinstance(actual_value, SupportsFloat)
                and math.isclose(expected_value, actual_value, rel_tol=1e-6)
            ):
                continue

            # Convert dates to timestamps as Pandas will give a warning when comparing dates and timestamps.
            if isinstance(expected_value, datetime.date):
                expected_value = pd.Timestamp(expected_value)

            if isinstance(actual_value, datetime.date):
                actual_value = pd.Timestamp(actual_value)

            if (
                isinstance(expected_value, pd.Timestamp)
                and isinstance(actual_value, pd.Timestamp)
                # If expected has no tz but actual is UTC, remove timezone. Some engines add UTC by default.
                and actual_value.tzname() == "UTC"
                and expected_value.tzname() is None
            ):
                actual_value = actual_value.tz_localize(None)
                expected_value = expected_value.tz_localize(None)

            if expected_value != actual_value:
                return False
    return True


def assert_dataframes_equal(
    actual: pd.DataFrame,
    expected: pd.DataFrame,
    sort_columns: bool = True,
    allow_empty: bool = False,
    compare_names_using_lowercase: bool = False,
) -> None:
    """Check that contents of DataFrames are the same.

    If sort_columns is set to false, value and column order needs to be the same.
    If compare_names_using_lowercase is set to True, we copy the dataframes and lower-case their names.
    This is useful for Snowflake query output comparisons.
    """
    if compare_names_using_lowercase:
        actual = actual.copy()
        expected = expected.copy()
        actual.columns = actual.columns.str.lower()
        expected.columns = expected.columns.str.lower()

    if set(actual.columns) != set(expected.columns):
        raise ValueError(
            f"DataFrames do not contain the same columns. actual: {set(actual.columns)}, "
            f"expected: {set(expected.columns)}"
        )

    if not allow_empty and actual.shape[0] == 0 and expected.shape[0] == 0:
        raise AssertionError("Both dataframes have no rows; likely there is a mistake with the test")

    if sort_columns:
        sort_by = list(sorted(actual.columns.tolist()))
        expected = expected.loc[:, sort_by].sort_values(sort_by).reset_index(drop=True)
        actual = actual.loc[:, sort_by].sort_values(sort_by).reset_index(drop=True)

    if not _dataframes_contain_same_data(actual=actual, expected=expected):
        raise ValueError(
            f"Dataframes not equal.\n"
            f"Expected:\n{expected.to_markdown(index=False)}"
            "\n---\n"
            f"Actual:\n{actual.to_markdown(index=False)}"
        )
