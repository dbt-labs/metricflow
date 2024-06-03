from __future__ import annotations

import logging
import math
from typing import SupportsFloat

import pandas as pd

from metricflow.data_table.mf_table import MetricFlowDataTable
from tests_metricflow.sql.compare_data_table import check_data_tables_are_equal

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
            # NaNs can't be compared for equality.
            if pd.isna(expected.iloc[c, r]) and pd.isna(actual.iloc[c, r]):
                pass
            elif isinstance(expected.iloc[c, r], SupportsFloat) and isinstance(actual.iloc[c, r], SupportsFloat):
                if not math.isclose(expected.iloc[c, r], actual.iloc[c, r], rel_tol=1e-6):
                    return False
            elif (
                isinstance(expected.iloc[c, r], pd.Timestamp)
                and isinstance(actual.iloc[c, r], pd.Timestamp)
                # If expected has no tz but actual is UTC, remove timezone. Some engines add UTC by default.
                and actual.iloc[c, r].tzname() == "UTC"
                and expected.iloc[c, r].tzname() is None
            ):
                if actual.iloc[c, r].tz_localize(None) != expected.iloc[c, r].tz_localize(None):
                    return False
            elif expected.iloc[c, r] != actual.iloc[c, r]:
                return False
    return True


def assert_data_tables_equal(
    actual: MetricFlowDataTable,
    expected: MetricFlowDataTable,
    sort_columns: bool = True,
    allow_empty: bool = False,
    compare_names_using_lowercase: bool = False,
) -> None:
    """Check that contents of DataTables are the same.

    If sort_columns is set to false, value and column order needs to be the same.
    If compare_names_using_lowercase is set to True, we copy the data_tables and lower-case their names.
    This is useful for Snowflake query output comparisons.
    """
    check_data_tables_are_equal(
        expected_table=expected,
        actual_table=actual,
        ignore_order=sort_columns,
        allow_empty=allow_empty,
        compare_column_names_using_lowercase=compare_names_using_lowercase,
    )
