from __future__ import annotations

import logging

from metricflow.data_table.mf_table import MetricFlowDataTable
from tests_metricflow.sql.compare_data_table import check_data_tables_are_equal

logger = logging.getLogger(__name__)


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
