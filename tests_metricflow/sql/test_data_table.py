from __future__ import annotations

import logging

import pytest

from metricflow.data_table.mf_table import MetricFlowDataTable
from tests_metricflow.sql.compare_data_table import check_data_tables_are_equal

logger = logging.getLogger(__name__)


def test_sorted() -> None:  # noqa: D103:
    expected_table = MetricFlowDataTable.create_from_rows(
        column_names=["a", "b"],
        rows=[(1, 2), (3, 4)],
    )
    actual_table = MetricFlowDataTable.create_from_rows(
        column_names=["b", "a"],
        rows=[(4, 3), (2, 1)],
    ).sorted()

    check_data_tables_are_equal(
        expected_table=expected_table,
        actual_table=actual_table,
        ignore_order=False,
    )


def test_mismatch() -> None:  # noqa: D103:
    expected_table = MetricFlowDataTable.create_from_rows(
        column_names=["a", "b"],
        rows=[(1, 2), (3, 4)],
    )
    actual_table = MetricFlowDataTable.create_from_rows(
        column_names=["a", "b"],
        rows=[(1, 2), (3, 5)],
    )

    with pytest.raises(ValueError):
        check_data_tables_are_equal(
            expected_table=expected_table,
            actual_table=actual_table,
            ignore_order=False,
        )
