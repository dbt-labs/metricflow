from __future__ import annotations

import logging
from decimal import Decimal

import pytest

from metricflow.data_table.mf_table import MetricFlowDataTable
from tests_metricflow.sql.compare_data_table import check_data_tables_are_equal

logger = logging.getLogger(__name__)


@pytest.fixture
def example_table() -> MetricFlowDataTable:  # noqa: D103
    return MetricFlowDataTable.create_from_rows(
        column_names=["col_0", "col_1"],
        rows=[
            (0, "a"),
            (1, "b"),
        ],
    )


def test_properties(example_table: MetricFlowDataTable) -> None:  # noqa: D103
    assert example_table.column_count == 2
    assert example_table.row_count == 2
    assert tuple(example_table.column_names) == ("col_0", "col_1")


def test_input_type(example_table: MetricFlowDataTable) -> None:  # noqa: D103
    table_from_decimals = MetricFlowDataTable.create_from_rows(
        column_names=["col_0", "col_1"],
        rows=[
            (Decimal(0), "a"),
            (Decimal(1), "b"),
        ],
    )

    table_from_floats = MetricFlowDataTable.create_from_rows(
        column_names=["col_0", "col_1"],
        rows=[
            (0.0, "a"),
            (1.0, "b"),
        ],
    )

    assert table_from_decimals.rows == table_from_floats.rows


def test_invalid_row_length() -> None:  # noqa: D103
    with pytest.raises(ValueError):
        MetricFlowDataTable.create_from_rows(
            column_names=["col_0", "col_1"],
            rows=(
                (1, "a"),
                (2,),
            ),
        )


def test_invalid_cell_type(example_table: MetricFlowDataTable) -> None:  # noqa: D103
    with pytest.raises(ValueError):
        MetricFlowDataTable.create_from_rows(
            column_names=["col_0", "col_1"],
            rows=(
                (1, "a"),
                (2, 1.0),
            ),
        )


def test_column_name_index(example_table: MetricFlowDataTable) -> None:  # noqa: D103
    assert example_table.column_name_index("col_0") == 0
    assert example_table.column_name_index("col_1") == 1
    with pytest.raises(ValueError):
        example_table.column_name_index("invalid_index")


def test_sorted() -> None:  # noqa: D103:
    expected_table = MetricFlowDataTable.create_from_rows(
        column_names=["a", "b"],
        rows=[(0, 1), (2, 3)],
    )
    actual_table = MetricFlowDataTable.create_from_rows(
        column_names=["b", "a"],
        rows=[(3, 2), (1, 0)],
    ).sorted()

    check_data_tables_are_equal(
        expected_table=expected_table,
        actual_table=actual_table,
        ignore_order=False,
    )


def test_mismatch() -> None:  # noqa: D103:
    expected_table = MetricFlowDataTable.create_from_rows(
        column_names=["a", "b"],
        rows=[(0, 1), (2, 3)],
    )
    actual_table = MetricFlowDataTable.create_from_rows(
        column_names=["a", "b"],
        rows=[(0, 1), (2, 4)],
    )

    with pytest.raises(ValueError):
        check_data_tables_are_equal(
            expected_table=expected_table,
            actual_table=actual_table,
            ignore_order=False,
        )


def test_get_cell_value(example_table: MetricFlowDataTable) -> None:  # noqa: D103
    assert example_table.get_cell_value(0, 0) == 0
    assert example_table.get_cell_value(0, 1) == "a"
    assert example_table.get_cell_value(1, 0) == 1
    assert example_table.get_cell_value(1, 1) == "b"


def test_column_values_iterator(example_table: MetricFlowDataTable) -> None:  # noqa: D103
    assert tuple(example_table.column_values_iterator(0)) == (0, 1)
    assert tuple(example_table.column_values_iterator(1)) == ("a", "b")
