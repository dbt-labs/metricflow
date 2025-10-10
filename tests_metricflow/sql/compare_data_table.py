from __future__ import annotations

import datetime
import difflib
import math
from dataclasses import dataclass
from typing import Dict, Optional, SupportsFloat

from metricflow_semantics.toolkit.mf_logging.pretty_print import PrettyFormatDictOption, mf_pformat_dict

from metricflow.data_table.column_types import CellValue
from metricflow.data_table.mf_table import MetricFlowDataTable


def _generate_table_diff_fields(
    expected_table: MetricFlowDataTable, actual_table: MetricFlowDataTable
) -> Dict[str, str]:
    differ = difflib.Differ()
    expected_table_text = expected_table.text_format()
    actual_table_text = actual_table.text_format()
    diff = differ.compare(expected_table_text.splitlines(keepends=True), actual_table_text.splitlines(keepends=True))
    return {
        "expected_table": expected_table_text,
        "actual_table": actual_table_text,
        "expected_table_to_actual_table_diff": "".join(diff),
    }


@dataclass(frozen=True)
class DataTableMismatch:
    """Describes a mismatch in a cell between two tables."""

    message: str
    row_index: int
    column_index: int
    expected_value: CellValue
    actual_value: CellValue


def _check_table_cells_for_mismatch(
    expected_table: MetricFlowDataTable, actual_table: MetricFlowDataTable
) -> Optional[DataTableMismatch]:
    for row_index in range(expected_table.row_count):
        for column_index in range(expected_table.column_count):
            # NaNs can't be compared for equality.
            expected_value = expected_table.get_cell_value(row_index, column_index)
            actual_value = actual_table.get_cell_value(row_index, column_index)
            if isinstance(expected_value, SupportsFloat) and isinstance(actual_value, SupportsFloat):
                if math.isnan(expected_value) and math.isnan(actual_value):
                    continue
                if not math.isclose(expected_value, actual_value, rel_tol=1e-6):
                    return DataTableMismatch(
                        message="`SupportsFloat` value mismatch",
                        row_index=row_index,
                        column_index=column_index,
                        expected_value=expected_value,
                        actual_value=actual_value,
                    )
            # It should be safe to remove this once `MetricFlowDataTable` is validated as it doesn't allow timezones.
            elif (
                isinstance(expected_value, datetime.datetime)
                and isinstance(actual_value, datetime.datetime)
                # If expected has no tz but actual is UTC, remove timezone. Some engines add UTC by default.
                and (actual_value.tzinfo == "UTC" and expected_value.tzinfo is None)
            ):
                if expected_value.replace(tzinfo=None) != actual_value.replace(tzinfo=None):
                    return DataTableMismatch(
                        message="`datetime` value mismatch",
                        row_index=row_index,
                        column_index=column_index,
                        expected_value=expected_value,
                        actual_value=actual_value,
                    )
            elif expected_value != actual_value:
                return DataTableMismatch(
                    message="Value mismatch",
                    row_index=row_index,
                    column_index=column_index,
                    expected_value=expected_value,
                    actual_value=actual_value,
                )

    return None


def check_data_tables_are_equal(
    expected_table: MetricFlowDataTable,
    actual_table: MetricFlowDataTable,
    ignore_order: bool = True,
    allow_empty: bool = False,
    compare_column_names_using_lowercase: bool = False,
) -> None:
    """Check if this is equal to another table. If not, raise an exception.

    This was migrated from an existing implementation based on `pandas` data_tables.
    """
    if compare_column_names_using_lowercase:
        expected_table = expected_table.with_lower_case_column_names()
        actual_table = actual_table.with_lower_case_column_names()

    # Sort after case changes since the order can change after a case change. e.g. underscore comes
    # before lowercase.
    if ignore_order:
        expected_table = expected_table.sorted()
        actual_table = actual_table.sorted()

    if expected_table.column_names != actual_table.column_names:
        raise ValueError(
            mf_pformat_dict(
                "Column descriptions do not match.",
                {
                    "expected_table_column_names": expected_table.column_names,
                    "actual_table_column_names": actual_table.column_names,
                },
            )
        )

    if expected_table.row_count != actual_table.row_count:
        raise ValueError(
            mf_pformat_dict(
                "Row counts do not match.",
                dict(
                    **{
                        "expected_table_row_count": expected_table.row_count,
                        "actual_table_row_count": actual_table.row_count,
                    },
                    **_generate_table_diff_fields(expected_table=expected_table, actual_table=actual_table),
                ),
                format_option=PrettyFormatDictOption(preserve_raw_strings=True),
            )
        )

    if not allow_empty and expected_table.row_count == 0:
        raise ValueError(
            mf_pformat_dict(
                f"Expected table is empty and {allow_empty=}. This may indicate an error in configuring the test.",
                _generate_table_diff_fields(expected_table=expected_table, actual_table=actual_table),
                format_option=PrettyFormatDictOption(preserve_raw_strings=True),
            )
        )

    mismatch = _check_table_cells_for_mismatch(expected_table=expected_table, actual_table=actual_table)

    if mismatch is not None:
        raise ValueError(
            mf_pformat_dict(
                mismatch.message,
                dict(
                    **{
                        "row_index": mismatch.row_index,
                        "column_index": mismatch.column_index,
                        "expected_value": mismatch.expected_value,
                        "actual_value": mismatch.actual_value,
                    },
                    **_generate_table_diff_fields(expected_table=expected_table, actual_table=actual_table),
                ),
                format_option=PrettyFormatDictOption(preserve_raw_strings=True),
            )
        )

    return


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
