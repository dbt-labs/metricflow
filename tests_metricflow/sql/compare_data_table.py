from __future__ import annotations

import datetime
import difflib
import itertools
import math
from dataclasses import dataclass
from typing import Dict, Optional, SupportsFloat

from metricflow_semantics.mf_logging.pretty_print import mf_pformat_many

from metricflow.data_table.column_types import CellValue
from metricflow.data_table.mf_column import ColumnDescription
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
    if ignore_order:
        expected_table = expected_table.sorted()
        actual_table = actual_table.sorted()

    if compare_column_names_using_lowercase:
        expected_table = expected_table.lower_case_column_names()
        actual_table = actual_table.lower_case_column_names()

    if expected_table.column_names != actual_table.column_names:
        raise ValueError(
            mf_pformat_many(
                "Column descriptions do not match.",
                {
                    "expected_table_column_names": expected_table.column_names,
                    "actual_table_column_names": actual_table.column_names,
                },
            )
        )

    if expected_table.column_descriptions != actual_table.column_descriptions:
        normalized_column_types = tuple(
            ColumnDescription.normalize_column_types(
                expected_table_column_description.column_type,
                actual_table_column_description.column_type,
            )
            for expected_table_column_description, actual_table_column_description in itertools.zip_longest(
                expected_table.column_descriptions, actual_table.column_descriptions
            )
        )
        actual_table = actual_table.with_column_types(normalized_column_types)
        expected_table = expected_table.with_column_types(normalized_column_types)

    if expected_table.row_count != actual_table.row_count:
        raise ValueError(
            mf_pformat_many(
                "Row counts do not match.",
                dict(
                    **{
                        "expected_table_row_count": expected_table.row_count,
                        "actual_table_row_count": actual_table.row_count,
                    },
                    **_generate_table_diff_fields(expected_table=expected_table, actual_table=actual_table),
                ),
                preserve_raw_strings=True,
            )
        )

    if not allow_empty and expected_table.row_count == 0:
        raise ValueError(
            mf_pformat_many(
                f"Expected table is empty and {allow_empty=}. This may indicate an error in configuring the test.",
                _generate_table_diff_fields(expected_table=expected_table, actual_table=actual_table),
                preserve_raw_strings=True,
            )
        )
    mismatch = _check_table_cells_for_mismatch(expected_table=expected_table, actual_table=actual_table)

    if mismatch is not None:
        raise ValueError(
            mf_pformat_many(
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
                preserve_raw_strings=True,
            )
        )

    return

    # if expected_table.column_descriptions != actual_table.column_descriptions:
    #     raise ValueError(
    #         mf_pformat_many(
    #             f"Column descriptions do not match.",
    #             {
    #                 "self_column_descriptions": expected_table.column_descriptions,
    #                 "other_column_descriptions": actual_table.column_descriptions,
    #             }
    #         )
    #     )
    #
    # if expected_table.row_count != actual_table.row_count:
    #     raise ValueError(
    #         f"Row count mismatch. Expected table has {expected_table.row_count} rows, but the actual table has "
    #         f"{actual_table.row_count} rows."
    #         f"\n"
    #         f"\n{_diff_tables(expected_table, actual_table)}"
    #
    #     )
    #
    #
    #
    # error_message = None
    # for row_index in range(expected_table.row_count):
    #     for column_index in range(expected_table.column_count):
    #         # NaNs can't be compared for equality.
    #         expected_value = expected_table.get_cell_value(row_index, column_index)
    #         actual_value = actual_table.get_cell_value(row_index, column_index)
    #         if math.isnan(expected_value) and math.isnan(actual_value):
    #             continue
    #         elif isinstance(expected_value, SupportsFloat) and isinstance(actual_value, SupportsFloat):
    #             if not math.isclose(expected_value, actual_value, rel_tol=1e-6):
    #                 error_message = (
    #                     f"Float value mismatch at {row_index=}, {column_index=}. "
    #                     f"Expected value: {expected_value} Actual Value: {actual_value}"
    #                 )
    #         elif (
    #                 isinstance(expected_value, datetime.datetime)
    #                 and isinstance(actual_table, datetime.datetime)
    #                 # If expected has no tz but actual is UTC, remove timezone. Some engines add UTC by default.
    #                 and actual_value.tzinfo == "UTC"
    #                 and expected_value.tzinfo is None
    #         ):
    #             if expected_value.replace(tzinfo=None) != actual_value.replace(tzinfo=None):
    #                 error_message = (
    #                     f"datetime value mismatch at {row_index=}, {column_index=}. "
    #                     f"Expected value: {expected_value.isoformat()} Actual Value: {actual_value.isoformat()}"
    #                 )
    #         elif expected_value != actual_value:
    #             error_message = (
    #                 f"Value mismatch at {row_index=}, {column_index=}. "
    #                 f"Expected value: {repr(expected_value)} Actual Value: {repr(actual_value)}"
    #             )
    #
    #         if error_message is not None:
    #             break
    #     if error_message is not None:
    #         break

    # if error_message is not None:
    #     raise ValueError("\n".join([error_message, _diff_tables(expected_table, actual_table)]))
