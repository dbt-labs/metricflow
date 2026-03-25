from __future__ import annotations

import difflib
import logging
from collections import defaultdict
from collections.abc import Mapping, Sequence

from metricflow_semantics.toolkit.mf_logging.lazy_formattable import LazyFormat
from metricflow_semantics.toolkit.table_helpers import IsolatedTabulateRunner

logger = logging.getLogger(__name__)


class PaddedTextTableBuilder:
    """A builder for a pair of text tables to include congruent padding for comparison in tests.

    The issue with comparing text tables is that if there's mismatch due to a long item in only one of the compared
    tables, the width of the entire corresponding table can change. This results in a diff for the entire table.

    This generates tables that pad columns using appropriate whitespace so that in the above case, the diff only
    includes relevant rows.
    """

    def __init__(self, tablefmt: str = "simple_outline") -> None:  # noqa: D107
        self._column_name_to_max_width: dict[str, int] = defaultdict(int)
        self._left_rows: list[Mapping[str, str]] = []
        self._right_rows: list[Mapping[str, str]] = []
        self._tablefmt = tablefmt

    def _record_max_column_widths(self, rows: Sequence[Mapping[str, str]]) -> None:
        """For each row, update the maximum known width of the column name / value."""
        for row in rows:
            for column_name, value in row.items():
                self._column_name_to_max_width[column_name] = max(
                    self._column_name_to_max_width[column_name], len(value), len(column_name)
                )

    def add_left_rows(self, rows: Sequence[Mapping[str, str]]) -> None:  # noqa: D102
        self._record_max_column_widths(rows)
        self._left_rows.extend(rows)

    def add_right_rows(self, rows: Sequence[Mapping[str, str]]) -> None:  # noqa: D102
        self._record_max_column_widths(rows)
        self._right_rows.extend(rows)

    def format_left_table(self) -> str:  # noqa: D102
        return self._format_table(self._left_rows)

    def format_right_table(self) -> str:  # noqa: D102
        return self._format_table(self._right_rows)

    def _format_table(self, rows: Sequence[Mapping[str, str]]) -> str:
        new_rows: list[dict[str, str]] = []
        for row in rows:
            new_row = {}
            for column_name, max_width in self._column_name_to_max_width.items():
                padded_column_value = (row.get(column_name) or "").ljust(max_width)
                padded_column_name = column_name.ljust(max_width)
                new_row[padded_column_name] = padded_column_value
            new_rows.append(new_row)

        return IsolatedTabulateRunner.tabulate(tabular_data=new_rows, headers="keys", tablefmt=self._tablefmt)

    def assert_tables_equal(self, log_table_on_match: bool) -> None:
        """Assert that the left and right tables match.

        If `log_table_on_match` is set, the generated table is logged (for prototyping).
        """
        left_table = self.format_left_table()
        right_table = self.format_right_table()

        if left_table == right_table:
            additional_format_kwargs = {"table": left_table} if log_table_on_match else {}
            logger.debug(LazyFormat("Left and right tables match", **additional_format_kwargs))
            return

        diff_lines = difflib.unified_diff(
            a=left_table.splitlines(keepends=True),
            b=right_table.splitlines(keepends=True),
            fromfile="Left Table",
            tofile="Right Table",
            n=0,
        )

        diff = "".join(diff_lines)
        assert False, LazyFormat(
            "Mismatch between left and right tables",
            left_table=left_table,
            right_table=right_table,
            diff=diff,
        ).evaluated_value
