from __future__ import annotations

import itertools
import logging
import typing
from abc import ABC, abstractmethod

from typing_extensions import override

from metricflow_semantics.toolkit.table_helpers import IsolatedTabulateRunner

if typing.TYPE_CHECKING:
    from metricflow_semantics.test_helpers.performance.profiling import SessionReport

logger = logging.getLogger(__name__)


class SessionReportTextFormatter(ABC):
    """Interface for formatting performance profile reports to text (e.g. table, markdown)."""

    @abstractmethod
    def format_report(self, report: SessionReport) -> str:
        """Format a session report according to this formatter."""
        raise NotImplementedError


class TableTextFormatter(SessionReportTextFormatter):
    """Formats a `SessionReport` to a text table for logs."""

    def __init__(self, row_limit: int = 40) -> None:  # noqa: D107
        self._row_limit = row_limit

    def _justify_and_pad_column(
        self, rows: list[dict[str, str]], column_name: str, left_justify: bool = True, pad_character: str = " "
    ) -> None:
        """Pad / justify the columns e.g. so that the numbers place-values line up.

        If `left_justify` is not set, it will be right justified.
        """
        max_column_length: int = max(itertools.chain((len(column_name),), (len(row[column_name]) for row in rows)))
        justify_character = "<" if left_justify else ">"
        for row in rows:
            previous_value = row[column_name]
            row[column_name] = f"{previous_value:{pad_character}{justify_character}{max_column_length}}"

    @override
    def format_report(self, report: SessionReport) -> str:
        rows: list[dict[str, str]] = []
        for function_report in report.functions.values():
            rows.append(
                {
                    "total": f"{function_report.total_time:.3f}",
                    "body": f"{function_report.body_time:.3f}",
                    "function_name": function_report.function_name,
                    "calls": str(function_report.total_calls),
                    "base_calls": str(function_report.base_calls),
                }
            )
        # Justify numerical columns.
        for column_name in ("total", "body", "calls", "base_calls"):
            self._justify_and_pad_column(rows, column_name, left_justify=False)
        # Add middle dot to make it easier to see the associated row values.
        self._justify_and_pad_column(rows, "function_name", pad_character="·")

        rows.sort(reverse=True, key=lambda row: (row["total"], row["body"], row["calls"], row["base_calls"]))

        items = (
            f"Session ID: {report.session_id}",
            IsolatedTabulateRunner.tabulate(
                tabular_data=rows[: self._row_limit],
                headers="keys",
                tablefmt="simple_outline",
            ),
        )
        return "\n".join(items)
