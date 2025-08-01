from __future__ import annotations

import difflib
import logging

from metricflow_semantics.helpers.table_helpers import IsolatedTabulateRunner
from metricflow_semantics.mf_logging.lazy_formattable import LazyFormat
from metricflow_semantics.model.semantics.linkable_element_set_base import BaseLinkableElementSet

from tests_metricflow_semantics.experimental.semantic_graph.table_helpers2 import EqualColumnWidthTableFormatter
from tests_metricflow_semantics.experimental.semantic_graph.test_helpers import convert_linkable_element_set_to_rows

logger = logging.getLogger(__name__)


def assert_linkable_element_sets_equal(  # noqa: D103
    left_set: BaseLinkableElementSet,
    right_set: BaseLinkableElementSet,
) -> None:
    headers = ("Dunder Name", "Metric-Subquery Entity-Links", "Type", "Properties", "Derived-From Semantic Models")

    left_rows = convert_linkable_element_set_to_rows(left_set)
    right_rows = convert_linkable_element_set_to_rows(right_set)

    equalizer = EqualColumnWidthTableFormatter()

    equalizer.add_headers(headers)
    equalizer.add_rows(left_rows)
    equalizer.add_rows(right_rows)

    new_left_rows = equalizer.reformat_rows(left_rows)
    new_right_rows = equalizer.reformat_rows(right_rows)

    left_table = IsolatedTabulateRunner.tabulate(tabular_data=new_left_rows, headers="keys", tablefmt="simple_outline")
    right_table = IsolatedTabulateRunner.tabulate(
        tabular_data=new_right_rows, headers="keys", tablefmt="simple_outline"
    )

    if left_table != right_table:
        diff_lines = difflib.unified_diff(
            a=left_table.splitlines(keepends=True),
            b=right_table.splitlines(keepends=True),
            fromfile="Left Result",
            tofile="Right Result",
        )
        diff = "".join(diff_lines)
        assert False, LazyFormat(
            "Mismatch between left and right sets", left_table=left_table, right_table=right_table, diff=diff
        )

    logger.debug(LazyFormat("Left and right sets match", spec_table=left_table))
