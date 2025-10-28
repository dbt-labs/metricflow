from __future__ import annotations

import html
from contextlib import contextmanager
from typing import Iterator, Optional

from typing_extensions import Self

from metricflow_semantics.toolkit.mf_graph.formatting.graphviz_html import (
    GraphvizHtmlAlignment,
    GraphvizHtmlText,
)
from metricflow_semantics.toolkit.mf_logging.lazy_formattable import LazyFormat
from metricflow_semantics.toolkit.mf_type_aliases import AnyLengthTuple
from metricflow_semantics.toolkit.string_helpers import mf_indent


class GraphvizHtmlTableBuilder:
    """Builds "HTML" for `graphviz` labels.

    Note that the "HTML" for `graphviz` is not exactly like standard HTML.
    See: https://graphviz.org/doc/info/shapes.html#html
    """

    def __init__(self, table_cell_padding: int = 0, table_border: int = 0) -> None:  # noqa: D107
        self._table_border = table_border
        self._table_cell_padding = table_cell_padding
        self._table_cell_spacing = 0
        # Lines of text that should be inside the `<TABLE>` tag.
        self._inner_lines: list[str] = []
        # When adding to `_inner_lines`, each line should be indented to this level.
        self._current_line_indent_level = 0
        self._build_result: Optional[AnyLengthTuple[str]] = None

    def _add_line(self, line: str) -> None:
        """Indent and add a line of text that should be output by `build()`."""
        self._inner_lines.append(mf_indent(line, indent_level=self._current_line_indent_level))

    @contextmanager
    def _indented_section(self) -> Iterator[None]:
        """Use this context to make lines added by `_add_line()` indented by `_current_line_indent_level`."""
        self._current_line_indent_level += 1
        yield None
        self._current_line_indent_level -= 1

    @contextmanager
    def new_row_builder(self) -> Iterator[_GraphvizHtmlTableRowBuilder]:
        """Return a builder for adding a new row to the table."""
        if self._build_result is not None:
            raise RuntimeError(
                LazyFormat("Can't add a row as this has already been built", result_lines=self._build_result)
            )
        row_builder = _GraphvizHtmlTableRowBuilder()
        yield row_builder
        with self._indented_section():
            for line in row_builder.build_lines():
                self._add_line(line)

    def build_lines(self) -> AnyLengthTuple[str]:
        """Return the lines corresponding to the table that this describes.

        This can be only called once per builder instance.
        """
        if self._build_result is not None:
            raise RuntimeError(LazyFormat("This has already been built", build_result=self._build_result))
        if len(self._inner_lines) == 0:
            return ()
        return (
            (
                "<<TABLE"
                f' BORDER="{self._table_border}"'
                f' CELLPADDING="{self._table_cell_padding}"'
                f' CELLSPACING="{self._table_cell_spacing}"'
                f">",
            )
            + tuple(self._inner_lines)
            + ("</TABLE>>",)
        )

    def build(self) -> str:
        """Similar to `.build_lines()` but returns a single string."""
        return "\n".join(self.build_lines())


class _GraphvizHtmlTableRowBuilder:
    """Builder for a row in the table."""

    def __init__(self) -> None:
        self._current_lines: list[str] = ["<TR>"]
        self._build_result: Optional[AnyLengthTuple[str]] = None

    def add_column(
        self,
        column_text: GraphvizHtmlText = GraphvizHtmlText(),
        column_span: Optional[int] = None,
        alignment: Optional[GraphvizHtmlAlignment] = None,
        cell_padding: Optional[int] = None,
        border: Optional[int] = None,
        width: Optional[int] = None,
    ) -> Self:
        """Adds a column with the given options to the row being built."""
        if self._build_result:
            raise RuntimeError(LazyFormat("Can't add as this has already been built", result_lines=self._current_lines))
        html_element_attributes: dict[str, str] = {}
        if column_span is not None:
            if column_span <= 0:
                raise ValueError(LazyFormat("Column span must be positive", column_span=column_span))
            html_element_attributes["COLSPAN"] = str(column_span)
        if alignment is not None:
            html_element_attributes["ALIGN"] = str(alignment.value)
        if cell_padding is not None:
            html_element_attributes["CELLPADDING"] = str(cell_padding)
        if border is not None:
            html_element_attributes["BORDER"] = str(border)
        if width is not None:
            html_element_attributes["WIDTH"] = str(width)

        html_element_items = ["TD"]
        for key, value in html_element_attributes.items():
            html_element_items.append(f'{key}="{html.escape(value)}"')

        self._current_lines.append(
            mf_indent(
                "".join(
                    [
                        "<" + " ".join(html_element_items) + ">",
                        str(column_text),
                        "</TD>",
                    ]
                )
            )
        )
        return self

    def build_lines(self) -> AnyLengthTuple[str]:
        if self._build_result is not None:
            raise RuntimeError(
                LazyFormat("Can't build as this has already been built", build_result=self._build_result)
            )
        self._current_lines.append("</TR>")
        self._build_result = tuple(self._current_lines)
        return self._build_result
