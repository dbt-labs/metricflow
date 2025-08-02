from __future__ import annotations

import logging
from collections import defaultdict
from collections.abc import Mapping, Sequence
from typing import Mapping, Sequence

from metricflow_semantics.mf_logging.lazy_formattable import LazyFormat

logger = logging.getLogger(__name__)


class EqualColumnWidthTableFormatter:
    def __init__(self) -> None:
        self._column_name_to_max_width: dict[str, int] = defaultdict(int)

    def add_rows(self, rows: Sequence[Mapping[str, str]]) -> None:
        for row in rows:
            for column_name, value in row.items():
                self._column_name_to_max_width[column_name] = max(
                    self._column_name_to_max_width[column_name], len(value)
                )

    def add_headers(self, headers: Sequence[str]) -> None:
        for column_name in headers:
            self._column_name_to_max_width[column_name] = max(
                self._column_name_to_max_width[column_name], len(column_name)
            )

    def reformat_rows(self, rows: Sequence[Mapping[str, str]]) -> list[dict[str, str]]:
        new_rows: list[dict[str, str]] = []

        for row in rows:
            new_row = {}
            for column_name, value in row.items():
                max_width = self._column_name_to_max_width.get(column_name)

                if max_width is None:
                    raise ValueError(
                        LazyFormat(
                            "Unknown key",
                            column_name=column_name,
                            known_column_names=self._column_name_to_max_width.keys(),
                        )
                    )
                new_row[column_name] = value.ljust(max_width)
            new_rows.append(new_row)

        return new_rows
