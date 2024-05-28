from __future__ import annotations

from dataclasses import dataclass
from functools import cached_property
from typing import Iterator, Sequence, Tuple, Type

from metricflow.data_table.column_types import CellValue


@dataclass(frozen=True)
class ColumnDescription:
    """Describes a single column in a data table."""

    column_name: str
    column_type: Type[CellValue]

    def with_lower_case_column_name(self) -> ColumnDescription:  # noqa: D102
        return ColumnDescription(
            column_name=self.column_name.lower(),
            column_type=self.column_type,
        )


@dataclass(frozen=True)
class ColumnDescriptionSet:
    """Describes a collection of columns in a data table."""

    column_descriptions: Tuple[ColumnDescription, ...]

    def __iter__(self) -> Iterator[ColumnDescription]:  # noqa: D105
        return iter(self.column_descriptions)

    @cached_property
    def column_names(self) -> Sequence[str]:  # noqa: D102
        return tuple(column_description.column_name for column_description in self.column_descriptions)

    @cached_property
    def column_types(self) -> Sequence[Type]:  # noqa: D102
        return tuple(column_description.column_type for column_description in self.column_descriptions)
