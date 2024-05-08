from __future__ import annotations

from dataclasses import dataclass
from functools import cached_property
from typing import Iterator, Sequence, Tuple, Type

from metricflow.data_table.column_types import CellValue, MetricflowNoneType


@dataclass(frozen=True)
class ColumnDescription:
    """Describes a single column in a data table."""

    column_name: str
    column_type: Type[CellValue]

    def __post_init__(self) -> None:  # noqa: D105
        assert (
            self.column_name is not None
        ), f"Unexpected {self.column_name=}. `Any` types may have resulted in missed checks"
        assert (
            self.column_type is not None
        ), f"Unexpected {self.column_type=}. `Any` types may have resulted in missed checks"

    def lower_case_column_name(self) -> ColumnDescription:  # noqa: D102
        return ColumnDescription(
            column_name=self.column_name.lower(),
            column_type=self.column_type,
        )

    @staticmethod
    def normalize_column_types(left_type: Type[CellValue], right_type: Type[CellValue]) -> Type[CellValue]:
        """Normalize the column types for comparison.

        * Normalizing means to convert the types to a common type so that tables can be more easily compared.
        * NoneType and AnyOtherType -> AnyOtherType
        * float and int -> float
        """
        if left_type is right_type:
            return left_type
        # If either type is NoneType, then use the other type since by definition, values can be None.
        # A column can have the NoneType when creating a table where the column value is all None, so we can't
        # infer the type.
        elif left_type is MetricflowNoneType:
            normalized_type = right_type
        elif right_type is MetricflowNoneType:
            normalized_type = left_type
        elif {left_type, right_type} == {int, float}:
            normalized_type = float
        # If they're different types, then strings can always be used.
        else:
            normalized_type = str

        return normalized_type


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
