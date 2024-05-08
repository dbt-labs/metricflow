from __future__ import annotations

import datetime
import decimal
import itertools
import logging
from dataclasses import dataclass
from typing import Iterable, Iterator, List, Optional, Sequence, SupportsFloat, Tuple, Type

import dateutil.parser
import tabulate
from metricflow_semantics.mf_logging.formatting import indent
from metricflow_semantics.mf_logging.pretty_print import mf_pformat, mf_pformat_many
from typing_extensions import Self

from metricflow.data_table.column_types import CellValue, InputCellValue, row_cell_types
from metricflow.data_table.mf_column import ColumnDescription

logger = logging.getLogger(__name__)


@dataclass(frozen=True, eq=False)
class MetricFlowDataTable:
    """Container for tabular data stored in memory.

    Don't use `=` to compare tables as there many be NaNs.
    """

    column_descriptions: Tuple[ColumnDescription, ...]
    rows: Tuple[Tuple[CellValue, ...], ...]

    def __post_init__(self) -> None:  # noqa: D105
        expected_column_count = self.column_count
        for row_index, row in enumerate(self.rows):
            row_column_count = len(row)
            assert row_column_count == expected_column_count, (
                f"Row at index {row_index} has {row_column_count} columns instead of {expected_column_count}. "
                f"Row is:"
                f"\n{indent(mf_pformat(row))}"
            )
            for column_index, cell_value in enumerate(row):
                expected_cell_value_type = self.column_descriptions[column_index].column_type
                assert cell_value is None or isinstance(cell_value, expected_cell_value_type), mf_pformat_many(
                    "Cell value type mismatch.",
                    {
                        "row_index": row_index,
                        "column_index": column_index,
                        "expected_cell_value_type": expected_cell_value_type,
                        "actual_cell_value_type": type(cell_value),
                        "cell_value": cell_value,
                    },
                )

    @property
    def column_count(self) -> int:  # noqa: D102
        return len(self.column_descriptions)

    @property
    def row_count(self) -> int:  # noqa: D102
        return len(self.rows)

    def column_name_index(self, column_name: str) -> int:  # noqa: D102
        for i, column_description in enumerate(self.column_descriptions):
            if column_description.column_name == column_name:
                return i
        raise ValueError(
            f"Unknown column name {repr(column_name)}. Known column names are:"
            f"\n{indent(mf_pformat([column_name for column_name in self.column_descriptions]))}"
        )

    @property
    def column_names(self) -> Sequence[str]:  # noqa: D102
        return tuple(column_description.column_name for column_description in self.column_descriptions)

    def _column_index_for_column_name(self, target_column_name: str) -> int:
        column_names = self.column_names
        for i, column_name in enumerate(column_names):
            if column_name == target_column_name:
                return i
        raise ValueError(
            f"Unknown column name {repr(target_column_name)}. Known column names are:"
            f"\n{indent(mf_pformat(column_names))}"
        )

    def column_values_iterator(self, column_index: int) -> Iterator[CellValue]:  # noqa: D102
        return (row[column_index] for row in self.rows)

    def column_values_iterator_by_name(self, column_name: str) -> Iterator[CellValue]:  # noqa: D102
        return (row[self._column_index_for_column_name(column_name)] for row in self.rows)

    def _sorted_by_column_name(self) -> MetricFlowDataTable:  # noqa: D102
        # row_dict_by_row_index: Dict[int, Dict[str, CellType]] = defaultdict(dict)

        new_rows: List[List[CellValue]] = [[] for _ in range(self.row_count)]
        sorted_column_names = sorted(self.column_names)
        for column_name in sorted_column_names:
            old_column_index = self._column_index_for_column_name(column_name)
            for row_index, cell_value in enumerate(self.column_values_iterator(old_column_index)):
                new_rows[row_index].append(cell_value)

        return MetricFlowDataTable(
            column_descriptions=tuple(
                self.column_descriptions[self._column_index_for_column_name(column_name)]
                for column_name in sorted_column_names
            ),
            rows=tuple(
                tuple(row_dict[column_index] for column_index in range(self.column_count)) for row_dict in new_rows
            ),
        )

    def _sorted_by_row(self) -> MetricFlowDataTable:  # noqa: D102
        def _cell_sort_key(cell: CellValue) -> str:
            if isinstance(cell, datetime.datetime):
                return cell.isoformat()
            return str(cell)

        def _row_sort_key(row: Tuple[CellValue, ...]) -> Tuple[str, ...]:
            return tuple(_cell_sort_key(cell) for cell in row)

        return MetricFlowDataTable(
            column_descriptions=self.column_descriptions,
            rows=tuple(sorted((row for row in self.rows), key=_row_sort_key)),
        )

    def sorted(self) -> MetricFlowDataTable:  # noqa: D102
        return self._sorted_by_column_name()._sorted_by_row()

    def text_format(self, float_decimals: int = 2) -> str:  # noqa: D102
        str_rows: List[List[str]] = []
        for row in self.rows:
            str_row: List[str] = []
            for cell_value in row:
                if isinstance(cell_value, float):
                    str_row.append(f"{cell_value:.{float_decimals}f}")
                    continue

                if isinstance(cell_value, datetime.datetime):
                    if cell_value.time() == datetime.time.min:
                        str_row.append(cell_value.date().isoformat())
                    else:
                        str_row.append(cell_value.isoformat())
                    continue

                str_row.append(str(cell_value))
            str_rows.append(str_row)
        return tabulate.tabulate(
            tabular_data=tuple(tuple(str_row) for str_row in str_rows),
            headers=tuple(column_description.column_name for column_description in self.column_descriptions),
        )

    def lower_case_column_names(self) -> MetricFlowDataTable:  # noqa: D102
        return MetricFlowDataTable(
            column_descriptions=tuple(
                column_description.lower_case_column_name() for column_description in self.column_descriptions
            ),
            rows=self.rows,
        )

    def get_cell_value(self, row_index: int, column_index: int) -> CellValue:  # noqa: D102
        return self.rows[row_index][column_index]

    @staticmethod
    def create_from_rows(  # noqa: D102
        column_names: Sequence[str], rows: Iterable[Sequence[CellValue]]
    ) -> MetricFlowDataTable:
        builder = _MetricFlowDataTableBuilder(column_names)
        for row in rows:
            builder.add_row(row)
        return builder.build()

    @staticmethod
    def create_from_inferred_type_rows(
        column_names: Sequence[str], rows: Iterable[Sequence[str]]
    ) -> MetricFlowDataTable:
        """Create a table from a row where all values have been cast to strings."""
        builder = _MetricFlowDataTableBuilder(column_names)
        for row in rows:
            builder.add_inferred_type_row(row)
        return builder.build()

    def with_column_types(self, new_column_types: Sequence[Type[CellValue]]) -> MetricFlowDataTable:
        """Return this table but with rows converted to the new types."""
        new_column_types_count = len(new_column_types)
        current_column_count = self.column_count
        if new_column_types_count != current_column_count:
            raise ValueError(f"Column count mismatch: {current_column_count=} {new_column_types_count=}")

        new_rows: List[Tuple[CellValue, ...]] = []
        for row in self.rows:
            new_row: List[CellValue] = []
            for new_column_type, cell_value in itertools.zip_longest(new_column_types, row):
                if isinstance(cell_value, new_column_type):
                    new_row.append(cell_value)
                elif new_column_type is float and isinstance(cell_value, SupportsFloat):
                    new_row.append(float(cell_value))
                elif new_column_type is str:
                    new_row.append(str(cell_value))
                else:
                    raise RuntimeError(f"Unhandled case: {new_column_type=}, {cell_value=}")
            new_rows.append(tuple(new_row))

        return MetricFlowDataTable(
            column_descriptions=tuple(
                ColumnDescription(column_name=column_description.column_name, column_type=new_column_type)
                for column_description, new_column_type in itertools.zip_longest(
                    self.column_descriptions, new_column_types
                )
            ),
            rows=tuple(new_rows),
        )


class _MetricFlowDataTableBuilder:
    """Helps build `MetricFlowDataTable`, one row at a time.

    This validates each row as it is input to give better error messages.
    """

    def __init__(self, column_names: Sequence[str]) -> None:  # noqa: D107
        self._rows: List[Tuple[CellValue, ...]] = []
        self._column_names = tuple(column_names)

    def _normalize_column_types_and_build_table(self) -> MetricFlowDataTable:  # noqa: D102
        column_types_so_far: Optional[Tuple[Type[CellValue], ...]] = None
        cell_value: CellValue
        for row in self._rows:
            if column_types_so_far is None:
                column_types_so_far = row_cell_types(row)
                continue

            # If the types of the objects in the row are the same, no need for updates.
            row_column_types = row_cell_types(row)
            if row_column_types == column_types_so_far:
                continue

            # Types of objects in the row are different from what's known so far. Update the column types.
            updated_column_types: List[Type[CellValue]] = []
            for column_type_so_far, cell_type, cell_value in itertools.zip_longest(
                column_types_so_far, row_column_types, row
            ):
                # No change in the inferred column type, so just copy.
                if column_type_so_far is cell_type:
                    updated_column_types.append(column_type_so_far)
                    continue

                normalized_column_type = ColumnDescription.normalize_column_types(column_type_so_far, cell_type)
                updated_column_types.append(normalized_column_type)

            column_types_so_far = tuple(updated_column_types)

        if column_types_so_far is None:
            column_types_so_far = tuple(type(None) for _ in range(len(self._column_names)))

        final_column_types = column_types_so_far

        updated_rows: List[Tuple[CellValue, ...]] = []
        for row in self._rows:
            updated_row: List[CellValue] = []

            for column_type, cell_value in itertools.zip_longest(final_column_types, row):
                if isinstance(cell_value, column_type) or cell_value is None:
                    updated_row.append(cell_value)
                elif column_type is str:
                    updated_row.append(str(cell_value))
                    continue
                elif column_type is float and isinstance(cell_value, SupportsFloat):
                    updated_row.append(float(cell_value))
                else:
                    raise RuntimeError(f"Case not handled: {column_type=} {cell_value=}")
            updated_rows.append(tuple(updated_row))

        return MetricFlowDataTable(
            column_descriptions=tuple(
                ColumnDescription(column_name=column_name, column_type=column_type)
                for column_name, column_type in itertools.zip_longest(self._column_names, final_column_types)
            ),
            rows=tuple(updated_rows),
        )

    def _convert_row_to_base_types(self, row: Sequence[InputCellValue]) -> Sequence[CellValue]:
        updated_row: List[CellValue] = []
        for cell_value in row:
            if isinstance(cell_value, decimal.Decimal):
                # If there is no fractional part, convert to an int. Otherwise, convert to a float.
                if cell_value.as_integer_ratio()[1] == 1:
                    updated_row.append(int(cell_value))
                else:
                    updated_row.append(float(cell_value))
            elif isinstance(cell_value, datetime.date):
                updated_row.append(datetime.datetime.combine(cell_value, datetime.datetime.min.time()))
            else:
                updated_row.append(cell_value)
        return updated_row

    def add_row(self, row: Sequence[InputCellValue]) -> Self:  # noqa: D102
        row = tuple(row)
        expected_column_count = len(self._column_names)
        actual_column_count = len(row)
        if actual_column_count != expected_column_count:
            raise ValueError(
                f"Input row has {actual_column_count} columns, but expected {expected_column_count} columns. Row is:"
                f"\n{indent(mf_pformat(row))}"
            )
        self._rows.append(tuple(self._convert_row_to_base_types(row)))
        return self

    def add_inferred_type_row(self, str_row: Sequence[str]) -> Self:
        """Add a row where all cell values are strings, but they may be other types. e.g. '1' can be 1."""
        return self.add_row(_MetricFlowDataTableBuilder._create_object_row_from_str_row(str_row))

    def build(self) -> MetricFlowDataTable:  # noqa: D102
        return self._normalize_column_types_and_build_table()

    @staticmethod
    def _create_object_row_from_str_row(values: Sequence[str]) -> Tuple[CellValue, ...]:
        return tuple(_MetricFlowDataTableBuilder._convert_string_to_object(value) for value in values)

    @staticmethod
    def _convert_string_to_object(object_str: Optional[str]) -> CellValue:
        if object_str is None:
            return None
        if object_str.lower() == "true":
            return True
        if object_str.lower() == "false":
            return False

        try:
            return int(object_str)
        except ValueError:
            pass

        try:
            return float(object_str)
        except ValueError:
            pass

        try:
            return dateutil.parser.parse(object_str)
        except dateutil.parser.ParserError:
            pass

        return object_str

    # def normalize_column_types(self, other: ColumnDescription) -> ColumnDescription:
    #     """Normalize the column types in `self` with the column types in `other`.
    #
    #     * Column names must be the same.
    #     * Normalizing means to convert the types to a common type so that tables can be more easily compared.
    #     * NoneType and AnyOtherType -> AnyOtherType
    #     """
    #     self_type = self.column_type
    #     other_type = other.column_type
    #     if self_type is other_type:
    #         return self
    #     # If either type is NoneType, then use the other type since by definition, values can be None.
    #     # A column can have the NoneType when creating a table where the column value is all None, so we can't
    #     # infer the type.
    #     elif self_type is MetricflowNoneType:
    #         normalized_type = other_type
    #     elif other_type is MetricflowNoneType:
    #         normalized_type = self_type
    #     # If they're different types, then strings can always be used.
    #     else:
    #         normalized_type = str
    #
    #     return ColumnDescription(
    #         column_name=self.column_name,
    #         column_type=normalized_type,
    #     )
