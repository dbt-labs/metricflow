from __future__ import annotations

import datetime
import decimal
from typing import Sequence, Tuple, Type, Union

CellValue = Union[int, float, str, datetime.datetime, bool, None]
InputCellValue = Union[int, float, str, datetime.datetime, bool, None, decimal.Decimal, datetime.date]
MetricflowNoneType = type(None)


def row_cell_types(row: Sequence[CellValue]) -> Tuple[Type[CellValue], ...]:
    """Return the cell type / column type for the objects in the row."""
    return tuple(cell_type(cell) for cell in row)


def cell_type(cell_value: CellValue) -> Type[CellValue]:
    """Return the cell type / column type for the object in the cell."""
    return type(cell_value)
