from dataclasses import InitVar, dataclass, field
from datetime import date, datetime
from typing import Dict, List, Optional, TypeVar, Generic

from metricflow.dataflow.sql_column import SqlColumn, SqlColumnType
from metricflow.dataflow.sql_table import SqlTable
from metricflow.errors.errors import InferenceError
from metricflow.protocols.sql_client import SqlClient
from metricflow.inference.context.base import InferenceContext, InferenceContextProvider

T = TypeVar("T", str, int, float, date, datetime)


@dataclass(frozen=True)
class ColumnStatistics(Generic[T]):
    """Holds statistical data about a column."""

    column: SqlColumn

    dtype: InitVar[Optional[str]]
    type: SqlColumnType = field(init=False)

    row_count: int
    distinct_row_count: int
    null_count: int
    min_value: Optional[T]
    max_value: Optional[T]

    def __post_init__(self, dtype: Optional[str]):  # noqa: D
        object.__setattr__(self, "type", SqlColumnType.from_pandas_dtype(dtype))

    @property
    def is_empty(self) -> bool:
        """Whether the column has any rows"""
        return self.row_count == 0

    @property
    def cardinality(self) -> float:
        """The proportion between unique values and the row count of the column."""
        if self.is_empty:
            raise InferenceError("Cannot determine the cardinality of empty column.")

        return self.distinct_row_count / self.row_count

    @property
    def is_nullable(self) -> bool:
        """Whether the column is nullable or not"""
        return self.null_count != 0


@dataclass(frozen=True)
class TableStatistics:
    """Holds statistical data about a table."""

    column_stats: InitVar[List[ColumnStatistics]]

    table: SqlTable
    columns: Dict[SqlColumn, ColumnStatistics] = field(default_factory=lambda: {}, init=False)

    def __post_init__(self, column_stats: List[ColumnStatistics]) -> None:  # noqa: D
        for col in column_stats:
            self.columns[col.column] = col


@dataclass(frozen=True)
class DataWarehouseInferenceContext(InferenceContext):
    """The inference context for a data warehouse. Holds statistics and metadata about each column."""

    table_stats: InitVar[List[TableStatistics]]

    tables: Dict[SqlTable, TableStatistics] = field(default_factory=lambda: {}, init=False)
    columns: Dict[SqlColumn, ColumnStatistics] = field(default_factory=lambda: {}, init=False)

    def __post_init__(self, table_stats: List[TableStatistics]) -> None:  # noqa: D
        for stats in table_stats:
            self.tables[stats.table] = stats
            for column in stats.columns.values():
                self.columns[column.column] = column


@dataclass(frozen=True)
class DataWarehouseInferenceContextProvider(InferenceContextProvider[DataWarehouseInferenceContext]):
    """Provides inference context from a data warehouse by querying data from its tables.

    client: the underlying SQL engine client that will be used for querying table data.
    tables: an exhaustive list of all tables that should be queried.
    max_sample_size: max number of rows to sample from each table
    """

    client: SqlClient
    tables: List[SqlTable]
    max_sample_size: int = 1000

    def _get_table_statistics(self, table: SqlTable) -> TableStatistics:
        """Fetch statistics about a single table by querying the warehouse."""

        query = f"SELECT * FROM {table.sql} LIMIT {self.max_sample_size}"
        df = self.client.query(query)
        column_stats = [
            ColumnStatistics(
                column=SqlColumn(table=table, name=col_name),
                dtype=str(series.dtype),
                row_count=len(series),
                distinct_row_count=series.nunique(dropna=False),
                null_count=series.isnull().sum(),
                min_value=series.min(),
                max_value=series.max(),
            )
            for col_name, series in df.iteritems()
        ]
        return TableStatistics(table=table, column_stats=column_stats)

    def get_context(self) -> DataWarehouseInferenceContext:
        """Query the data warehouse for statistics about all tables and populate a context with it."""
        table_stats = [self._get_table_statistics(table) for table in self.tables]
        return DataWarehouseInferenceContext(table_stats=table_stats)
