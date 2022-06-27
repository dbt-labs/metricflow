from metricflow.dataflow.sql_table import SqlTable
from metricflow.dataflow.sql_column import SqlColumn, SqlColumnType


def test_sql_column() -> None:  # noqa: D
    sql_column = SqlColumn(
        table=SqlTable(db_name="test_db", schema_name="test_schema", table_name="test_table"), name="test_column"
    )
    column_str = "test_db.test_schema.test_table.test_column"

    assert sql_column.sql == column_str
    assert SqlColumn.from_string(column_str) == sql_column

    json_serialized_column = sql_column.json()
    deserialized_column = SqlColumn.parse_raw(json_serialized_column)

    assert sql_column == deserialized_column


def test_sql_column_type_from_pandas_dtype() -> None:  # noqa: D
    # int
    assert SqlColumnType.from_pandas_dtype("int") == SqlColumnType.INTEGER
    assert SqlColumnType.from_pandas_dtype("int8") == SqlColumnType.INTEGER
    assert SqlColumnType.from_pandas_dtype("int16") == SqlColumnType.INTEGER
    assert SqlColumnType.from_pandas_dtype("int32") == SqlColumnType.INTEGER
    assert SqlColumnType.from_pandas_dtype("int64") == SqlColumnType.INTEGER
    assert SqlColumnType.from_pandas_dtype("uint") == SqlColumnType.INTEGER
    assert SqlColumnType.from_pandas_dtype("uint8") == SqlColumnType.INTEGER
    assert SqlColumnType.from_pandas_dtype("uint16") == SqlColumnType.INTEGER
    assert SqlColumnType.from_pandas_dtype("uint32") == SqlColumnType.INTEGER
    assert SqlColumnType.from_pandas_dtype("uint64") == SqlColumnType.INTEGER

    # float
    assert SqlColumnType.from_pandas_dtype("float") == SqlColumnType.FLOAT
    assert SqlColumnType.from_pandas_dtype("float16") == SqlColumnType.FLOAT
    assert SqlColumnType.from_pandas_dtype("float32") == SqlColumnType.FLOAT
    assert SqlColumnType.from_pandas_dtype("float64") == SqlColumnType.FLOAT

    # bool
    assert SqlColumnType.from_pandas_dtype("bool") == SqlColumnType.BOOLEAN

    # string
    assert SqlColumnType.from_pandas_dtype("str") == SqlColumnType.STRING

    # datetime
    assert SqlColumnType.from_pandas_dtype("datetime64") == SqlColumnType.DATETIME

    # unknowns
    assert SqlColumnType.from_pandas_dtype(None) == SqlColumnType.UNKNOWN
    assert SqlColumnType.from_pandas_dtype("complex") == SqlColumnType.UNKNOWN
    assert SqlColumnType.from_pandas_dtype("complex64") == SqlColumnType.UNKNOWN
    assert SqlColumnType.from_pandas_dtype("complex128") == SqlColumnType.UNKNOWN
    assert SqlColumnType.from_pandas_dtype("asdf16") == SqlColumnType.UNKNOWN
    assert SqlColumnType.from_pandas_dtype("test32") == SqlColumnType.UNKNOWN
    assert SqlColumnType.from_pandas_dtype("shouldnotwork64") == SqlColumnType.UNKNOWN
