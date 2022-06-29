from unittest.mock import MagicMock, call

from metricflow.dataflow.sql_column import SqlColumn, SqlColumnType
from metricflow.dataflow.sql_table import SqlTable
from metricflow.inference.context.data_warehouse import (
    ColumnProperties,
    DataWarehouseInferenceContext,
    DataWarehouseInferenceContextProvider,
    TableProperties,
)


def test_column_statistics():  # noqa: D
    props = ColumnProperties(
        column=SqlColumn.from_string("db.schema.table.column"),
        type=SqlColumnType.INTEGER,
        row_count=10000,
        distinct_row_count=1000,
        is_nullable=True,
        null_count=1,
        min_value=0,
        max_value=9999,
    )
    assert props.type == SqlColumnType.INTEGER
    assert not props.is_empty

    empty_props = ColumnProperties(
        column=SqlColumn.from_string("db.schema.table.column"),
        type=SqlColumnType.UNKNOWN,
        row_count=0,
        distinct_row_count=0,
        is_nullable=False,
        null_count=0,
        min_value=None,
        max_value=None,
    )
    assert empty_props.type == SqlColumnType.UNKNOWN
    assert empty_props.is_empty


def test_table_statistics() -> None:  # noqa: D
    table = SqlTable.from_string("db.schema.table")
    col_props = [
        ColumnProperties(
            column=SqlColumn(table=table, column_name="column1"),
            type=SqlColumnType.INTEGER,
            row_count=1000,
            distinct_row_count=1000,
            is_nullable=False,
            null_count=0,
            min_value=0,
            max_value=999,
        ),
        ColumnProperties(
            column=SqlColumn(table=table, column_name="column2"),
            type=SqlColumnType.FLOAT,
            row_count=2000,
            distinct_row_count=1000,
            is_nullable=True,
            null_count=10,
            min_value=0,
            max_value=1000,
        ),
    ]

    table_props = TableProperties(table=table, column_props=col_props)

    assert table_props.columns == {
        col_props[0].column: col_props[0],
        col_props[1].column: col_props[1],
    }


def test_data_warehouse_inference_context() -> None:  # noqa: D
    t1 = SqlTable.from_string("db.schema1.table1")
    t2 = SqlTable.from_string("db.schema2.table1")

    t1_cols = [
        ColumnProperties(
            column=SqlColumn(table=t1, column_name="column1"),
            type=SqlColumnType.INTEGER,
            row_count=1000,
            distinct_row_count=1000,
            is_nullable=False,
            null_count=0,
            min_value=0,
            max_value=999,
        ),
        ColumnProperties(
            column=SqlColumn(table=t1, column_name="column2"),
            type=SqlColumnType.FLOAT,
            row_count=2000,
            distinct_row_count=1000,
            is_nullable=True,
            null_count=10,
            min_value=0,
            max_value=1000,
        ),
    ]
    t1_props = TableProperties(table=t1, column_props=t1_cols)

    t2_cols = [
        ColumnProperties(
            column=SqlColumn(table=t2, column_name="column_a"),
            type=SqlColumnType.FLOAT,
            row_count=1000,
            distinct_row_count=1000,
            is_nullable=False,
            null_count=0,
            min_value=0,
            max_value=999,
        ),
    ]
    t2_props = TableProperties(table=t2, column_props=t2_cols)

    ctx = DataWarehouseInferenceContext(table_props=[t1_props, t2_props])

    assert ctx.tables == {t1: t1_props, t2: t2_props}

    assert ctx.columns == {
        t1_cols[0].column: t1_cols[0],
        t1_cols[1].column: t1_cols[1],
        t2_cols[0].column: t2_cols[0],
    }


def test_context_provider() -> None:  # noqa: D
    tables = [
        SqlTable.from_string("db.schema.table1"),
        SqlTable.from_string("db.schema.table2"),
        SqlTable.from_string("db.schema.table3"),
    ]

    ctx_provider = DataWarehouseInferenceContextProvider(client=MagicMock(), tables=tables)

    object.__setattr__(ctx_provider, "_get_table_properties", MagicMock())
    table_props = [
        TableProperties(table=tables[0], column_props=[]),
        TableProperties(table=tables[1], column_props=[]),
        TableProperties(table=tables[2], column_props=[]),
    ]
    ctx_provider._get_table_properties.side_effect = table_props

    ctx = ctx_provider.get_context()

    ctx_provider._get_table_properties.assert_has_calls(
        [
            call(tables[0]),
            call(tables[1]),
            call(tables[2]),
        ]
    )

    assert ctx == DataWarehouseInferenceContext(table_props=table_props)
