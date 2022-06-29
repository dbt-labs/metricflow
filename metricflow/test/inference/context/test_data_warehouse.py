from unittest.mock import MagicMock, call

import pandas as pd

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
        null_count=1,
        min_value=0,
        max_value=9999,
    )
    assert props.type == SqlColumnType.INTEGER
    assert not props.is_empty
    assert props.is_nullable

    empty_props = ColumnProperties(
        column=SqlColumn.from_string("db.schema.table.column"),
        type=SqlColumnType.UNKNOWN,
        row_count=0,
        distinct_row_count=0,
        null_count=0,
        min_value=None,
        max_value=None,
    )
    assert empty_props.type == SqlColumnType.UNKNOWN
    assert empty_props.is_empty
    assert not empty_props.is_nullable


def test_table_statistics() -> None:  # noqa: D
    table = SqlTable.from_string("db.schema.table")
    col_props = [
        ColumnProperties(
            column=SqlColumn(table=table, column_name="column1"),
            type=SqlColumnType.INTEGER,
            row_count=1000,
            distinct_row_count=1000,
            null_count=0,
            min_value=0,
            max_value=999,
        ),
        ColumnProperties(
            column=SqlColumn(table=table, column_name="column2"),
            type=SqlColumnType.FLOAT,
            row_count=2000,
            distinct_row_count=1000,
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
            null_count=0,
            min_value=0,
            max_value=999,
        ),
        ColumnProperties(
            column=SqlColumn(table=t1, column_name="column2"),
            type=SqlColumnType.FLOAT,
            row_count=2000,
            distinct_row_count=1000,
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
    t1_df = pd.DataFrame(
        [[0, "lucas", 10], [1, "paul", 20], [2, "tom", 30], [3, "thomas", 30]], columns=["user_id", "name", "points"]
    )

    t2_df = pd.DataFrame(
        [[7, "tinky winky", 4.1], [None, "gipsy", 4.2], [None, "lala", 4.1], [10, "po", 4]],
        columns=["user_id", "name", "height"],
    )
    # nullable integer columns are converted by pandas to float,
    # so we need to cast it back to nullable int
    t2_df["user_id"] = t2_df["user_id"].astype("Int64")

    client = MagicMock()
    client.query = MagicMock()
    client.query.side_effect = [t1_df, t2_df]

    ctx_provider = DataWarehouseInferenceContextProvider(
        client=client,
        tables=[SqlTable.from_string("db.schema.table1"), SqlTable.from_string("db.schema.table2")],
        max_sample_size=5,
    )

    ctx = ctx_provider.get_context()

    client.query.assert_has_calls(
        [
            call("SELECT * FROM db.schema.table1 LIMIT 5"),
            call("SELECT * FROM db.schema.table2 LIMIT 5"),
        ],
        any_order=True,
    )

    assert ctx == DataWarehouseInferenceContext(
        [
            TableProperties(
                table=SqlTable.from_string("db.schema.table1"),
                column_props=[
                    ColumnProperties(
                        column=SqlColumn.from_string("db.schema.table1.user_id"),
                        type=SqlColumnType.INTEGER,
                        row_count=4,
                        distinct_row_count=4,
                        null_count=0,
                        min_value=0,
                        max_value=3,
                    ),
                    ColumnProperties(
                        column=SqlColumn.from_string("db.schema.table1.name"),
                        type=SqlColumnType.STRING,
                        row_count=4,
                        distinct_row_count=4,
                        null_count=0,
                        min_value="lucas",
                        max_value="tom",
                    ),
                    ColumnProperties(
                        column=SqlColumn.from_string("db.schema.table1.points"),
                        type=SqlColumnType.INTEGER,
                        row_count=4,
                        distinct_row_count=3,
                        null_count=0,
                        min_value=10,
                        max_value=30,
                    ),
                ],
            ),
            TableProperties(
                table=SqlTable.from_string("db.schema.table2"),
                column_props=[
                    ColumnProperties(
                        column=SqlColumn.from_string("db.schema.table2.user_id"),
                        type=SqlColumnType.INTEGER,
                        row_count=4,
                        distinct_row_count=3,
                        null_count=2,
                        min_value=7,
                        max_value=10,
                    ),
                    ColumnProperties(
                        column=SqlColumn.from_string("db.schema.table2.name"),
                        type=SqlColumnType.STRING,
                        row_count=4,
                        distinct_row_count=4,
                        null_count=0,
                        min_value="gipsy",
                        max_value="tinky winky",
                    ),
                    ColumnProperties(
                        column=SqlColumn.from_string("db.schema.table2.height"),
                        type=SqlColumnType.FLOAT,
                        row_count=4,
                        distinct_row_count=3,
                        null_count=0,
                        min_value=4,
                        max_value=4.2,
                    ),
                ],
            ),
        ]
    )
