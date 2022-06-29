import json
from unittest.mock import MagicMock, call

import pandas as pd

from metricflow.dataflow.sql_table import SqlTable
from metricflow.dataflow.sql_column import SqlColumn, SqlColumnType
from metricflow.inference.context.data_warehouse import TableProperties, ColumnProperties, DataWarehouseInferenceContext
from metricflow.inference.context.snowflake import SnowflakeInferenceContextProvider


def test_column_type_conversion() -> None:  # noqa: D
    ctx_provider = SnowflakeInferenceContextProvider(client=MagicMock(), tables=[])

    # known snowflake types
    assert ctx_provider._column_type_from_show_columns_data_type("FIXED") == SqlColumnType.INTEGER
    assert ctx_provider._column_type_from_show_columns_data_type("TEXT") == SqlColumnType.STRING
    assert ctx_provider._column_type_from_show_columns_data_type("REAL") == SqlColumnType.FLOAT
    assert ctx_provider._column_type_from_show_columns_data_type("BOOLEAN") == SqlColumnType.BOOLEAN
    assert ctx_provider._column_type_from_show_columns_data_type("DATE") == SqlColumnType.DATETIME
    assert ctx_provider._column_type_from_show_columns_data_type("TIMESTAMP_TZ") == SqlColumnType.DATETIME
    assert ctx_provider._column_type_from_show_columns_data_type("TIMESTAMP_LTZ") == SqlColumnType.DATETIME
    assert ctx_provider._column_type_from_show_columns_data_type("TIMESTAMP_NTZ") == SqlColumnType.DATETIME

    # unknowns
    assert ctx_provider._column_type_from_show_columns_data_type("BINARY") == SqlColumnType.UNKNOWN
    assert ctx_provider._column_type_from_show_columns_data_type("TIME") == SqlColumnType.UNKNOWN


def test_context_provider() -> None:  # noqa: D
    # See for SHOW COLUMNS result dataframe spec:
    # https://docs.snowflake.com/en/sql-reference/sql/show-columns.html
    show_columns_result = pd.DataFrame(
        {
            "column_name": ["INTCOL", "STRCOL"],
            "schema_name": ["SCHEMA", "SCHEMA"],
            "table_name": ["TABLE", "TABLE"],
            "database_name": ["DB", "DB"],
            "data_type": [
                json.dumps({"type": "FIXED", "nullable": False}),
                json.dumps({"type": "TEXT", "nullable": True}),
            ],
        }
    )

    stats_result = pd.DataFrame(
        {
            "intcol_countdistinct": [10],
            "intcol_min": [0],
            "intcol_max": [10],
            "intcol_countnull": [0],
            "strcol_countdistinct": [40],
            "strcol_min": ["aaaa"],
            "strcol_max": ["zzzz"],
            "strcol_countnull": [10],
            "rowcount": [50],
        }
    )

    client = MagicMock()
    client.query = MagicMock()
    client.query.side_effect = [show_columns_result, stats_result]

    ctx_provider = SnowflakeInferenceContextProvider(
        client=client,
        tables=[SqlTable.from_string("db.schema.table")],
        max_sample_size=50,
    )

    ctx = ctx_provider.get_context()

    client.query.assert_has_calls(
        [
            call("SHOW COLUMNS IN TABLE db.schema.table"),
            # make sure it produces correct SQL
            call(
                "SELECT "
                "COUNT(DISTINCT intcol) AS intcol_countdistinct, "
                "MIN(intcol) AS intcol_min, "
                "MAX(intcol) AS intcol_max, "
                # no need to query for count null if schema says it is not nullable
                "0 AS intcol_countnull, "
                "COUNT(DISTINCT strcol) AS strcol_countdistinct, "
                "MIN(strcol) AS strcol_min, "
                "MAX(strcol) AS strcol_max, "
                "SUM(CASE WHEN strcol IS NULL THEN 1 ELSE 0 END) AS strcol_countnull, "
                "COUNT(*) AS rowcount "
                "FROM db.schema.table SAMPLE (50 ROWS)"
            ),
        ],
        any_order=False,
    )

    assert ctx == DataWarehouseInferenceContext(
        [
            TableProperties(
                table=SqlTable.from_string("db.schema.table"),
                column_props=[
                    ColumnProperties(
                        column=SqlColumn.from_string("db.schema.table.intcol"),
                        type=SqlColumnType.INTEGER,
                        row_count=50,
                        distinct_row_count=10,
                        is_nullable=False,
                        null_count=0,
                        min_value=0,
                        max_value=10,
                    ),
                    ColumnProperties(
                        column=SqlColumn.from_string("db.schema.table.strcol"),
                        type=SqlColumnType.STRING,
                        row_count=50,
                        distinct_row_count=40,
                        is_nullable=True,
                        null_count=10,
                        min_value="aaaa",
                        max_value="zzzz",
                    ),
                ],
            ),
        ]
    )
