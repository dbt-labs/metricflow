from __future__ import annotations

import json

from metricflow.data_table.mf_table import MetricFlowDataTable
from metricflow.inference.context.data_warehouse import (
    ColumnProperties,
    DataWarehouseInferenceContextProvider,
    InferenceColumnType,
    TableProperties,
)
from metricflow.sql.sql_column import SqlColumn
from metricflow.sql.sql_table import SqlTable


class SnowflakeInferenceContextProvider(DataWarehouseInferenceContextProvider):
    """The snowflake implementation for a DataWarehouseInferenceContextProvider."""

    COUNT_DISTINCT_SUFFIX = "countdistinct"
    COUNT_NULL_SUFFIX = "countnull"
    MIN_SUFFIX = "min"
    MAX_SUFFIX = "max"

    def _column_type_from_show_columns_data_type(self, type_str: str) -> InferenceColumnType:
        """Get the correspondent InferenceColumnType from Snowflake's returned type string.

        See for reference: https://docs.snowflake.com/en/sql-reference/sql/show-columns.html
        For string types: https://docs.snowflake.com/en/sql-reference/data-types-text.html#data-types-for-text-strings
        """
        type_str = type_str.upper()
        type_mapping = {
            "FIXED": InferenceColumnType.INTEGER,
            "REAL": InferenceColumnType.FLOAT,
            "BOOLEAN": InferenceColumnType.BOOLEAN,
            "DATE": InferenceColumnType.DATETIME,
            "TIMESTAMP_TZ": InferenceColumnType.DATETIME,
            "TIMESTAMP_LTZ": InferenceColumnType.DATETIME,
            "TIMESTAMP_NTZ": InferenceColumnType.DATETIME,
        }
        string_prefixes = {
            "VARCHAR",
            "CHAR",
            "CHARACTER",
            "NCHAR",
            "STRING",
            "TEXT",
            "NVARCHAR",
            "NVARCHAR2",
            "CHAR VARYING",
            "NCHAR VARYING",
        }

        if type_str in type_mapping:
            return type_mapping[type_str]

        # This might be a string type, which can either be something like "TEXT" or "VARCHAR(256)"
        for prefix in string_prefixes:
            if type_str.startswith(prefix):
                return InferenceColumnType.STRING

        return InferenceColumnType.UNKNOWN

    def _get_select_list_for_column_name(self, name: str, count_nulls: bool) -> str:
        statements = [
            f"COUNT(DISTINCT '{name}') AS {name}_{SnowflakeInferenceContextProvider.COUNT_DISTINCT_SUFFIX}",
            f"MIN('{name}') AS {name}_{SnowflakeInferenceContextProvider.MIN_SUFFIX}",
            f"MAX('{name}') AS {name}_{SnowflakeInferenceContextProvider.MAX_SUFFIX}",
            (
                f"SUM(CASE WHEN '{name}' IS NULL THEN 1 ELSE 0 END) AS {name}_{SnowflakeInferenceContextProvider.COUNT_NULL_SUFFIX}"
                if count_nulls
                else f"0 AS {name}_{SnowflakeInferenceContextProvider.COUNT_NULL_SUFFIX}"
            ),
        ]

        return ", ".join(statements)

    def _get_one_int(self, data_table: MetricFlowDataTable, column_name: str) -> int:
        if len(data_table.rows) == 0:
            raise ValueError("No rows in the data table")

        return_value = data_table.get_cell_value(0, data_table.column_name_index(column_name))

        if isinstance(return_value, int):
            return int(return_value)
        raise RuntimeError(f"Unhandled case {return_value=}")

    def _get_one_str(self, data_table: MetricFlowDataTable, column_name: str) -> str:
        if len(data_table.rows) == 0:
            raise ValueError("No rows in the data table")

        return str(data_table.get_cell_value(0, data_table.column_name_index(column_name)))

    def _get_table_properties(self, table: SqlTable) -> TableProperties:
        all_columns_query = f"SHOW COLUMNS IN TABLE {table.sql}"
        all_columns = self._client.query(all_columns_query)

        sql_column_list = []
        col_types = {}
        col_nullable = {}
        select_lists = []

        for row in all_columns.rows:
            column = SqlColumn.from_names(
                db_name=str(row[all_columns.column_name_index("database_name")]).lower(),
                schema_name=str(row[all_columns.column_name_index("schema_name")]).lower(),
                table_name=str(row[all_columns.column_name_index("table_name")]).lower(),
                column_name=str(row[all_columns.column_name_index("column_name")]).lower(),
            )
            sql_column_list.append(column)

            type_dict = json.loads(str(row[all_columns.column_name_index("data_type")]))
            col_types[column] = self._column_type_from_show_columns_data_type(type_dict["type"])
            col_nullable[column] = type_dict["nullable"]
            select_lists.append(
                self._get_select_list_for_column_name(
                    name=column.column_name,
                    count_nulls=col_nullable[column],
                )
            )

        select_lists.append("COUNT(*) AS rowcount")
        select_list = ", ".join(select_lists)
        statistics_query = f"SELECT {select_list} FROM {table.sql} SAMPLE ({self.max_sample_size} ROWS)"
        statistics_data_table = self._client.query(statistics_query)

        column_props = [
            ColumnProperties(
                column=column,
                type=col_types[column],
                is_nullable=col_nullable[column],
                null_count=self._get_one_int(
                    statistics_data_table,
                    f"{column.column_name}_{SnowflakeInferenceContextProvider.COUNT_NULL_SUFFIX}",
                ),
                row_count=self._get_one_int(statistics_data_table, "rowcount"),
                distinct_row_count=self._get_one_int(
                    statistics_data_table,
                    f"{column.column_name}_{SnowflakeInferenceContextProvider.COUNT_DISTINCT_SUFFIX}",
                ),
                min_value=self._get_one_str(
                    statistics_data_table,
                    f"{column.column_name}_{SnowflakeInferenceContextProvider.MIN_SUFFIX}",
                ),
                max_value=self._get_one_str(
                    statistics_data_table,
                    f"{column.column_name}_{SnowflakeInferenceContextProvider.MAX_SUFFIX}",
                ),
            )
            for column in sql_column_list
        ]

        return TableProperties(table=table, column_props=column_props)
