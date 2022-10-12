from __future__ import annotations

import logging
import time
from typing import Optional, ClassVar, Dict, Sequence

import pandas as pd
import sqlalchemy
from databricks import sql

from metricflow.dataflow.sql_table import SqlTable
from metricflow.protocols.sql_client import SqlEngineAttributes, SqlEngine
from metricflow.sql.render.sql_plan_renderer import DefaultSqlQueryPlanRenderer, SqlQueryPlanRenderer
from metricflow.sql.sql_bind_parameters import SqlBindParameters
from metricflow.sql_clients.base_sql_client_implementation import BaseSqlClientImplementation
from metricflow.sql_clients.common_client import SqlDialect

logger = logging.getLogger(__name__)

HTTP_PATH_KEY = "httppath="

# This is a non-exhaustive list of pandas dtypes, but in theory it will cover the ones we need to support
# for data frames generated and run through type inference.
PANDAS_TO_SQL_DTYPES = {
    "string": "string",
    "object": "string",
    "float64": "double",
    "bool": "boolean",
    "boolean": "boolean",
    "int64": "int",
    "datetime64[ns]": "timestamp",
}


class DatabricksEngineAttributes(SqlEngineAttributes):
    """SQL engine attributes for Databricks."""

    sql_engine_type: ClassVar[SqlEngine] = SqlEngine.DATABRICKS

    # SQL Engine capabilities
    date_trunc_supported: ClassVar[bool] = True
    full_outer_joins_supported: ClassVar[bool] = True
    indexes_supported: ClassVar[bool] = False
    multi_threading_supported: ClassVar[bool] = True
    timestamp_type_supported: ClassVar[bool] = True
    timestamp_to_string_comparison_supported: ClassVar[bool] = True
    # So far the only clear way to cancel a query is through the Databricks UI.
    cancel_submitted_queries_supported: ClassVar[bool] = False

    # SQL Dialect replacement strings
    double_data_type_name: ClassVar[str] = "DOUBLE"
    timestamp_type_name: ClassVar[Optional[str]] = "TIMESTAMP"

    # MetricFlow attributes
    sql_query_plan_renderer: ClassVar[SqlQueryPlanRenderer] = DefaultSqlQueryPlanRenderer()


class DatabricksSqlClient(BaseSqlClientImplementation):
    """Client used to connect to Databricks engine."""

    def __init__(self, host: str, http_path: str, access_token: str) -> None:  # noqa: D
        self.host = host
        self.http_path = http_path
        self.access_token = access_token

    @staticmethod
    def from_connection_details(url: str, password: Optional[str]) -> DatabricksSqlClient:  # noqa: D
        """Parse MF_SQL_ENGINE_URL & MF_SQL_ENGINE_PASSWORD into useful connection params.

        Using just these 2 env variables ensures uniformity across engines.
        """
        try:
            split_url = url.split(";")
            parsed_url = sqlalchemy.engine.url.make_url(split_url[0])
            http_path = ""
            for piece in split_url[1:]:
                if HTTP_PATH_KEY in piece.lower():
                    __, http_path = piece.split("=")
                    break
            dialect = SqlDialect.DATABRICKS.value
            if not http_path:
                raise ValueError("HTTP path not found in URL.")
            if parsed_url.drivername != dialect:
                raise ValueError(f"Unexpected dialect in URL: {parsed_url.drivername}. Expected: {dialect}")
            if not parsed_url.host:
                raise ValueError("Host not found in URL.")
        except ValueError as e:
            # If any errors in parsing URL, show user what expected URL looks like.
            raise ValueError(
                f"Unexpected format for MF_SQL_ENGINE_URL. Expected: `{dialect}://<HOST>:443;HttpPath=<HTTP PATH>`."
            ) from e

        if not password:
            raise ValueError(f"Password not supplied for {url}")

        return DatabricksSqlClient(host=parsed_url.host, http_path=http_path, access_token=password)

    def get_connection(self) -> sql.client.Connection:
        """Get connection to Databricks cluster/warehouse."""
        return sql.connect(server_hostname=self.host, http_path=self.http_path, access_token=self.access_token)

    @property
    def sql_engine_attributes(self) -> SqlEngineAttributes:
        """Databricks engine attributes."""
        return DatabricksEngineAttributes()

    @staticmethod
    def params_or_none(bind_params: SqlBindParameters) -> Optional[Dict[str, str]]:
        """If there are no parameters, use None to prevent collision with `%` wildcard."""
        return None if bind_params == SqlBindParameters() else bind_params.param_dict

    def _engine_specific_query_implementation(self, stmt: str, bind_params: SqlBindParameters) -> pd.DataFrame:
        with self.get_connection() as connection:
            with connection.cursor() as cursor:
                cursor.execute(operation=stmt, parameters=self.params_or_none(bind_params))
                logger.info("Fetching query results as PyArrow Table.")
                pyarrow_df = cursor.fetchall_arrow()

        logger.info("Beginning conversion of PyArrow Table to pandas DataFrame.")
        pandas_df = pyarrow_df.to_pandas()
        logger.info("Completed conversion of PyArrow Table to pandas DataFrame.")
        # Remove tz from any datetime cols. Databricks tables add UTC by default.
        for col_name in pandas_df:
            if pd.api.types.is_datetime64_any_dtype(pandas_df[col_name]):
                pandas_df[col_name] = pandas_df[col_name].dt.tz_localize(None)
        return pandas_df

    def _engine_specific_execute_implementation(self, stmt: str, bind_params: SqlBindParameters) -> None:
        """Execute statement, returning nothing."""
        with self.get_connection() as connection:
            with connection.cursor() as cursor:
                logger.info(f"Executing SQL statment: {stmt}")
                cursor.execute(operation=stmt, parameters=self.params_or_none(bind_params))

    def _engine_specific_dry_run_implementation(self, stmt: str, bind_params: SqlBindParameters) -> None:
        """Check that query will run successfully without actually running the query, error if not."""
        stmt = f"EXPLAIN {stmt}"

        with self.get_connection() as connection:
            with connection.cursor() as cursor:
                logger.info(f"Executing SQL statment: {stmt}")
                cursor.execute(operation=stmt, parameters=self.params_or_none(bind_params))

                # If the plan contains errors, they won't be raised. Parse plan string to find & raise errors.
                result = str(cursor.fetchall_arrow()["plan"][0])
                if "org.apache.spark.sql.AnalysisException" in result:
                    error = result.split("== Physical Plan ==")[1].split(";")[0]
                    raise sql.exc.ServerOperationError(error)

    def create_table_from_dataframe(  # noqa: D
        self, sql_table: SqlTable, df: pd.DataFrame, chunk_size: Optional[int] = None
    ) -> None:
        logger.info(f"Creating table '{sql_table.sql}' from a DataFrame with {df.shape[0]} row(s)")
        start_time = time.time()
        with self.get_connection() as connection:
            with connection.cursor() as cursor:
                # Create table
                # update dtypes to convert None to NA in boolean columns.
                # This mirrors the SQLAlchemy schema detection logic in pandas.io.sql
                df = df.convert_dtypes()
                columns = df.columns
                columns_to_insert = []
                for i in range(len(df.columns)):
                    # Format as "column_name column_type"
                    columns_to_insert.append(f"{columns[i]} {PANDAS_TO_SQL_DTYPES[str(df[columns[i]].dtype).lower()]}")
                cursor.execute(f"CREATE TABLE IF NOT EXISTS {sql_table.sql} ({', '.join(columns_to_insert)})")

                # Insert rows
                values = ""
                for row in df.itertuples(index=False, name=None):
                    cells = ""
                    for cell in row:
                        cells += ", " if cells else ""
                        if pd.isnull(cell):
                            # Databricks does not support None, NA, nan, or NaT
                            cells += "null"
                        elif type(cell) in [str, pd.Timestamp]:
                            # Wrap cell in quotes & escape existing single quotes
                            escaped_cell = str(cell).replace("'", "\\'")
                            cells += f"'{escaped_cell}'"
                        else:
                            cells += str(cell)

                    values += (",\n" if values else "") + f"({cells})"
                    if chunk_size and len(values) == chunk_size:
                        cursor.execute(f"INSERT INTO {sql_table.sql} VALUES {values}")
                        values = ""
                if values:
                    cursor.execute(f"INSERT INTO {sql_table.sql} VALUES {values}")

        logger.info(f"Created table '{sql_table.sql}' from a DataFrame in {time.time() - start_time:.2f}s")

    def list_tables(self, schema_name: str) -> Sequence[str]:  # noqa: D
        with self.get_connection() as connection:
            with connection.cursor() as cursor:
                cursor.tables(schema_name=schema_name)
                return [table.TABLE_NAME for table in cursor.fetchall()]

    def cancel_submitted_queries(self) -> None:  # noqa: D
        pass

    def render_execution_param_key(self, execution_param_key: str) -> str:
        """Wrap execution parameter key with syntax accepted by engine."""
        return f"%({execution_param_key})s"
