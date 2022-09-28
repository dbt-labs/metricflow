from __future__ import annotations
from typing import Optional, List, ClassVar
import pandas as pd
import logging
import time
import pyarrow
import sqlalchemy
from databricks import sql
from metricflow.sql_clients.common_client import SqlDialect
from metricflow.sql_clients.base_sql_client_implementation import BaseSqlClientImplementation
from metricflow.protocols.sql_client import SqlEngineAttributes, SupportedSqlEngine
from metricflow.sql.sql_bind_parameters import SqlBindParameters
from metricflow.dataflow.sql_table import SqlTable
from metricflow.sql.render.sql_plan_renderer import SqlQueryPlanRenderer
from metricflow.sql.render.databricks import DatabricksSqlQueryPlanRenderer

logger = logging.getLogger(__name__)

HTTP_PATH_KEY = "httppath="
PYARROW_TO_SQL_DTYPES = {
    "string": "string",
    "double": "double",
    "bool": "boolean",
    "int64": "int",
    "timestamp[ns]": "timestamp",
}


class DatabricksEngineAttributes(SqlEngineAttributes):
    """SQL engine attributes for Databricks."""

    sql_engine_type: ClassVar[SupportedSqlEngine] = SupportedSqlEngine.DATABRICKS

    # SQL Engine capabilities
    date_trunc_supported: ClassVar[bool] = True
    full_outer_joins_supported: ClassVar[bool] = True
    indexes_supported: ClassVar[bool] = True
    multi_threading_supported: ClassVar[bool] = True
    timestamp_type_supported: ClassVar[bool] = True
    timestamp_to_string_comparison_supported: ClassVar[bool] = True
    cancel_submitted_queries_supported: ClassVar[bool] = True

    # SQL Dialect replacement strings
    double_data_type_name: ClassVar[str] = "DOUBLE"
    timestamp_type_name: ClassVar[Optional[str]] = "TIMESTAMP"

    # MetricFlow attributes
    sql_query_plan_renderer: ClassVar[SqlQueryPlanRenderer] = DatabricksSqlQueryPlanRenderer()


# TODO: support both cluster and SQL warehouse connection. SQL warehouse is prob what we have now? pending Jordan
class DatabricksSqlClient(BaseSqlClientImplementation):
    """Client used to connect to Databricks engine."""

    def __init__(self, host: str, http_path: str, access_token: str) -> None:  # noqa: D
        self.host = host
        self.http_path = http_path
        self.access_token = access_token

    # TODO: why do we even need this method? just for tests? Shouldn't we use the same configs in tests as prod?
    @staticmethod
    def from_connection_details(url: str, password: Optional[str]) -> DatabricksSqlClient:  # noqa: D
        # Is the input format right for this? What's normal for databricks users?
        try:
            split_url = url.split(";")  # TODO: there might not only be http path in there
            parsed_url = sqlalchemy.engine.url.make_url(split_url[0])
            for piece in split_url:
                if HTTP_PATH_KEY in piece.lower():
                    __, http_path = piece.split("=")
            dialect = SqlDialect.DATABRICKS.value
            if not http_path or parsed_url.drivername != dialect or not parsed_url.host:
                raise ValueError
        except:  # noqa: E722
            # If any errors in parsing URL, show user what expected URL looks like.
            raise ValueError(
                "Unexpected format for MF_SQL_ENGINE_URL. Expected: `databricks://<HOST>:443;HttpPath=<HTTP PATH>"
            )

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

    def _engine_specific_query_implementation(self, stmt: str, bind_params: SqlBindParameters) -> pd.DataFrame:
        with self.get_connection() as connection:  # this syntax might not close itself automatically
            with connection.cursor() as cursor:
                cursor.execute(operation=stmt, parameters=bind_params.param_dict)
                logger.info("Fetching query results as PyArrow Table.")
                pyarrow_df = cursor.fetchall_arrow()

        logger.info("Beginning conversion of PyArrow Table to pandas DataFrame.")
        pandas_df = pyarrow_df.to_pandas()
        logger.info("Completed conversion of PyArrow Table to pandas DataFrame.")
        return pandas_df

    def _engine_specific_execute_implementation(self, stmt: str, bind_params: SqlBindParameters) -> None:
        """Execute statement, returning nothing."""
        with self.get_connection() as connection:
            with connection.cursor() as cursor:
                logger.info(f"Executing SQL statment: {stmt}")
                cursor.execute(operation=stmt, parameters=bind_params.param_dict)

    def _engine_specific_dry_run_implementation(self, stmt: str, bind_params: SqlBindParameters) -> None:
        """Check that query will run successfully without actually running the query, error if not."""
        self._engine_specific_execute_implementation(stmt=f"EXPLAIN {stmt}", bind_params=bind_params)

    # TODO: this is VERYYY SLOWWWWW.
    def create_table_from_dataframe(  # noqa: D
        self, sql_table: SqlTable, df: pd.DataFrame, chunk_size: Optional[int] = None
    ) -> None:
        logger.info(f"Creating table '{sql_table.sql}' from a DataFrame with {df.shape[0]} row(s)")
        start_time = time.time()
        with self.get_connection() as connection:
            with connection.cursor() as cursor:
                pyarrow_df = pyarrow.Table.from_pandas(
                    df
                )  # TODO: if this piece is slow, trash it and use pandas dtypes

                # Create table
                columns = [col for col in pyarrow_df.itercolumns()]
                column_names = pyarrow_df.column_names
                columns_to_insert = []
                for i in range(pyarrow_df.num_columns):
                    # Format as "column_name column_type"
                    columns_to_insert.append(f"{column_names[i]} {PYARROW_TO_SQL_DTYPES[str(columns[i].type)]}")
                cursor.execute(f"CREATE TABLE IF NOT EXISTS {sql_table.sql} ({', '.join(columns_to_insert)})")

                # Insert rows
                values = []
                for row in df.itertuples(index=False, name=None):
                    cells = []
                    for cell in row:
                        if type(cell) in [str, pd.Timestamp]:
                            # Wrap cell in quotes & escape existing single quotes
                            cells.append(f"""'{str(cell).replace("'", '"')}'""")
                        else:
                            cells.append(str(cell))
                    values.append(f"({', '.join(cells)})")
                cursor.execute(f"INSERT INTO {sql_table.sql} VALUES {', '.join(values)}")

        logger.info(f"Created table '{sql_table.sql}' from a DataFrame in {time.time() - start_time:.2f}s")

    def list_tables(self, schema_name: str) -> List[str]:  # noqa: D
        with self.get_connection() as connection:
            with connection.cursor() as cursor:
                cursor.tables(schema_name=schema_name)
                return [table.TABLE_NAME for table in cursor.fetchall()]

    def cancel_submitted_queries(self) -> None:  # noqa: D
        pass
