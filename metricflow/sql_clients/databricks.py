from typing import Optional, List, ClassVar
import pandas as pd
import logging
import time
from databricks import sql, Connection
from metricflow.sql_clients.base_sql_client_implementation import BaseSqlClientImplementation
from metricflow.protocols.sql_client import SqlEngineAttributes, SupportedSqlEngine
from metricflow.sql.sql_bind_parameters import SqlBindParameters
from metricflow.dataflow.sql_table import SqlTable
from metricflow.sql.render.sql_plan_renderer import SqlQueryPlanRenderer
from metricflow.sql.render.databricks import DatabricksSqlQueryPlanRenderer

logger = logging.getLogger(__name__)


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
class Databricks(BaseSqlClientImplementation):
    """Client used to connect to Databricks engine."""

    @property
    def get_connection(self) -> Connection:
        """Get connection to Databricks cluster/warehouse."""
        # TODO
        return sql.connect(server_hostname="", http_path="", access_token="")

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
        pandas_df = pyarrow_df.toPandas()
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

    def create_table_from_dataframe(  # noqa: D
        self, sql_table: SqlTable, df: pd.DataFrame, chunk_size: Optional[int] = None
    ) -> None:
        logger.info(f"Creating table '{sql_table.sql}' from a DataFrame with {df.shape[0]} row(s)")
        start_time = time.time()
        with self.get_connection() as connection:
            pd.io.sql.to_sql(
                frame=df,
                name=sql_table.table_name,
                con=connection,
                schema=sql_table.schema_name,
                index=False,
                if_exists="fail",
                method="multi",
                chunksize=chunk_size,
            )
        logger.info(f"Created table '{sql_table.sql}' from a DataFrame in {time.time() - start_time:.2f}s")

    def list_tables(self, schema_name: str) -> List[str]:  # noqa: D
        with self.get_connection() as connection:
            with connection.cursor() as cursor:
                cursor.tables(schema_name=schema_name)
                return [
                    table.TABLE_NAME for table in cursor.fetchall()
                ]  # will this close itself if I return before close?
