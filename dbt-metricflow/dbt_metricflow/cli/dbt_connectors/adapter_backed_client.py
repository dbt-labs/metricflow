from __future__ import annotations

import enum
import logging
import time

from dbt.adapters.base import BaseAdapter
from dbt_common.exceptions.base import DbtDatabaseError
from dbt_semantic_interfaces.enum_extension import assert_values_exhausted
from metricflow_semantics.errors.error_classes import SqlBindParametersNotSupportedError
from metricflow_semantics.sql.sql_bind_parameters import SqlBindParameterSet
from metricflow_semantics.toolkit.id_helpers import mf_random_id
from metricflow_semantics.toolkit.mf_logging.lazy_formattable import LazyFormat

from metricflow.data_table.mf_table import MetricFlowDataTable
from metricflow.protocols.sql_client import SqlEngine
from metricflow.sql.render.big_query import BigQuerySqlPlanRenderer
from metricflow.sql.render.databricks import DatabricksSqlPlanRenderer
from metricflow.sql.render.duckdb_renderer import DuckDbSqlPlanRenderer
from metricflow.sql.render.postgres import PostgresSQLSqlPlanRenderer
from metricflow.sql.render.redshift import RedshiftSqlPlanRenderer
from metricflow.sql.render.snowflake import SnowflakeSqlPlanRenderer
from metricflow.sql.render.sql_plan_renderer import SqlPlanRenderer
from metricflow.sql.render.trino import TrinoSqlPlanRenderer
from metricflow.sql_request.sql_request_attributes import SqlRequestId

logger = logging.getLogger(__name__)


# Discovered via trial and error from the original, and now defunct DatabricksSqlClient implementation
DATABRICKS_SQL_WAREHOUSE_EXPLAIN_PLAN_ERROR_KEY = "Error occurred during query planning"
DATABRICKS_CLUSTER_EXPLAIN_PLAN_ERROR_KEY = "org.apache.spark.sql.AnalysisException"


class SupportedAdapterTypes(enum.Enum):
    """Enumeration of supported dbt adapter types."""

    DATABRICKS = "databricks"
    POSTGRES = "postgres"
    SNOWFLAKE = "snowflake"
    REDSHIFT = "redshift"
    BIGQUERY = "bigquery"
    DUCKDB = "duckdb"
    TRINO = "trino"

    @property
    def sql_engine_type(self) -> SqlEngine:
        """Return the SqlEngine corresponding to the supported adapter type."""
        if self is SupportedAdapterTypes.BIGQUERY:
            return SqlEngine.BIGQUERY
        elif self is SupportedAdapterTypes.DATABRICKS:
            return SqlEngine.DATABRICKS
        elif self is SupportedAdapterTypes.POSTGRES:
            return SqlEngine.POSTGRES
        elif self is SupportedAdapterTypes.REDSHIFT:
            return SqlEngine.REDSHIFT
        elif self is SupportedAdapterTypes.SNOWFLAKE:
            return SqlEngine.SNOWFLAKE
        elif self is SupportedAdapterTypes.DUCKDB:
            return SqlEngine.DUCKDB
        elif self is SupportedAdapterTypes.TRINO:
            return SqlEngine.TRINO
        else:
            assert_values_exhausted(self)

    @property
    def sql_plan_renderer(self) -> SqlPlanRenderer:
        """Return the SqlPlanRenderer corresponding to the supported adapter type."""
        if self is SupportedAdapterTypes.BIGQUERY:
            return BigQuerySqlPlanRenderer()
        elif self is SupportedAdapterTypes.DATABRICKS:
            return DatabricksSqlPlanRenderer()
        elif self is SupportedAdapterTypes.POSTGRES:
            return PostgresSQLSqlPlanRenderer()
        elif self is SupportedAdapterTypes.REDSHIFT:
            return RedshiftSqlPlanRenderer()
        elif self is SupportedAdapterTypes.SNOWFLAKE:
            return SnowflakeSqlPlanRenderer()
        elif self is SupportedAdapterTypes.DUCKDB:
            return DuckDbSqlPlanRenderer()
        elif self is SupportedAdapterTypes.TRINO:
            return TrinoSqlPlanRenderer()
        else:
            assert_values_exhausted(self)


class AdapterBackedSqlClient:
    """SqlClient implementation which delegates database operations to a dbt BaseAdapter instance.

    This is a generic wrapper class meant to cover all of our logging, querying, and internal configuration
    needs while delegating all connection state management and warehouse communication work to an underlying
    dbt adapter instance. This relies on BaseAdapter, rather than SQLAdapter, because BigQuery is an instance
    of the more generic BaseAdapter class.
    """

    def __init__(self, adapter: BaseAdapter):
        """Initializer sourced from a BaseAdapter instance.

        The dbt BaseAdapter should already be fully initialized, including all credential verification, and
        ready for use for establishing connections and issuing queries.
        """
        self._adapter = adapter
        try:
            adapter_type = SupportedAdapterTypes(self._adapter.type())
        except ValueError as e:
            raise ValueError(
                f"Adapter type {self._adapter.type()} is not supported. Must be one "
                f"of {[item.value for item in SupportedAdapterTypes]}."
            ) from e

        self._sql_engine_type = adapter_type.sql_engine_type
        self._sql_plan_renderer = adapter_type.sql_plan_renderer
        logger.debug(
            LazyFormat(lambda: f"Initialized AdapterBackedSqlClient with dbt adapter type `{adapter_type.value}`")
        )

    @property
    def sql_engine_type(self) -> SqlEngine:
        """An enumerated value representing the underlying SqlEngine supported by the dbt adapter for this instance."""
        return self._sql_engine_type

    @property
    def sql_plan_renderer(self) -> SqlPlanRenderer:
        """Dialect-specific SQL query plan renderer used for converting MetricFlow's query plan to executable SQL."""
        return self._sql_plan_renderer

    def query(
        self,
        stmt: str,
        sql_bind_parameter_set: SqlBindParameterSet = SqlBindParameterSet(),
    ) -> MetricFlowDataTable:
        """Query statement; result expected to be data which will be returned as a DataTable.

        Args:
            stmt: The SQL query statement to run. This should produce output via a SELECT
            sql_bind_parameter_set: The parameter replacement mapping for filling in concrete values for SQL query
            parameters.
        """
        start = time.perf_counter()
        request_id = SqlRequestId(f"mf_rid__{mf_random_id()}")
        if sql_bind_parameter_set.param_dict:
            raise SqlBindParametersNotSupportedError(
                f"Invalid query statement - we do not support queries with bind parameters through dbt adapters! "
                f"Bind params: {sql_bind_parameter_set.param_dict}"
            )
        logger.info(
            LazyFormat("Running query() statement", statement=stmt, param_dict=sql_bind_parameter_set.param_dict)
        )
        with self._adapter.connection_named(f"MetricFlow_request_{request_id}"):
            # returns a Tuple[AdapterResponse, agate.Table] but the decorator converts it to Any
            result = self._adapter.execute(sql=stmt, auto_begin=True, fetch=True)
            logger.info(LazyFormat(lambda: f"query() returned from dbt Adapter with response {result[0]}"))

        agate_data = result[1]
        rows = [row.values() for row in agate_data.rows]
        data_table = MetricFlowDataTable.create_from_rows(
            column_names=agate_data.column_names,
            rows=rows,
        )
        stop = time.perf_counter()

        logger.info(
            LazyFormat(
                "Finished running query()", runtime=f"{stop - start:.2f}s", returned_row_count=data_table.row_count
            )
        )
        return data_table

    def execute(
        self,
        stmt: str,
        sql_bind_parameter_set: SqlBindParameterSet = SqlBindParameterSet(),
    ) -> None:
        """Execute a SQL statement. No result will be returned.

        Args:
            stmt: The SQL query statement to run. This should not produce output.
            sql_bind_parameter_set: The parameter replacement mapping for filling in
                concrete values for SQL query parameters.
        """
        if sql_bind_parameter_set.param_dict:
            raise SqlBindParametersNotSupportedError(
                f"Invalid execute statement - we do not support execute commands with bind parameters through dbt "
                f"adapters! Bind params: {SqlBindParameterSet.param_dict}"
            )
        start = time.perf_counter()
        request_id = SqlRequestId(f"mf_rid__{mf_random_id()}")
        logger.info(
            LazyFormat("Running execute() statement", statement=stmt, param_dict=sql_bind_parameter_set.param_dict)
        )
        with self._adapter.connection_named(f"MetricFlow_request_{request_id}"):
            result = self._adapter.execute(stmt, auto_begin=True, fetch=False)
            # Calls to execute often involve some amount of DDL so we commit here
            self._adapter.commit_if_has_connection()
            logger.debug(LazyFormat(lambda: f"execute() returned from dbt Adapter with response  {result[0]}"))
        stop = time.perf_counter()
        logger.info(LazyFormat("Finished execute()", runtime=f"{stop - start:.2f}s"))

        return None

    def dry_run(
        self,
        stmt: str,
        sql_bind_parameter_set: SqlBindParameterSet = SqlBindParameterSet(),
    ) -> None:
        """Dry run statement; checks that the 'stmt' is queryable. Returns None on success.

        Raises an exception if the 'stmt' isn't queryable.

        Args:
            stmt: The SQL query statement to dry run.
            sql_bind_parameter_set: The parameter replacement mapping for filling in
                concrete values for SQL query parameters.
        """
        start = time.perf_counter()
        logger.info(LazyFormat("Running dry run", statement=stmt, param_dict=sql_bind_parameter_set.param_dict))
        request_id = SqlRequestId(f"mf_rid__{mf_random_id()}")
        connection_name = f"MetricFlow_dry_run_request_{request_id}"
        # TODO - consolidate to self._adapter.validate_sql() when all implementations will work from within MetricFlow

        # Trino has a bug where explain command actually creates table. Wrapping with validate to avoid this.
        # See https://github.com/trinodb/trino/issues/130
        if self.sql_engine_type is SqlEngine.TRINO:
            with self._adapter.connection_named(connection_name):
                # Either the response will be bool value or a string with error message from Trino.
                result = self._adapter.execute(f"EXPLAIN (type validate) {stmt}", auto_begin=True, fetch=True)
                has_error = False if str(result[0]) == "SUCCESS" else True
                if has_error:
                    raise DbtDatabaseError("Encountered error in Trino dry run.")

        elif self.sql_engine_type is SqlEngine.BIGQUERY:
            with self._adapter.connection_named(connection_name):
                self._adapter.validate_sql(stmt)
        else:
            is_databricks = self.sql_engine_type is SqlEngine.DATABRICKS
            with self._adapter.connection_named(connection_name):
                results = self._adapter.execute(f"EXPLAIN {stmt}", auto_begin=True, fetch=is_databricks)

            if is_databricks:
                # We have to parse the output results from adapter.execute, but only in Databricks. We should probably use
                # a subclass for this and override things properly, but there's hopefully an upstream fix in our near
                # future so let's just put this ugly bit in for now.
                plan_output_str = "\n".join([",".join(row.values()) for row in results[1].rows])
                has_error = (
                    plan_output_str.find(DATABRICKS_CLUSTER_EXPLAIN_PLAN_ERROR_KEY) != -1
                    or plan_output_str.find(DATABRICKS_SQL_WAREHOUSE_EXPLAIN_PLAN_ERROR_KEY) != -1
                )
                if has_error:
                    raise DbtDatabaseError(f"Encountered error in Databricks dry run. Full output: {plan_output_str}")

        stop = time.perf_counter()
        logger.info(LazyFormat("Finished running the dry run", runtime=f"{stop - start:.2f}s"))
        return

    def close(self) -> None:  # noqa: D102
        self._adapter.cancel_open_connections()

    def render_bind_parameter_key(self, bind_parameter_key: str) -> str:
        """Wrap execution parameter key with syntax accepted by engine."""
        raise SqlBindParametersNotSupportedError(
            "We do not support queries with bind parameters through dbt adapters, so we do not have rendering enabled!"
        )
