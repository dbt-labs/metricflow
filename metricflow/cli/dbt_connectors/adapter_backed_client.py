from __future__ import annotations

import enum
import logging
import time

import pandas as pd
from dbt.adapters.base.impl import BaseAdapter
from dbt.exceptions import DbtDatabaseError
from dbt_semantic_interfaces.enum_extension import assert_values_exhausted
from dbt_semantic_interfaces.pretty_print import pformat_big_objects

from metricflow.errors.errors import SqlBindParametersNotSupportedError
from metricflow.formatting import indent_log_line
from metricflow.protocols.sql_client import SqlEngine
from metricflow.random_id import random_id
from metricflow.sql.render.big_query import BigQuerySqlQueryPlanRenderer
from metricflow.sql.render.databricks import DatabricksSqlQueryPlanRenderer
from metricflow.sql.render.duckdb_renderer import DuckDbSqlQueryPlanRenderer
from metricflow.sql.render.postgres import PostgresSQLSqlQueryPlanRenderer
from metricflow.sql.render.redshift import RedshiftSqlQueryPlanRenderer
from metricflow.sql.render.snowflake import SnowflakeSqlQueryPlanRenderer
from metricflow.sql.render.sql_plan_renderer import SqlQueryPlanRenderer
from metricflow.sql.sql_bind_parameters import SqlBindParameters
from metricflow.sql_request.sql_request_attributes import SqlJsonTag, SqlRequestId, SqlRequestTagSet
from metricflow.sql_request.sql_statement_metadata import CombinedSqlTags, SqlStatementCommentMetadata

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
        else:
            assert_values_exhausted(self)

    @property
    def sql_query_plan_renderer(self) -> SqlQueryPlanRenderer:
        """Return the SqlQueryPlanRenderer corresponding to the supported adapter type."""
        if self is SupportedAdapterTypes.BIGQUERY:
            return BigQuerySqlQueryPlanRenderer()
        elif self is SupportedAdapterTypes.DATABRICKS:
            return DatabricksSqlQueryPlanRenderer()
        elif self is SupportedAdapterTypes.POSTGRES:
            return PostgresSQLSqlQueryPlanRenderer()
        elif self is SupportedAdapterTypes.REDSHIFT:
            return RedshiftSqlQueryPlanRenderer()
        elif self is SupportedAdapterTypes.SNOWFLAKE:
            return SnowflakeSqlQueryPlanRenderer()
        elif self is SupportedAdapterTypes.DUCKDB:
            return DuckDbSqlQueryPlanRenderer()
        else:
            assert_values_exhausted(self)


class AdapterBackedSqlClient:
    """SqlClient implementation which delegates database operations to a dbt BaseAdapter instance.

    This is a generic wrpaper class meant to cover all of our logging, querying, and internal configuration
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
        self._sql_query_plan_renderer = adapter_type.sql_query_plan_renderer
        logger.info(f"Initialized AdapterBackedSqlClient with dbt adapter type `{adapter_type.value}`")

    @property
    def sql_engine_type(self) -> SqlEngine:
        """An enumerated value representing the underlying SqlEngine supported by the dbt adapter for this instance."""
        return self._sql_engine_type

    @property
    def sql_query_plan_renderer(self) -> SqlQueryPlanRenderer:
        """Dialect-specific SQL query plan renderer used for converting MetricFlow's query plan to executable SQL."""
        return self._sql_query_plan_renderer

    def query(
        self,
        stmt: str,
        sql_bind_parameters: SqlBindParameters = SqlBindParameters(),
        extra_tags: SqlJsonTag = SqlJsonTag(),
    ) -> pd.DataFrame:
        """Query statement; result expected to be data which will be returned as a DataFrame.

        Args:
            stmt: The SQL query statement to run. This should produce output via a SELECT
            sql_bind_parameters: The parameter replacement mapping for filling in
                concrete values for SQL query parameters.
            extra_tags: An object containing JSON serialized tags meant for annotating queries.
        """
        start = time.time()
        request_id = SqlRequestId(f"mf_rid__{random_id()}")
        combined_tags = AdapterBackedSqlClient._consolidate_tags(json_tags=extra_tags, request_id=request_id)
        statement = SqlStatementCommentMetadata.add_tag_metadata_as_comment(
            sql_statement=stmt, combined_tags=combined_tags
        )
        if sql_bind_parameters.param_dict:
            raise SqlBindParametersNotSupportedError(
                f"Invalid execute statement - we do not support queries with bind parameters through dbt adapters! "
                f"Bind params: {sql_bind_parameters.param_dict}"
            )
        logger.info(AdapterBackedSqlClient._format_run_query_log_message(statement, sql_bind_parameters))
        with self._adapter.connection_named(f"MetricFlow_request_{request_id}"):
            # returns a Tuple[AdapterResponse, agate.Table] but the decorator converts it to Any
            result = self._adapter.execute(sql=statement, auto_begin=True, fetch=True)
            logger.info(f"Query returned from dbt Adapter with response {result[0]}")

        agate_data = result[1]
        df = pd.DataFrame([row.values() for row in agate_data.rows], columns=agate_data.column_names)
        stop = time.time()
        logger.info(f"Finished running the query in {stop - start:.2f}s with {df.shape[0]} row(s) returned")
        return df

    def execute(
        self,
        stmt: str,
        sql_bind_parameters: SqlBindParameters = SqlBindParameters(),
        extra_tags: SqlJsonTag = SqlJsonTag(),
    ) -> None:
        """Execute a SQL statement. No result will be returned.

        Args:
            stmt: The SQL query statement to run. This should not produce output.
            sql_bind_parameters: The parameter replacement mapping for filling in
                concrete values for SQL query parameters.
            extra_tags: An object containing JSON serialized tags meant for annotating queries.
        """
        if sql_bind_parameters.param_dict:
            raise SqlBindParametersNotSupportedError(
                f"Invalid execute statement - we do not support execute commands with bind parameters through dbt "
                f"adapters! Bind params: {SqlBindParameters.param_dict}"
            )
        start = time.time()
        request_id = SqlRequestId(f"mf_rid__{random_id()}")
        combined_tags = AdapterBackedSqlClient._consolidate_tags(json_tags=extra_tags, request_id=request_id)
        statement = SqlStatementCommentMetadata.add_tag_metadata_as_comment(
            sql_statement=stmt, combined_tags=combined_tags
        )
        logger.info(AdapterBackedSqlClient._format_run_query_log_message(statement, sql_bind_parameters))
        with self._adapter.connection_named(f"MetricFlow_request_{request_id}"):
            result = self._adapter.execute(statement, auto_begin=True, fetch=False)
            # Calls to execute often involve some amount of DDL so we commit here
            self._adapter.commit_if_has_connection()
            logger.info(f"Query executed via dbt Adapter with response {result[0]}")
        stop = time.time()
        logger.info(f"Finished running the query in {stop - start:.2f}s")
        return None

    def dry_run(
        self,
        stmt: str,
        sql_bind_parameters: SqlBindParameters = SqlBindParameters(),
    ) -> None:
        """Dry run statement; checks that the 'stmt' is queryable. Returns None on success.

        Raises an exception if the 'stmt' isn't queryable.

        Args:
            stmt: The SQL query statement to dry run.
            sql_bind_parameters: The parameter replacement mapping for filling in
                concrete values for SQL query parameters.
        """
        start = time.time()
        logger.info(
            f"Running dry_run of:"
            f"\n\n{indent_log_line(stmt)}\n"
            + (f"\nwith parameters: {dict(sql_bind_parameters.param_dict)}" if sql_bind_parameters.param_dict else "")
        )
        request_id = SqlRequestId(f"mf_rid__{random_id()}")
        connection_name = f"MetricFlow_dry_run_request_{request_id}"
        # TODO - consolidate to self._adapter.validate_sql() when all implementations will work from within MetricFlow
        if self.sql_engine_type is SqlEngine.BIGQUERY:
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

        stop = time.time()
        logger.info(f"Finished running the dry_run in {stop - start:.2f}s")
        return

    def close(self) -> None:  # noqa: D
        self._adapter.cancel_open_connections()

    def render_bind_parameter_key(self, bind_parameter_key: str) -> str:
        """Wrap execution parameter key with syntax accepted by engine."""
        raise SqlBindParametersNotSupportedError(
            "We do not support queries with bind parameters through dbt adapters, so we do not have rendering enabled!"
        )

    @staticmethod
    def _format_run_query_log_message(statement: str, sql_bind_parameters: SqlBindParameters) -> str:
        """Helper for creating nicely formatted query logging."""
        message = f"Running query:\n\n{indent_log_line(statement)}"
        if len(sql_bind_parameters.param_dict) > 0:
            message += (
                f"\n"
                f"\n"
                f"with parameters:\n"
                f"\n"
                f"{indent_log_line(pformat_big_objects(sql_bind_parameters.param_dict))}"
            )
        return message

    @staticmethod
    def _consolidate_tags(json_tags: SqlJsonTag, request_id: SqlRequestId) -> CombinedSqlTags:
        """Consolidates json tags and request ID into a single set of tags."""
        return CombinedSqlTags(
            system_tags=SqlRequestTagSet().add_request_id(request_id=request_id),
            extra_tag=json_tags,
        )
