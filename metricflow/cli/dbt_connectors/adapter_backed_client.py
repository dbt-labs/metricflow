from __future__ import annotations

import enum
import logging
import textwrap
import time
from typing import Optional, Sequence

import pandas as pd
from dbt.adapters.base.impl import BaseAdapter
from dbt_semantic_interfaces.enum_extension import assert_values_exhausted
from dbt_semantic_interfaces.pretty_print import pformat_big_objects

from metricflow.dataflow.sql_table import SqlTable
from metricflow.errors.errors import SqlBindParametersNotSupportedError
from metricflow.logging.formatting import indent_log_line
from metricflow.protocols.sql_client import SqlEngine
from metricflow.protocols.sql_request import SqlJsonTag, SqlRequestId, SqlRequestTagSet
from metricflow.random_id import random_id
from metricflow.sql.render.postgres import PostgresSQLSqlQueryPlanRenderer
from metricflow.sql.render.snowflake import SnowflakeSqlQueryPlanRenderer
from metricflow.sql.render.sql_plan_renderer import SqlQueryPlanRenderer
from metricflow.sql.sql_bind_parameters import SqlBindParameters
from metricflow.sql_clients.sql_statement_metadata import CombinedSqlTags, SqlStatementCommentMetadata

logger = logging.getLogger(__name__)


class SupportedAdapterTypes(enum.Enum):
    """Enumeration of supported dbt adapter types."""

    POSTGRES = "postgres"
    SNOWFLAKE = "snowflake"

    @property
    def sql_engine_type(self) -> SqlEngine:
        """Return the SqlEngine corresponding to the supported adapter type."""
        if self is SupportedAdapterTypes.POSTGRES:
            return SqlEngine.POSTGRES
        elif self is SupportedAdapterTypes.SNOWFLAKE:
            return SqlEngine.SNOWFLAKE
        else:
            assert_values_exhausted(self)

    @property
    def sql_query_plan_renderer(self) -> SqlQueryPlanRenderer:
        """Return the SqlQueryPlanRenderer corresponding to the supported adapter type."""
        if self is SupportedAdapterTypes.POSTGRES:
            return PostgresSQLSqlQueryPlanRenderer()
        elif self is SupportedAdapterTypes.SNOWFLAKE:
            return SnowflakeSqlQueryPlanRenderer()
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
        # TODO - rely on self._adapter.dry_run() when it is available so this will work for BigQuery.
        self.execute(f"EXPLAIN {stmt}")
        stop = time.time()
        logger.info(f"Finished running the dry_run in {stop - start:.2f}s")
        return

    def create_table_from_dataframe(
        self,
        sql_table: SqlTable,
        df: pd.DataFrame,
        chunk_size: Optional[int] = None,
    ) -> None:
        """Create a table in the data warehouse containing the contents of the dataframe.

        Only used in tutorials and tests.

        Args:
            sql_table: The SqlTable object representing the table location to use
            df: The Pandas DataFrame object containing the column schema and data to load
            chunk_size: The number of rows to insert per transaction
        """
        logger.info(f"Creating table '{sql_table.sql}' from a DataFrame with {df.shape[0]} row(s)")
        start_time = time.time()
        with self._adapter.connection_named("MetricFlow_create_from_dataframe"):
            # Create table
            # update dtypes to convert None to NA in boolean columns.
            # This mirrors the SQLAlchemy schema detection logic in pandas.io.sql
            df = df.convert_dtypes()
            columns = df.columns
            columns_to_insert = []
            for i in range(len(df.columns)):
                # Format as "column_name column_type"
                columns_to_insert.append(
                    f"{columns[i]} {self._get_type_from_pandas_dtype(str(df[columns[i]].dtype).lower())}"
                )
            self._adapter.execute(
                f"CREATE TABLE IF NOT EXISTS {sql_table.sql} ({', '.join(columns_to_insert)})",
                auto_begin=True,
                fetch=False,
            )
            self._adapter.commit_if_has_connection()

            # Insert rows
            values = []
            for row in df.itertuples(index=False, name=None):
                cells = []
                for cell in row:
                    if pd.isnull(cell):
                        # use null keyword instead of isNA/None/etc.
                        cells.append("null")
                    elif type(cell) in [str, pd.Timestamp]:
                        # Wrap cell in quotes & escape existing single quotes
                        escaped_cell = str(cell).replace("'", "''")
                        cells.append(f"'{escaped_cell}'")
                    else:
                        cells.append(str(cell))

                values.append(f"({', '.join(cells)})")
                if chunk_size and len(values) == chunk_size:
                    value_string = ",\n".join(values)
                    self._adapter.execute(
                        f"INSERT INTO {sql_table.sql} VALUES {value_string}", auto_begin=True, fetch=False
                    )
                    values = []
            if values:
                value_string = ",\n".join(values)
                self._adapter.execute(
                    f"INSERT INTO {sql_table.sql} VALUES {value_string}", auto_begin=True, fetch=False
                )
            # Commit all insert transaction at once
            self._adapter.commit_if_has_connection()

        logger.info(f"Created table '{sql_table.sql}' from a DataFrame in {time.time() - start_time:.2f}s")

    def _get_type_from_pandas_dtype(self, dtype: str) -> str:
        """Helper method to get the engine-specific type value.

        The dtype dict here is non-exhaustive but should be adequate for our needs.
        """
        # TODO: add type handling for string/bool/bigint types for all engines
        if dtype == "string" or dtype == "object":
            return "text"
        elif dtype == "boolean" or dtype == "bool":
            return "boolean"
        elif dtype == "int64":
            return "bigint"
        elif dtype == "float64":
            return self._sql_query_plan_renderer.expr_renderer.double_data_type
        elif dtype == "datetime64[ns]":
            return self._sql_query_plan_renderer.expr_renderer.timestamp_data_type
        else:
            raise ValueError(f"Encountered unexpected Pandas dtype ({dtype})!")

    def list_tables(self, schema_name: str) -> Sequence[str]:
        """Get a list of the table names in a given schema. Only used in tutorials and tests."""
        # TODO: Short term, make this work with as many engines as possible. Medium term, remove this altogether.
        if self.sql_engine_type is SqlEngine.SNOWFLAKE:
            # Snowflake likes capitalizing things, except when it doesn't. We can get away with this due to its
            # limited scope of usage.
            schema_name = schema_name.upper()

        df = self.query(
            textwrap.dedent(
                f"""\
                SELECT table_name FROM information_schema.tables
                WHERE table_schema = '{schema_name}'
                """
            ),
        )
        if df.empty:
            return []

        # Lower casing table names and data frame names for consistency between Snowflake and other clients.
        # As above, we can do this because it isn't used in any consequential situations.
        df.columns = df.columns.str.lower()
        return [t.lower() for t in df["table_name"]]

    def table_exists(self, sql_table: SqlTable) -> bool:
        """Check if a given table exists. Only used in tutorials and tests."""
        return sql_table.table_name in self.list_tables(sql_table.schema_name)

    def create_schema(self, schema_name: str) -> None:
        """Create the given schema in a data warehouse. Only used in tutorials and tests."""
        self.execute(f"CREATE SCHEMA IF NOT EXISTS {schema_name}")

    def drop_schema(self, schema_name: str, cascade: bool = True) -> None:
        """Drop the given schema from the data warehouse. Only used in tests."""
        self.execute(f"DROP SCHEMA IF EXISTS {schema_name}{' CASCADE' if cascade else ''}")

    def drop_table(self, sql_table: SqlTable) -> None:
        """Drop the given table from the data warehouse. Only used in tutorials and tests."""
        self.execute(f"DROP TABLE IF EXISTS {sql_table.sql}")

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
