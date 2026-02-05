from __future__ import annotations

import logging
import time

from metricflow_semantics.errors.error_classes import SqlBindParametersNotSupportedError
from metricflow_semantics.sql.sql_bind_parameters import SqlBindParameterSet
from metricflow_semantics.toolkit.mf_logging.lazy_formattable import LazyFormat
from sqlalchemy import Engine
from sqlalchemy import text as sa_text
from sqlalchemy.dialects import registry
from sqlalchemy.dialects.postgresql.psycopg2 import PGDialect_psycopg2
from sqlalchemy.exc import SQLAlchemyError

from metricflow.data_table.mf_table import MetricFlowDataTable
from metricflow.protocols.sql_client import SqlEngine
from metricflow.sql.render.sql_plan_renderer import SqlPlanRenderer

logger = logging.getLogger(__name__)


class MetricFlowRedshiftDialect(PGDialect_psycopg2):
    """Custom dialect for Redshift.

    The sqlalchemy-redshift package does not support SqlAlchemy 2.x natively, so we include this simple custom
    dialect that allows for connection via SqlAlchemy's create_engine API. Note this will not support all
    SqlAlchemy features, but since we only use the engine connection and direct execution of sql text statements
    this is sufficient for our purposes.

    A full-featured redshift SqlClient would need to instead use something like redshift-connector, or fall back
    to sqlalchemy-redshift with sqlalchemy < 2.0.

    Note - none of the upstream base classe are typed, so we just override everything here for mypy.
    """

    # These properties are set at class level in the built in dialect classes, so we follow that standard here.
    name = "mf_redshift_psycopg2"
    supports_statement_cache = False

    def _set_backslash_escapes(self, connection):  # type: ignore
        """Override for problematic method in SqlAlchemy 2.x.

        This is a method returning a value that overrides a boolean property in the base class.
        Without this override the default Postgres dialect will execute a statement that redshift does not support.
        """
        return False


# Make our custom redshift dialect available to SqlAlchemy.
registry.register("mf_redshift_psycopg2", __name__, "MetricFlowRedshiftDialect")


class SqlAlchemyBasedSqlClient:
    """SqlClient implementation using SqlAlchemy Engine for database operations."""

    def __init__(
        self, engine: Engine, sql_engine_type: SqlEngine, sql_plan_renderer: SqlPlanRenderer, dry_run_engine: Engine
    ) -> None:
        """Initialize with a SqlAlchemy Engine.

        Args:
            engine: SqlAlchemy Engine instance (already configured)
            sql_engine_type: MetricFlow SqlEngine enum value
            sql_plan_renderer: Dialect-specific SQL plan renderer
            dry_run_engine: SqlAlchemy Engine instance for dry run operations.
              This exists because BigQuery requires a different engine configuration for dry run operations.
        """
        self._engine = engine
        self._dry_run_engine = engine if dry_run_engine is None else dry_run_engine
        self._sql_engine_type = sql_engine_type
        self._sql_plan_renderer = sql_plan_renderer
        logger.debug(LazyFormat("Initialized SqlAlchemyBasedSqlClient.", engine_type=f"{sql_engine_type.value}"))

    @property
    def sql_engine_type(self) -> SqlEngine:
        """Return the SqlEngine type for this client."""
        return self._sql_engine_type

    @property
    def sql_plan_renderer(self) -> SqlPlanRenderer:
        """Return the SQL plan renderer for this client."""
        return self._sql_plan_renderer

    def query(
        self,
        stmt: str,
        sql_bind_parameter_set: SqlBindParameterSet = SqlBindParameterSet(),
    ) -> MetricFlowDataTable:
        """Execute a query and return results as MetricFlowDataTable.

        Uses SqlAlchemy Connection with automatic transaction management.
        For SELECT queries, transaction is rolled back after fetching results.
        """
        start = time.perf_counter()

        if sql_bind_parameter_set.param_dict:
            raise SqlBindParametersNotSupportedError(
                f"Bind parameters not yet supported in SqlAlchemy client. "
                f"Params: {sql_bind_parameter_set.param_dict}"
            )

        logger.info(
            LazyFormat(
                "Running query() statement",
                statement=stmt,
                param_dict=sql_bind_parameter_set.param_dict,
            )
        )

        try:
            # Use context manager for automatic connection lifecycle
            with self._engine.connect() as conn:
                # Execute query - SqlAlchemy automatically starts a transaction
                result = conn.execute(sa_text(stmt))

                # Fetch all rows
                rows = result.fetchall()
                column_names = list(result.keys())

                # Convert to MetricFlowDataTable format
                data_table = MetricFlowDataTable.create_from_rows(
                    column_names=column_names,
                    rows=[list(row) for row in rows],
                )

                # Transaction is automatically rolled back on context exit
                # (appropriate for SELECT queries)

        except SQLAlchemyError as e:
            logger.error(LazyFormat("Query failed:", error=f"{e}"))
            raise

        stop = time.perf_counter()
        logger.info(
            LazyFormat(
                "Finished running query()",
                runtime=f"{stop - start:.2f}s",
                returned_row_count=data_table.row_count,
            )
        )

        return data_table

    def execute(
        self,
        stmt: str,
        sql_bind_parameter_set: SqlBindParameterSet = SqlBindParameterSet(),
    ) -> None:
        """Execute a statement without returning results.

        Commits the transaction for DDL/DML statements.
        """
        if sql_bind_parameter_set.param_dict:
            raise SqlBindParametersNotSupportedError(
                f"Bind parameters not yet supported in SqlAlchemy client. "
                f"Params: {sql_bind_parameter_set.param_dict}"
            )

        start = time.perf_counter()

        logger.info(
            LazyFormat(
                "Running execute() statement",
                statement=stmt,
                param_dict=sql_bind_parameter_set.param_dict,
            )
        )

        try:
            with self._engine.connect() as conn:
                conn.execute(sa_text(stmt))
                # Explicitly commit for DDL/DML operations
                conn.commit()

        except SQLAlchemyError as e:
            logger.error(LazyFormat("Execute failed:", error=f"{e}"))
            raise

        stop = time.perf_counter()
        logger.info(LazyFormat("Finished execute()", runtime=f"{stop - start:.2f}s"))

    def dry_run(
        self,
        stmt: str,
        sql_bind_parameter_set: SqlBindParameterSet = SqlBindParameterSet(),
    ) -> None:
        """Validate a statement can be executed without running it.

        Uses EXPLAIN or engine-specific validation.
        """
        start = time.perf_counter()
        logger.info(
            LazyFormat(
                "Running dry run",
                statement=stmt,
                param_dict=sql_bind_parameter_set.param_dict,
            )
        )

        try:
            with self._dry_run_engine.connect() as conn:
                if self.sql_engine_type is SqlEngine.TRINO:
                    # Trino: Use EXPLAIN (type validate) to avoid side effects
                    result = conn.execute(sa_text(f"EXPLAIN (type validate) {stmt}"))
                elif self.sql_engine_type is SqlEngine.BIGQUERY:
                    # BigQuery uses an engine configured in dry_run mode
                    conn.execute(sa_text(f"{stmt}"))

                elif self.sql_engine_type is SqlEngine.DATABRICKS:
                    # Databricks: EXPLAIN returns plan, check for error markers
                    result = conn.execute(sa_text(f"EXPLAIN {stmt}"))
                    plan_output = "\n".join([str(row) for row in result.fetchall()])

                    # Check for known error markers
                    if (
                        "Error occurred during query planning" in plan_output
                        or "org.apache.spark.sql.AnalysisException" in plan_output
                    ):
                        raise RuntimeError(f"Databricks dry run failed: {plan_output}")

                else:
                    # Default: Use EXPLAIN for other engines
                    conn.execute(sa_text(f"EXPLAIN {stmt}"))

                # No commit needed for dry run - transaction rolls back

        except SQLAlchemyError as e:
            logger.error(LazyFormat("Dry run failed:", error=f"{e}"))
            raise

        stop = time.perf_counter()
        logger.info(LazyFormat("Finished dry run", runtime=f"{stop - start:.2f}s"))

    def close(self) -> None:
        """Close the SqlAlchemy engine and all connections."""
        self._engine.dispose()
        logger.debug("SqlAlchemy engine disposed")

    def render_bind_parameter_key(self, bind_parameter_key: str) -> str:
        """Wrap bind parameter key with engine-specific syntax.

        Currently not implemented as bind parameters are not supported
        through the adapter client interface.
        """
        raise SqlBindParametersNotSupportedError("Bind parameters not yet supported in SqlAlchemy client")
