from __future__ import annotations

import logging
import time

from metricflow_semantics.errors.error_classes import SqlBindParametersNotSupportedError
from metricflow_semantics.sql.sql_bind_parameters import SqlBindParameterSet
from metricflow_semantics.toolkit.mf_logging.lazy_formattable import LazyFormat
from sqlalchemy import Engine
from sqlalchemy import text as sa_text
from sqlalchemy.exc import SQLAlchemyError

from metricflow.data_table.mf_table import MetricFlowDataTable
from metricflow.protocols.sql_client import SqlEngine
from metricflow.sql.render.sql_plan_renderer import SqlPlanRenderer

logger = logging.getLogger(__name__)


class SqlAlchemyBasedSqlClient:
    """SqlClient implementation using SqlAlchemy Engine for database operations."""

    def __init__(
        self,
        engine: Engine,
        sql_engine_type: SqlEngine,
        sql_plan_renderer: SqlPlanRenderer,
    ) -> None:
        """Initialize with a SqlAlchemy Engine.

        Args:
            engine: SqlAlchemy Engine instance (already configured)
            sql_engine_type: MetricFlow SqlEngine enum value
            sql_plan_renderer: Dialect-specific SQL plan renderer
        """
        self._engine = engine
        self._sql_engine_type = sql_engine_type
        self._sql_plan_renderer = sql_plan_renderer
        logger.debug(f"Initialized SqlAlchemyBasedSqlClient with engine type `{sql_engine_type.value}`")

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
            logger.error(f"Query failed: {e}")
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
            logger.error(f"Execute failed: {e}")
            raise

        stop = time.perf_counter()
        logger.info(f"Finished execute() in {stop - start:.2f}s")

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
            with self._engine.connect() as conn:
                if self.sql_engine_type is SqlEngine.TRINO:
                    # Trino: Use EXPLAIN (type validate) to avoid side effects
                    result = conn.execute(sa_text(f"EXPLAIN (type validate) {stmt}"))
                elif self.sql_engine_type is SqlEngine.BIGQUERY:
                    # BigQuery: Use dry_run execution option
                    # Note: This requires specific BigQuery dialect configuration
                    # For now, use EXPLAIN as fallback
                    conn.execute(sa_text(f"EXPLAIN {stmt}"))

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
            logger.error(f"Dry run failed: {e}")
            raise

        stop = time.perf_counter()
        logger.info(f"Finished dry run in {stop - start:.2f}s")

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
