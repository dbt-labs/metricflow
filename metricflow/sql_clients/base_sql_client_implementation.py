import logging
import textwrap
import time
from abc import ABC, abstractmethod
from typing import Dict, List, Optional, Any, Tuple

import jinja2
import pandas as pd

from metricflow.dataflow.sql_table import SqlTable
from metricflow.protocols.sql_client import SqlEngineAttributes, SqlClient
from metricflow.sql.sql_bind_parameters import SqlBindParameters

logger = logging.getLogger(__name__)


class SqlClientException(Exception):
    """Raised when an interaction with the SQL engine has an error."""

    pass


class BaseSqlClientImplementation(ABC, SqlClient):
    """Abstract implementation that other SQL clients are based on."""

    INDENT = "    "

    def generate_health_check_tests(self, schema_name: str) -> List[Tuple[str, Any]]:  # type: ignore
        """List of base health checks we want to perform."""
        table_name = "health_report"
        return [
            ("SELECT 1", lambda: self.execute("SELECT 1")),
            (f"Create schema '{schema_name}'", lambda: self.create_schema(schema_name)),
            (
                f"Create table '{schema_name}.{table_name}' with a SELECT",
                lambda: self.create_table_as_select(
                    SqlTable(schema_name=schema_name, table_name="health_report"), "SELECT 'test' AS test_col"
                ),
            ),
            (
                f"Drop table '{schema_name}.{table_name}'",
                lambda: self.drop_table(SqlTable(schema_name=schema_name, table_name="health_report")),
            ),
        ]

    def health_checks(self, schema_name: str) -> Dict[str, Dict[str, str]]:
        """Perform health checks"""
        checks_to_run = self.generate_health_check_tests(schema_name)
        results: Dict[str, Dict[str, str]] = {}

        for step, check in checks_to_run:
            status = "SUCCESS"
            err_string = ""
            try:
                resp = check()
                logger.info(f"Health Check Item {step}: succeeded" + f" with response {str(resp)}" if resp else None)
            except Exception as e:
                status = "FAIL"
                err_string = str(e)
                logger.error(f"Health Check Item {step}: failed with error {err_string}")

            results[f"{type(self).__name__.lower()} - {step}"] = {
                "status": status,
                "error_message": err_string,
            }

        return results

    def query(
        self,
        stmt: str,
        sql_bind_parameters: SqlBindParameters = SqlBindParameters(),
    ) -> pd.DataFrame:
        """Query statement; result expected to be data which will be returned as a DataFrame

        :param stmt str:  - The SQL query statement to run. This should produce output via a SELECT
        :param sql_bind_parameters SqlQueryExecutionParameters: - The parameter replacement mapping for filling in
        concrete values for SQL query parameters.
        """
        start = time.time()
        logger.info(
            f"Running query:"
            f"\n\n{textwrap.indent(stmt, prefix=BaseSqlClientImplementation.INDENT)}\n"
            + (f"\nwith parameters: {dict(sql_bind_parameters.param_dict)}" if sql_bind_parameters.param_dict else "")
        )
        df = self._engine_specific_query_implementation(stmt, sql_bind_parameters)
        if not isinstance(df, pd.DataFrame):
            raise RuntimeError(f"Expected query to return a DataFrame, got {type(df)}")
        stop = time.time()
        logger.info(f"Finished running the query in {stop - start:.2f}s with {df.shape[0]} row(s) returned")
        return df

    def execute(  # noqa: D
        self,
        stmt: str,
        sql_bind_parameters: SqlBindParameters = SqlBindParameters(),
    ) -> None:
        start = time.time()
        logger.info(
            f"Running query:"
            f"\n\n{textwrap.indent(stmt, prefix=BaseSqlClientImplementation.INDENT)}\n"
            + (f"\nwith parameters: {dict(sql_bind_parameters.param_dict)}" if sql_bind_parameters.param_dict else "")
        )
        self._engine_specific_execute_implementation(stmt, sql_bind_parameters)
        stop = time.time()
        logger.info(f"Finished running the query in {stop - start:.2f}s")
        return None

    @property
    @abstractmethod
    def sql_engine_attributes(self) -> SqlEngineAttributes:
        """Struct of SQL engine attributes.

        This is used by MetricFlow for things like SQL rendering and safe use of multi-threading.
        """
        raise NotImplementedError

    @abstractmethod
    def _engine_specific_query_implementation(self, stmt: str, bind_params: SqlBindParameters) -> pd.DataFrame:
        """Sub-classes should implement this to query the engine."""
        pass

    @abstractmethod
    def _engine_specific_execute_implementation(self, stmt: str, bind_params: SqlBindParameters) -> None:
        """Sub-classes should implement this to execute a statement that doesn't return results."""
        pass

    @abstractmethod
    def create_table_from_dataframe(  # noqa: D
        self,
        sql_table: SqlTable,
        df: pd.DataFrame,
        chunk_size: Optional[int] = None,
    ) -> None:
        pass

    @abstractmethod
    def list_tables(self, schema_name: str) -> List[str]:  # noqa: D
        pass

    def table_exists(self, sql_table: SqlTable) -> bool:  # noqa: D
        return sql_table.table_name in self.list_tables(sql_table.schema_name)

    def create_table_as_select(  # noqa: D
        self,
        sql_table: SqlTable,
        select_query: str,
        sql_bind_parameters: SqlBindParameters = SqlBindParameters(),
    ) -> None:
        self.execute(
            jinja2.Template(
                textwrap.dedent(
                    """\
                    CREATE TABLE {{ sql_table }} AS
                      {{ select_query | indent(2) }}
                    """
                ),
                undefined=jinja2.StrictUndefined,
            ).render(
                sql_table=sql_table.sql,
                select_query=select_query,
            ),
            sql_bind_parameters=sql_bind_parameters,
        )

    def create_schema(self, schema_name: str) -> None:  # noqa: D
        self.execute(f"CREATE SCHEMA IF NOT EXISTS {schema_name}")

    def drop_schema(self, schema_name: str, cascade: bool = True) -> None:  # noqa: D
        self.execute(f"DROP SCHEMA IF EXISTS {schema_name}{' CASCADE' if cascade else ''}")

    def drop_table(self, sql_table: SqlTable) -> None:  # noqa: D
        self.execute(f"DROP TABLE IF EXISTS {sql_table.sql}")

    def close(self) -> None:  # noqa: D
        pass
