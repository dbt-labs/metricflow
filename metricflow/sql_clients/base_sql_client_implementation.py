from __future__ import annotations

import logging
import textwrap
import threading
import time
from abc import ABC, abstractmethod
from typing import Any, Tuple, Sequence
from typing import Optional, List, Dict

import jinja2
import pandas as pd

from metricflow.dataflow.sql_table import SqlTable
from metricflow.logging.formatting import indent_log_line
from metricflow.object_utils import random_id, pformat_big_objects
from metricflow.protocols.async_sql_client import AsyncSqlClient
from metricflow.protocols.sql_client import (
    SqlEngineAttributes,
)
from metricflow.protocols.sql_client import SqlIsolationLevel
from metricflow.protocols.sql_request import SqlRequestId, SqlRequestResult, SqlRequestTagSet, SqlJsonTag
from metricflow.sql.sql_bind_parameters import SqlBindParameters
from metricflow.sql_clients.async_request import SqlStatementCommentMetadata, CombinedSqlTags
from metricflow.sql_clients.common_client import check_isolation_level

logger = logging.getLogger(__name__)


class SqlClientException(Exception):
    """Raised when an interaction with the SQL engine has an error."""

    pass


class BaseSqlClientImplementation(ABC, AsyncSqlClient):
    """Abstract implementation that other SQL clients are based on."""

    def __init__(self) -> None:  # noqa: D
        self._request_id_to_thread: Dict[SqlRequestId, BaseSqlClientImplementation.SqlRequestExecutorThread] = {}
        self._state_lock = threading.Lock()

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

    @staticmethod
    def _format_run_query_log_message(statement: str, sql_bind_parameters: SqlBindParameters) -> str:
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

    def query(
        self,
        stmt: str,
        sql_bind_parameters: SqlBindParameters = SqlBindParameters(),
    ) -> pd.DataFrame:
        """Query statement; result expected to be data which will be returned as a DataFrame

        Args:
            stmt: The SQL query statement to run. This should produce output via a SELECT
            sql_bind_parameters: The parameter replacement mapping for filling in
                concrete values for SQL query parameters.
        """

        start = time.time()
        logger.info(BaseSqlClientImplementation._format_run_query_log_message(stmt, sql_bind_parameters))
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
        logger.info(BaseSqlClientImplementation._format_run_query_log_message(stmt, sql_bind_parameters))
        self._engine_specific_execute_implementation(stmt, sql_bind_parameters)
        stop = time.time()
        logger.info(f"Finished running the query in {stop - start:.2f}s")
        return None

    def dry_run(
        self,
        stmt: str,
        sql_bind_parameters: SqlBindParameters = SqlBindParameters(),
    ) -> None:
        """Dry run statement; checks that the 'stmt' is queryable. Returns None. Raises an exception if the 'stmt' isn't queryable.

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
        results = self._engine_specific_dry_run_implementation(stmt, sql_bind_parameters)
        stop = time.time()
        logger.info(f"Finished running the dry_run in {stop - start:.2f}s")
        return results

    @property
    @abstractmethod
    def sql_engine_attributes(self) -> SqlEngineAttributes:
        """Struct of SQL engine attributes.

        This is used by MetricFlow for things like SQL rendering and safe use of multi-threading.
        """
        raise NotImplementedError

    @abstractmethod
    def _engine_specific_query_implementation(
        self,
        stmt: str,
        bind_params: SqlBindParameters,
        isolation_level: Optional[SqlIsolationLevel] = None,
        system_tags: SqlRequestTagSet = SqlRequestTagSet(),
        extra_tags: SqlJsonTag = SqlJsonTag(),
    ) -> pd.DataFrame:
        """Sub-classes should implement this to query the engine."""
        pass

    @abstractmethod
    def _engine_specific_execute_implementation(
        self,
        stmt: str,
        bind_params: SqlBindParameters,
        isolation_level: Optional[SqlIsolationLevel] = None,
        system_tags: SqlRequestTagSet = SqlRequestTagSet(),
        extra_tags: SqlJsonTag = SqlJsonTag(),
    ) -> None:
        """Sub-classes should implement this to execute a statement that doesn't return results."""
        pass

    @abstractmethod
    def _engine_specific_dry_run_implementation(self, stmt: str, bind_params: SqlBindParameters) -> None:
        """Sub-classes should implement this to check a query will run successfully without actually running the query"""
        pass

    @abstractmethod
    def create_table_from_dataframe(  # noqa: D
        self,
        sql_table: SqlTable,
        df: pd.DataFrame,
        chunk_size: Optional[int] = None,
    ) -> None:
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

    def render_execution_param_key(self, execution_param_key: str) -> str:
        """Wrap execution parameter key with syntax accepted by engine."""
        return f":{execution_param_key}"

    def async_query(  # noqa: D
        self,
        statement: str,
        bind_parameters: SqlBindParameters = SqlBindParameters(),
        extra_tags: SqlJsonTag = SqlJsonTag(),
        isolation_level: Optional[SqlIsolationLevel] = None,
    ) -> SqlRequestId:
        check_isolation_level(self, isolation_level)
        with self._state_lock:
            request_id = SqlRequestId(f"mf_rid__{random_id()}")
            thread = BaseSqlClientImplementation.SqlRequestExecutorThread(
                sql_client=self,
                request_id=request_id,
                statement=statement,
                bind_parameters=bind_parameters,
                extra_tag=extra_tags,
                isolation_level=isolation_level,
            )
            self._request_id_to_thread[request_id] = thread
            self._request_id_to_thread[request_id].start()
            return request_id

    def async_execute(  # noqa: D
        self,
        statement: str,
        bind_parameters: SqlBindParameters = SqlBindParameters(),
        extra_tags: SqlJsonTag = SqlJsonTag(),
        isolation_level: Optional[SqlIsolationLevel] = None,
    ) -> SqlRequestId:
        check_isolation_level(self, isolation_level)
        with self._state_lock:
            request_id = SqlRequestId(f"mf_rid__{random_id()}")
            thread = BaseSqlClientImplementation.SqlRequestExecutorThread(
                sql_client=self,
                request_id=request_id,
                statement=statement,
                bind_parameters=bind_parameters,
                extra_tag=extra_tags,
                is_query=False,
                isolation_level=isolation_level,
            )
            self._request_id_to_thread[request_id] = thread
            self._request_id_to_thread[request_id].start()
            return request_id

    def async_request_result(self, query_id: SqlRequestId) -> SqlRequestResult:  # noqa: D
        thread: Optional[BaseSqlClientImplementation.SqlRequestExecutorThread] = None
        with self._state_lock:
            thread = self._request_id_to_thread.get(query_id)
            if thread is None:
                raise RuntimeError(
                    f"Query ID: {query_id} is not known. Either the query ID is invalid, or results for the query ID "
                    f"were already fetched."
                )

        thread.join()
        with self._state_lock:
            del self._request_id_to_thread[query_id]
        return thread.result

    def active_requests(self) -> Sequence[SqlRequestId]:  # noqa: D
        with self._state_lock:
            return tuple(executor_thread.request_id for executor_thread in self._request_id_to_thread.values())

    class SqlRequestExecutorThread(threading.Thread):
        """Thread that helps to execute a request to the SQL engine asynchronously."""

        def __init__(  # noqa: D
            self,
            sql_client: BaseSqlClientImplementation,
            request_id: SqlRequestId,
            statement: str,
            bind_parameters: SqlBindParameters,
            extra_tag: SqlJsonTag = SqlJsonTag(),
            is_query: bool = True,
            isolation_level: Optional[SqlIsolationLevel] = None,
        ) -> None:
            """Initializer.

            Args:
                sql_client: SQL client used to execute statements.
                request_id: The request ID associated with the statement.
                statement: The statement to execute.
                bind_parameters: The parameters to use for the statement.
                extra_tag: Tags that should be associated with the request for the statement.
                is_query: Whether the request is for .query (returns data) or .execute (does not return data)
                isolation_level: The isolation level to use for the query.
            """
            self._sql_client = sql_client
            self._request_id = request_id
            self._statement = statement
            self._bind_parameters = bind_parameters
            self._extra_tag = extra_tag
            self._result: Optional[SqlRequestResult] = None
            self._is_query = is_query
            self._isolation_level = isolation_level
            super().__init__(name=f"Async Execute SQL Request ID: {request_id}", daemon=True)

        def run(self) -> None:  # noqa: D
            start_time = time.time()
            try:
                combined_tags = CombinedSqlTags(
                    system_tags=SqlRequestTagSet().add_request_id(self._request_id),
                    extra_tag=self._extra_tag,
                )
                statement = SqlStatementCommentMetadata.add_tag_metadata_as_comment(
                    sql_statement=self._statement, combined_tags=combined_tags
                )

                logger.info(
                    BaseSqlClientImplementation._format_run_query_log_message(
                        statement=self._statement, sql_bind_parameters=self._bind_parameters
                    )
                )

                if self._is_query:
                    df = self._sql_client._engine_specific_query_implementation(
                        statement,
                        bind_params=self._bind_parameters,
                        isolation_level=self._isolation_level,
                        system_tags=combined_tags.system_tags,
                        extra_tags=self._extra_tag,
                    )
                    self._result = SqlRequestResult(df=df)
                else:
                    self._sql_client._engine_specific_execute_implementation(
                        statement,
                        bind_params=self._bind_parameters,
                        isolation_level=self._isolation_level,
                        system_tags=combined_tags.system_tags,
                        extra_tags=self._extra_tag,
                    )
                    self._result = SqlRequestResult(df=pd.DataFrame())
                logger.info(f"Successfully executed {self._request_id} in {time.time() - start_time:.2f}s")
            except Exception as e:
                logger.exception(
                    f"Unsuccessfully executed {self._request_id} in {time.time() - start_time:.2f}s with exception:"
                )
                self._result = SqlRequestResult(exception=e)

        @property
        def result(self) -> SqlRequestResult:  # noqa: D
            assert self._result is not None, ".result() should only be called once the thread is finished running"
            return self._result

        @property
        def request_id(self) -> SqlRequestId:  # noqa: D
            return self._request_id
