from __future__ import annotations

import logging
import time
from abc import ABC
from contextlib import contextmanager
from typing import Callable, Iterator, Mapping, Optional, Sequence, Set, Union

import pandas as pd
import sqlalchemy
from sqlalchemy import inspect

from metricflow.dataflow.sql_table import SqlTable
from metricflow.protocols.sql_client import SqlIsolationLevel
from metricflow.protocols.sql_request import SqlJsonTag, SqlRequestTagSet
from metricflow.sql.sql_bind_parameters import SqlBindParameters
from metricflow.sql_clients.async_request import CombinedSqlTags
from metricflow.sql_clients.base_sql_client_implementation import BaseSqlClientImplementation
from metricflow.sql_clients.common_client import check_isolation_level

logger = logging.getLogger(__name__)


class SqlAlchemySqlClient(BaseSqlClientImplementation, ABC):
    """Base class for to create DBClients for engines supported by SQLAlchemy."""

    def __init__(self, engine: sqlalchemy.engine.Engine) -> None:  # noqa: D
        self._engine = engine
        super().__init__()

    @staticmethod
    def build_engine_url(  # noqa: D
        dialect: str,
        database: str,
        username: str,
        password: Optional[str],
        host: str,
        port: Optional[int] = None,
        query: Optional[Mapping[str, Union[str, Sequence[str]]]] = None,
        driver: Optional[str] = None,
    ) -> sqlalchemy.engine.url.URL:
        return sqlalchemy.engine.url.URL.create(
            f"{dialect}+{driver}" if driver else f"{dialect}",
            username=username,
            password=password,
            host=host,
            port=port,
            database=database,
            **({"query": query} if query is not None else {}),
        )

    @staticmethod
    def create_engine(  # noqa: D
        dialect: str,
        port: int,
        database: str,
        username: str,
        password: str,
        host: str,
        driver: Optional[str] = None,
        query: Optional[Mapping[str, Union[str, Sequence[str]]]] = None,
    ) -> sqlalchemy.engine.Engine:
        connect_url = SqlAlchemySqlClient.build_engine_url(
            dialect=dialect,
            driver=driver,
            username=username,
            password=password,
            host=host,
            port=port,
            database=database,
            query=query,
        )
        # Without pool_pre_ping, it's possible for timed-out connections to be returned to the client and cause errors.
        # However, this can cause increase latency for slow engines.
        return sqlalchemy.create_engine(
            connect_url,
            pool_size=10,
            max_overflow=10,
            pool_pre_ping=True,
        )

    def list_tables(self, schema_name: str) -> Sequence[str]:  # noqa: D
        insp = inspect(self._engine)
        return insp.get_table_names(schema=schema_name)

    @contextmanager
    def _engine_connection(
        self,
        engine: sqlalchemy.engine.Engine,
        isolation_level: Optional[SqlIsolationLevel] = None,
        system_tags: SqlRequestTagSet = SqlRequestTagSet(),
        extra_tags: SqlJsonTag = SqlJsonTag(),
    ) -> Iterator[sqlalchemy.engine.Connection]:
        """Context Manager for providing a configured connection."""
        check_isolation_level(self, isolation_level)

        if isolation_level is not None:
            # Passing isolation_level=None will throw an error in some engines.
            conn = engine.connect().execution_options(isolation_level=isolation_level.value)
        else:
            conn = engine.connect()
        try:
            yield conn
        finally:
            conn.close()

    def _engine_specific_query_implementation(
        self,
        stmt: str,
        bind_params: SqlBindParameters,
        isolation_level: Optional[SqlIsolationLevel] = None,
        system_tags: SqlRequestTagSet = SqlRequestTagSet(),
        extra_tags: SqlJsonTag = SqlJsonTag(),
    ) -> pd.DataFrame:
        with self._engine_connection(
            self._engine, isolation_level=isolation_level, system_tags=system_tags, extra_tags=extra_tags
        ) as conn:
            return pd.read_sql_query(sqlalchemy.text(stmt), conn, params=bind_params.param_dict)

    def _engine_specific_execute_implementation(
        self,
        stmt: str,
        bind_params: SqlBindParameters,
        isolation_level: Optional[SqlIsolationLevel] = None,
        system_tags: SqlRequestTagSet = SqlRequestTagSet(),
        extra_tags: SqlJsonTag = SqlJsonTag(),
    ) -> None:
        with self._engine_connection(
            self._engine, isolation_level=isolation_level, system_tags=system_tags, extra_tags=extra_tags
        ) as conn:
            conn.execute(sqlalchemy.text(stmt), bind_params.param_dict)

    def _engine_specific_dry_run_implementation(self, stmt: str, bind_params: SqlBindParameters) -> None:  # noqa: D
        with self._engine_connection(self._engine) as conn:
            s = "EXPLAIN " + stmt
            conn.execute(sqlalchemy.text(s), bind_params.param_dict)

    def create_table_from_dataframe(  # noqa: D
        self, sql_table: SqlTable, df: pd.DataFrame, chunk_size: Optional[int] = None
    ) -> None:
        logger.info(f"Creating table '{sql_table.sql}' from a DataFrame with {df.shape[0]} row(s)")
        start_time = time.time()
        with self._engine_connection(self._engine) as conn:
            pd.io.sql.to_sql(
                frame=df,
                name=sql_table.table_name,
                con=conn,
                schema=sql_table.schema_name,
                index=False,
                if_exists="fail",
                method="multi",
                chunksize=chunk_size,
            )
        logger.info(f"Created table '{sql_table.sql}' from a DataFrame in {time.time() - start_time:.2f}s")

    @staticmethod
    def validate_query_params(
        url: sqlalchemy.engine.url.URL,
        required_parameters: Set[str],
        optional_parameters: Set[str],
    ) -> None:
        """Checks that the query parameters in the URL only include the valid parameters specified."""
        query_keys = set(url.query.keys())
        errors = []
        if not query_keys.issuperset(required_parameters):
            errors.append(f"Missing required parameters {required_parameters - query_keys}")
        if not query_keys.issubset(required_parameters.union(optional_parameters)):
            errors.append(f"Found extra parameters {query_keys - required_parameters.union(optional_parameters)}")

        if errors:
            raise ValueError(f"Found errors in the URL: {url}\n" + "\n".join(errors))

    def cancel_request(self, match_function: Callable[[CombinedSqlTags], bool]) -> int:  # noqa: D
        raise NotImplementedError
