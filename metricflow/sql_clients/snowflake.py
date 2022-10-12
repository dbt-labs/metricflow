from __future__ import annotations

import logging
import threading
import urllib.parse
from contextlib import contextmanager
from typing import ClassVar, Optional, Dict, Iterator, List, Tuple, Any, Set, Sequence

import pandas as pd
import sqlalchemy
from sqlalchemy.exc import ProgrammingError

from metricflow.protocols.sql_client import SqlEngine, SqlEngineAttributes
from metricflow.sql.render.sql_plan_renderer import DefaultSqlQueryPlanRenderer
from metricflow.sql.render.sql_plan_renderer import SqlQueryPlanRenderer
from metricflow.sql.sql_bind_parameters import SqlBindParameters
from metricflow.sql_clients.common_client import SqlDialect, not_empty
from metricflow.sql_clients.sqlalchemy_dialect import SqlAlchemySqlClient


class SnowflakeEngineAttributes(SqlEngineAttributes):
    """Engine-specific attributes for the Snowflake query engine

    This is an implementation of the SqlEngineAttributes protocol for Snowflake
    """

    sql_engine_type: ClassVar[SqlEngine] = SqlEngine.SNOWFLAKE

    # SQL Engine capabilities
    date_trunc_supported: ClassVar[bool] = True
    full_outer_joins_supported: ClassVar[bool] = True
    indexes_supported: ClassVar[bool] = False
    multi_threading_supported: ClassVar[bool] = True
    timestamp_type_supported: ClassVar[bool] = True
    timestamp_to_string_comparison_supported: ClassVar[bool] = True
    cancel_submitted_queries_supported: ClassVar[bool] = True

    # SQL Dialect replacement strings
    double_data_type_name: ClassVar[str] = "DOUBLE"
    timestamp_type_name: ClassVar[Optional[str]] = "TIMESTAMP"

    # MetricFlow attributes
    sql_query_plan_renderer: ClassVar[SqlQueryPlanRenderer] = DefaultSqlQueryPlanRenderer()


logger = logging.getLogger(__name__)


class SnowflakeSqlClient(SqlAlchemySqlClient):
    """Client for Snowflake.

    Note: By default, Snowflake uses uppercase for schema, table, and column
    names. To create or access them as lowercase, you must use double quotes.

    It's also tricky trying to get tests / queries on Snowflake working with
    https://docs.snowflake.com/en/sql-reference/parameters.html#quoted-identifiers-ignore-case enabled.
    For example, when listing table names, all tables would be upper case with that setting (causing an issue where
    data sources would constantly be primed because the table names didn't match).
    """

    DEFAULT_LOGIN_TIMEOUT = 60
    DEFAULT_CLIENT_SESSION_KEEP_ALIVE = True

    @staticmethod
    def _parse_url_query_params(url: str) -> Dict[str, str]:
        """Gets the warehouse from the query parameters in the URL, throwing an exception if not set properly."""
        url_query_params: Dict[str, str] = {}

        parsed_url = urllib.parse.urlparse(url)
        query_dict = urllib.parse.parse_qs(parsed_url.query)

        if "warehouse" not in query_dict:
            raise ValueError(f"Missing warehouse in URL query: {url}")

        if len(query_dict["warehouse"]) > 1:
            raise ValueError(f"Multiple warehouses in URL query: {url}")

        url_query_params["warehouse"] = query_dict["warehouse"][0]

        # optionally, role
        if "role" not in query_dict:
            return url_query_params

        if len(query_dict["role"]) > 1:
            raise ValueError(f"Multiple roles in URL query: {url}")

        url_query_params["role"] = query_dict["role"][0]
        return url_query_params

    @staticmethod
    def from_connection_details(url: str, password: Optional[str]) -> SnowflakeSqlClient:  # noqa: D
        parsed_url = sqlalchemy.engine.make_url(url)
        if parsed_url.drivername != SqlDialect.SNOWFLAKE.value:
            raise ValueError(f"Invalid dialect in URL for Snowflake: {url}")

        if parsed_url.port:
            raise ValueError(f"Snowflake URL should not have a port set: {url}")

        if not password:
            raise ValueError(f"Password not supplied for {url}")

        SqlAlchemySqlClient.validate_query_params(
            url=parsed_url, required_parameters={"warehouse"}, optional_parameters={"role"}
        )

        return SnowflakeSqlClient(
            host=not_empty(parsed_url.host, "host", url),
            username=not_empty(parsed_url.username, "username", url),
            password=password,
            database=not_empty(parsed_url.database, "database", url),
            url_query_params=SnowflakeSqlClient._parse_url_query_params(url),
        )

    def __init__(  # noqa: D
        self,
        database: str,
        username: str,
        password: str,
        host: str,
        url_query_params: Dict[str, str],
        login_timeout: int = DEFAULT_LOGIN_TIMEOUT,
        client_session_keep_alive: bool = DEFAULT_CLIENT_SESSION_KEEP_ALIVE,
    ) -> None:
        self._connection_url = SqlAlchemySqlClient.build_engine_url(
            dialect=SqlDialect.SNOWFLAKE.value,
            username=username,
            password=password,
            host=host,
            database=database,
            query=url_query_params,
        )
        self._engine_lock = threading.Lock()
        self._known_sessions_ids_lock = threading.Lock()
        self._known_session_ids: Set[int] = set()
        super().__init__(
            engine=self._create_engine(login_timeout=login_timeout, client_session_keep_alive=client_session_keep_alive)
        )

    def _create_engine(
        self,
        login_timeout: int = DEFAULT_LOGIN_TIMEOUT,
        client_session_keep_alive: bool = DEFAULT_CLIENT_SESSION_KEEP_ALIVE,
    ) -> sqlalchemy.engine.Engine:  # noqa: D
        return sqlalchemy.create_engine(
            self._connection_url,
            pool_size=10,
            max_overflow=10,
            pool_pre_ping=False,
            connect_args={"client_session_keep_alive": client_session_keep_alive, "login_timeout": login_timeout},
        )

    @property
    def sql_engine_attributes(self) -> SqlEngineAttributes:
        """Collection of attributes and features specific to the Snowflake SQL engine"""
        return SnowflakeEngineAttributes()

    @contextmanager
    def engine_connection(self, engine: sqlalchemy.engine.Engine) -> Iterator[sqlalchemy.engine.Connection]:
        """Context Manager for providing a configured connection.

        Snowflake allows setting a WEEK_START parameter on each session. This forces the value to be
        1, which means Monday. Future updates could parameterize this to read from some kind of
        options construct, which the DBClient could read in at initialization and use here (for example).
        At this time we hard-code the ISO standard.
        """

        with super().engine_connection(self._engine) as conn:
            # WEEK_START 1 means Monday.
            conn.execute("ALTER SESSION SET WEEK_START = 1;")
            results = conn.execute("SELECT CURRENT_SESSION()")
            sessions = []
            for row in results:
                sessions.append(row[0])
            assert len(sessions) == 1
            session = sessions[0]
            with self._known_sessions_ids_lock:
                self._known_session_ids.add(session)
            yield conn
            with self._known_sessions_ids_lock:
                self._known_session_ids.remove(session)

    def _query(  # noqa: D
        self,
        stmt: str,
        bind_params: SqlBindParameters = SqlBindParameters(),
        allow_re_auth: bool = True,
    ) -> pd.DataFrame:
        with self.engine_connection(engine=self._engine) as conn:
            try:
                return pd.read_sql_query(sqlalchemy.text(stmt), conn, params=bind_params.param_dict)
            except ProgrammingError as e:
                if "Authentication token has expired" in str(e) and allow_re_auth:
                    logger.warning(
                        "Snowflake authentication token expired. Attempting to re-auth, then we'll re-run the query"
                    )
                    with self._engine_lock:
                        self._engine.dispose()
                        self._engine = self._create_engine()
                    # this was our one chance to re-auth
                    return self._query(stmt, allow_re_auth=False, bind_params=bind_params)
                raise e

    def _engine_specific_query_implementation(  # noqa: D
        self, stmt: str, bind_params: SqlBindParameters
    ) -> pd.DataFrame:
        return self._query(stmt, bind_params)

    def list_tables(self, schema_name: str) -> Sequence[str]:  # noqa: D
        df = self.query(
            f"SHOW TABLES IN {schema_name}",
        )
        if df.empty:
            return []

        # Lower casing table names to be similar to other SQL clients. TBD on the implications of this.
        return [t.lower() for t in df["name"]]

    def generate_health_check_tests(self, schema_name: str) -> List[Tuple[str, Any]]:  # type: ignore # noqa: D
        additional_tests = [
            (
                "Connection State",
                lambda: str(
                    self.query(
                        "SELECT CURRENT_USER(), CURRENT_ROLE(), CURRENT_DATABASE(), CURRENT_WAREHOUSE(), CURRENT_SCHEMA();"
                    )
                ),
            )
        ]
        return super().generate_health_check_tests(schema_name=schema_name) + additional_tests

    def close(self) -> None:
        """Snowflake will hang pytest if this is not done."""
        with self._engine_lock:
            self._engine.dispose()

    def cancel_submitted_queries(self) -> None:  # noqa: D
        with super().engine_connection(self._engine) as conn:
            with self._known_sessions_ids_lock:
                for session_id in self._known_session_ids:
                    logger.info(f"Cancelling queries associated with session id: {session_id}")
                    conn.execute(f"SELECT SYSTEM$cancel_all_queries({session_id})")
