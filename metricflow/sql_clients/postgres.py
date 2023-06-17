from __future__ import annotations

import logging
from typing import Mapping, Optional, Sequence, Union

import sqlalchemy
from typing_extensions import override

from metricflow.protocols.sql_client import SqlEngine
from metricflow.sql.render.postgres import PostgresSQLSqlQueryPlanRenderer
from metricflow.sql.render.sql_plan_renderer import SqlQueryPlanRenderer
from metricflow.sql_clients.common_client import SqlDialect, not_empty
from metricflow.sql_clients.sqlalchemy_dialect import SqlAlchemySqlClient

logger = logging.getLogger(__name__)


class PostgresSqlClient(SqlAlchemySqlClient):
    """Implements Postgres."""

    @staticmethod
    def from_connection_details(url: str, password: Optional[str]) -> SqlAlchemySqlClient:  # noqa: D
        parsed_url = sqlalchemy.engine.url.make_url(url)
        dialect = SqlDialect.POSTGRESQL.value
        if parsed_url.drivername != dialect:
            raise ValueError(f"Expected dialect '{dialect}' in {url}")

        if password is None:
            raise ValueError(f"Password not supplied for {url}")

        return PostgresSqlClient(
            host=not_empty(parsed_url.host, "host", url),
            port=not_empty(parsed_url.port, "port", url),
            username=not_empty(parsed_url.username, "username", url),
            password=password,
            database=not_empty(parsed_url.database, "database", url),
            query=parsed_url.query,
        )

    def __init__(  # noqa: D
        self,
        port: int,
        database: str,
        username: str,
        password: str,
        host: str,
        query: Optional[Mapping[str, Union[str, Sequence[str]]]] = None,
    ) -> None:
        super().__init__(
            engine=self.create_engine(
                dialect=SqlDialect.POSTGRESQL.value,
                driver="psycopg2",
                port=port,
                database=database,
                username=username,
                password=password,
                host=host,
                query=query,
            )
        )

    @property
    @override
    def sql_engine_type(self) -> SqlEngine:
        return SqlEngine.POSTGRES

    @property
    @override
    def sql_query_plan_renderer(self) -> SqlQueryPlanRenderer:
        return PostgresSQLSqlQueryPlanRenderer()
