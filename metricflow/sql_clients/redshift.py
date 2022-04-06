import logging
from typing import ClassVar, Optional, Mapping, Union, Sequence

import sqlalchemy

from metricflow.protocols.sql_client import SupportedSqlEngine, SqlEngineAttributes
from metricflow.sql.render.redshift import RedshiftSqlQueryPlanRenderer
from metricflow.sql.render.sql_plan_renderer import SqlQueryPlanRenderer
from metricflow.sql_clients.common_client import SqlDialect, not_empty
from metricflow.sql_clients.sqlalchemy_dialect import SqlAlchemySqlClient

logger = logging.getLogger(__name__)


class RedshiftEngineAttributes:
    """Engine-specific attributes for the Redshift query engine

    This is an implementation of the SqlEngineAttributes protocol for Redshift
    """

    sql_engine_type: ClassVar[SupportedSqlEngine] = SupportedSqlEngine.REDSHIFT

    # SQL Engine capabilities
    date_trunc_supported: ClassVar[bool] = True
    full_outer_joins_supported: ClassVar[bool] = True
    indexes_supported: ClassVar[bool] = False
    multi_threading_supported: ClassVar[bool] = True
    timestamp_type_supported: ClassVar[bool] = True
    timestamp_to_string_comparison_supported: ClassVar[bool] = True

    # SQL Dialect replacement strings
    double_data_type_name: ClassVar[str] = "DOUBLE PRECISION"
    timestamp_type_name: ClassVar[Optional[str]] = "TIMESTAMP"

    # MetricFlow attributes
    sql_query_plan_renderer: ClassVar[SqlQueryPlanRenderer] = RedshiftSqlQueryPlanRenderer()


class RedshiftSqlClient(SqlAlchemySqlClient):
    """Implements Redshift."""

    @staticmethod
    def from_connection_details(url: str, password: Optional[str]) -> SqlAlchemySqlClient:  # noqa: D
        parsed_url = sqlalchemy.engine.url.make_url(url)
        dialect = SqlDialect.REDSHIFT.value
        if parsed_url.drivername != dialect:
            raise ValueError(f"Expected dialect '{dialect}' in {url}")

        if password is None:
            raise ValueError(f"Password not supplied for {url}")

        return RedshiftSqlClient(
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
                dialect=SqlDialect.REDSHIFT.value,
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
    def sql_engine_attributes(self) -> SqlEngineAttributes:
        """Collection of attributes and features specific to the Snowflake SQL engine"""
        return RedshiftEngineAttributes()
