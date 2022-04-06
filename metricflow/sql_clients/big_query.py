from __future__ import annotations

import json
from typing import ClassVar, Optional, List

import sqlalchemy

from metricflow.protocols.sql_client import SupportedSqlEngine, SqlEngineAttributes
from metricflow.sql.render.big_query import BigQuerySqlQueryPlanRenderer
from metricflow.sql.render.sql_plan_renderer import SqlQueryPlanRenderer
from metricflow.sql_clients.common_client import SqlDialect
from metricflow.sql_clients.sqlalchemy_dialect import SqlAlchemySqlClient


class BigQueryEngineAttributes:
    """Engine-specific attributes for the BigQuery query engine

    This is an implementation of the SqlEngineAttributes protocol for BigQuery
    """

    sql_engine_type: ClassVar[SupportedSqlEngine] = SupportedSqlEngine.BIGQUERY

    # SQL Engine capabilities
    date_trunc_supported: ClassVar[bool] = True
    full_outer_joins_supported: ClassVar[bool] = True
    indexes_supported: ClassVar[bool] = False
    multi_threading_supported: ClassVar[bool] = True
    timestamp_type_supported: ClassVar[bool] = True
    timestamp_to_string_comparison_supported: ClassVar[bool] = False

    # SQL Dialect replacement strings
    double_data_type_name: ClassVar[str] = "FLOAT64"
    timestamp_type_name: ClassVar[Optional[str]] = "DATETIME"

    # MetricFlow attributes
    sql_query_plan_renderer: ClassVar[SqlQueryPlanRenderer] = BigQuerySqlQueryPlanRenderer()


class BigQuerySqlClient(SqlAlchemySqlClient):
    """BigQuery implementation of SQL client"""

    @staticmethod
    def from_connection_details(url: str, password: Optional[str]) -> BigQuerySqlClient:  # noqa: D
        parsed_url = sqlalchemy.engine.url.make_url(url)
        dialect = SqlDialect.BIGQUERY.value
        if parsed_url.drivername != dialect:
            raise ValueError(f"Expected dialect '{dialect}' in {url}")

        if password is None:
            raise ValueError(f"Credentials not supplied for {url}")

        return BigQuerySqlClient(password=password)

    def __init__(self, password: str, query_string: str = "") -> None:
        """Creates a new BigQueryDBClient and tags it for tracing as big query."""
        # Without pool_pre_ping, it's possible for timed-out connections to be returned to the client and cause errors.
        # However, this can cause increase latency for slow engines.
        self._password = password
        super().__init__(engine=BigQuerySqlClient._create_bq_engine(query_string=query_string, password=password))

    @staticmethod
    def _create_bq_engine(query_string: str, password: str) -> sqlalchemy.engine.Engine:
        """Create the connection engine in SqlAlchemy to connect to BQ"""
        return sqlalchemy.create_engine(
            f"{SqlDialect.BIGQUERY.value}://" + "/?" + query_string,
            credentials_info=json.loads(password),  # "password := string representation of keyfile.json",
            pool_size=10,
            max_overflow=10,
            pool_pre_ping=True,
        )

    @property
    def sql_engine_attributes(self) -> SqlEngineAttributes:
        """Collection of attributes and features specific to the BigQuery SQL engine"""
        return BigQueryEngineAttributes()

    def list_tables(self, schema_name: str) -> List[str]:  # noqa: D
        with self.engine_connection(engine=self._engine) as conn:
            insp = sqlalchemy.inspection.inspect(conn)
            schema_dot_tables = insp.get_table_names(schema=schema_name)
            return [x.replace(schema_name + ".", "") for x in schema_dot_tables]
