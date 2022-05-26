from __future__ import annotations

import json
from typing import ClassVar, Optional, List

import sqlalchemy

from metricflow.protocols.sql_client import SupportedSqlEngine, SqlEngineAttributes
from metricflow.sql.render.big_query import BigQuerySqlQueryPlanRenderer
from metricflow.sql.render.sql_plan_renderer import SqlQueryPlanRenderer
from metricflow.sql.sql_bind_parameters import SqlBindParameters
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
    # Cancelling should be possible, but not yet implemented.
    cancel_submitted_queries_supported: ClassVar[bool] = False

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

    def __init__(self, project_id: str = "", password: Optional[str] = None, query_string: str = "") -> None:
        """Creates a new BigQueryDBClient and tags it for tracing as big query."""
        # Without pool_pre_ping, it's possible for timed-out connections to be returned to the client and cause errors.
        # However, this can cause increase latency for slow engines.
        self._password = password
        self._project_id = project_id
        super().__init__(
            engine=BigQuerySqlClient._create_bq_engine(
                query_string=query_string, project_id=project_id, password=password
            )
        )

    def _engine_specific_dry_run_implementation(self, stmt: str, bind_params: SqlBindParameters) -> None:
        """Overrides base `_engine_specific_dry_run_implementation` function for BigQuery specifics"""
        _engine = self._create_bq_engine(
            query_string="dry_run=true", project_id=self._project_id, password=self._password
        )
        with _engine.connect() as conn:
            conn.execute(sqlalchemy.text(stmt), bind_params.param_dict)

    @staticmethod
    def _create_bq_engine(
        query_string: str, project_id: str = "", password: Optional[str] = None
    ) -> sqlalchemy.engine.Engine:
        """Create the connection engine in SqlAlchemy to connect to BQ

        There are 2 methods of creating the BigQuery Engine,
        1. Using a service account JSON credential which will load into the credentials_info (Recommended for production use).
        2. Using ADC (Application Default Credentials) which allows a user to run `gcloud auth application-default login` to auth.
           This method would pass None to credentials_info and it will aggregate the credentials via,
           - Google's decision tree (https://google.aip.dev/auth/4110)

        Args:
            query_string: Additional query params to pass to engine construction.
            project_id: Project ID listed on the GCP platform.
            password: String representation of keyfile.json.
        """
        return sqlalchemy.create_engine(
            f"{SqlDialect.BIGQUERY.value}://{project_id}" + "/?" + query_string,
            credentials_info=json.loads(password) if password is not None else None,
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

    def cancel_submitted_queries(self) -> None:  # noqa: D
        raise NotImplementedError
