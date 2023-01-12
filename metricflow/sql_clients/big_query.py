from __future__ import annotations

import json
import logging
from typing import ClassVar, Optional, Dict, Callable
from typing import Sequence

import google.oauth2.service_account
import sqlalchemy
from google.cloud.bigquery import Client, QueryJob

from metricflow.protocols.sql_client import SqlEngine, SqlIsolationLevel
from metricflow.protocols.sql_client import (
    SqlEngineAttributes,
)
from metricflow.sql.render.big_query import BigQuerySqlQueryPlanRenderer
from metricflow.sql.render.sql_plan_renderer import SqlQueryPlanRenderer
from metricflow.sql.sql_bind_parameters import SqlBindParameters
from metricflow.sql_clients.async_request import SqlStatementCommentMetadata, CombinedSqlTags
from metricflow.sql_clients.common_client import SqlDialect
from metricflow.sql_clients.sqlalchemy_dialect import SqlAlchemySqlClient

logger = logging.getLogger(__name__)


class BigQueryEngineAttributes:
    """Engine-specific attributes for the BigQuery query engine

    This is an implementation of the SqlEngineAttributes protocol for BigQuery
    """

    sql_engine_type: ClassVar[SqlEngine] = SqlEngine.BIGQUERY

    # SQL Engine capabilities
    supported_isolation_levels: ClassVar[Sequence[SqlIsolationLevel]] = ()
    date_trunc_supported: ClassVar[bool] = True
    full_outer_joins_supported: ClassVar[bool] = True
    indexes_supported: ClassVar[bool] = False
    multi_threading_supported: ClassVar[bool] = True
    timestamp_type_supported: ClassVar[bool] = True
    timestamp_to_string_comparison_supported: ClassVar[bool] = False
    # Cancelling should be possible, but not yet implemented.
    cancel_submitted_queries_supported: ClassVar[bool] = True
    continuous_percentile_aggregation_supported: ClassVar[bool] = False
    discrete_percentile_aggregation_supported: ClassVar[bool] = False
    approximate_continuous_percentile_aggregation_supported: ClassVar[bool] = True
    approximate_discrete_percentile_aggregation_supported: ClassVar[bool] = False

    # SQL Dialect replacement strings
    double_data_type_name: ClassVar[str] = "FLOAT64"
    timestamp_type_name: ClassVar[Optional[str]] = "DATETIME"
    random_function_name: ClassVar[str] = "RAND"

    # MetricFlow attributes
    sql_query_plan_renderer: ClassVar[SqlQueryPlanRenderer] = BigQuerySqlQueryPlanRenderer()


class BigQuerySqlClient(SqlAlchemySqlClient):
    """BigQuery implementation of SQL client"""

    @staticmethod
    def from_connection_details(url: str, password: Optional[str] = None) -> BigQuerySqlClient:  # noqa: D
        parsed_url = sqlalchemy.engine.url.make_url(url)
        dialect = SqlDialect.BIGQUERY.value
        if parsed_url.drivername != dialect:
            raise ValueError(f"Expected dialect '{dialect}' in {url}")

        if password is None:
            raise ValueError(f"Credentials not supplied for {url}")

        return BigQuerySqlClient(password=password)

    def __init__(self, project_id: str = "", password: Optional[str] = None) -> None:
        """Creates a new BigQueryDBClient and tags it for tracing as big query."""
        # Without pool_pre_ping, it's possible for timed-out connections to be returned to the client and cause errors.
        # However, this can cause increase latency for slow engines.
        self._password = password
        password_json = json.loads(password) if password else {}
        self._project_id = project_id or password_json.get("project_id")
        bq_engine = BigQuerySqlClient._create_bq_engine(project_id=project_id, password=password)
        self._bq_client = Client(
            project=self._project_id,
            credentials=google.oauth2.service_account.Credentials.from_service_account_info(password_json)
            if password_json
            else None,
        )
        super().__init__(engine=bq_engine)

    def _engine_specific_dry_run_implementation(self, stmt: str, bind_params: SqlBindParameters) -> None:
        """Overrides base `_engine_specific_dry_run_implementation` function for BigQuery specifics"""
        _engine = self._create_bq_engine(
            query_field_values={"dry_run": "true"}, project_id=self._project_id, password=self._password
        )
        with _engine.connect() as conn:
            conn.execute(sqlalchemy.text(stmt), bind_params.param_dict)

    @staticmethod
    def _create_bq_engine(
        project_id: str = "", password: Optional[str] = None, query_field_values: Optional[Dict[str, str]] = None
    ) -> sqlalchemy.engine.Engine:
        """Create the connection engine in SqlAlchemy to connect to BQ

        There are 2 methods of creating the BigQuery Engine,
        1. Using a service account JSON credential which will load into the credentials_info (Recommended for production use).
        2. Using ADC (Application Default Credentials) which allows a user to run `gcloud auth application-default login` to auth.
           This method would pass None to credentials_info and it will aggregate the credentials via,
           - Google's decision tree (https://google.aip.dev/auth/4110)

        Args:
            query_field_values: The field/value pairs to use in the query string.
            project_id: Project ID listed on the GCP platform.
            password: String representation of keyfile.json.
        """
        if query_field_values is None:
            query_field_values = {}
        query_items = tuple(f"{field}={value}" for field, value in query_field_values.items())
        query_string = "&".join(query_items)

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

    def list_tables(self, schema_name: str) -> Sequence[str]:  # noqa: D
        with self._engine_connection(engine=self._engine) as conn:
            insp = sqlalchemy.inspection.inspect(conn)
            schema_dot_tables = insp.get_table_names(schema=schema_name)
            return [x.replace(schema_name + ".", "") for x in schema_dot_tables]

    def cancel_submitted_queries(self) -> None:  # noqa: D
        raise NotImplementedError

    def cancel_request(self, match_function: Callable[[CombinedSqlTags], bool]) -> int:  # noqa: D
        job: QueryJob
        canceled_job_ids = []
        # Couldn't find where these states were defined in the BQ libraries.
        for state in ["PENDING", "RUNNING"]:
            for job in self._bq_client.list_jobs(
                project=self._project_id,
                state_filter=state,
                # Considering putting a creation_time_min filter as well.
            ):
                parsed_tags = SqlStatementCommentMetadata.parse_tag_metadata_in_comments(job.query)

                # A job can move from the pending to the running state during iteration, so dedupe.
                if match_function(parsed_tags) and job.job_id not in canceled_job_ids:
                    logger.info(f"Canceling BQ job ID: {job.job_id}")
                    canceled_job_ids.append(job.job_id)
                    self._bq_client.cancel_job(job.job_id)
        return len(canceled_job_ids)
