import logging
import textwrap
from typing import ClassVar, Optional, Mapping, Union, Sequence, Callable

import sqlalchemy

from metricflow.protocols.sql_client import SqlEngine, SqlIsolationLevel
from metricflow.protocols.sql_client import SqlEngineAttributes
from metricflow.protocols.sql_request import SqlRequestTagSet
from metricflow.sql.render.redshift import RedshiftSqlQueryPlanRenderer
from metricflow.sql.render.sql_plan_renderer import SqlQueryPlanRenderer
from metricflow.sql_clients.async_request import SqlStatementCommentMetadata, CombinedSqlTags
from metricflow.sql_clients.common_client import SqlDialect, not_empty
from metricflow.sql_clients.sqlalchemy_dialect import SqlAlchemySqlClient

logger = logging.getLogger(__name__)


class RedshiftEngineAttributes:
    """Engine-specific attributes for the Redshift query engine

    This is an implementation of the SqlEngineAttributes protocol for Redshift
    """

    sql_engine_type: ClassVar[SqlEngine] = SqlEngine.REDSHIFT

    # SQL Engine capabilities
    supported_isolation_levels: ClassVar[Sequence[SqlIsolationLevel]] = (
        SqlIsolationLevel.READ_UNCOMMITTED,
        SqlIsolationLevel.READ_COMMITTED,
        SqlIsolationLevel.REPEATABLE_READ,
        SqlIsolationLevel.SERIALIZABLE,
    )
    date_trunc_supported: ClassVar[bool] = True
    full_outer_joins_supported: ClassVar[bool] = True
    indexes_supported: ClassVar[bool] = False
    multi_threading_supported: ClassVar[bool] = True
    timestamp_type_supported: ClassVar[bool] = True
    timestamp_to_string_comparison_supported: ClassVar[bool] = True
    cancel_submitted_queries_supported: ClassVar[bool] = True
    continuous_percentile_aggregation_supported: ClassVar[bool] = True
    discrete_percentile_aggregation_supported: ClassVar[bool] = False
    approximate_continuous_percentile_aggregation_supported: ClassVar[bool] = False
    approximate_discrete_percentile_aggregation_supported: ClassVar[bool] = True

    # SQL Dialect replacement strings
    double_data_type_name: ClassVar[str] = "DOUBLE PRECISION"
    timestamp_type_name: ClassVar[Optional[str]] = "TIMESTAMP"
    random_function_name: ClassVar[str] = "RANDOM"

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

    def cancel_submitted_queries(self) -> None:  # noqa: D
        for request_id in self.active_requests():
            self.cancel_request(SqlRequestTagSet.create_from_request_id(request_id))

    def cancel_request(self, match_function: Callable[[CombinedSqlTags], bool]) -> int:  # noqa: D
        result = self.query(
            textwrap.dedent(
                """\
                SELECT pid AS query_id, query AS query_text
                FROM stv_recents
                WHERE status='Running'
                """
            )
        )

        num_cancelled_queries = 0

        for query_id, query_text in result.values:
            parsed_tags = SqlStatementCommentMetadata.parse_tag_metadata_in_comments(query_text)

            # Check for a match where the query's tag
            if match_function(parsed_tags):
                logger.info(f"Cancelling query ID: {query_id}")
                self.execute(f"CANCEL {query_id}")
                num_cancelled_queries += 1

        return num_cancelled_queries
