import logging
import textwrap
from typing import ClassVar, Optional, List, Set

import jinja2
import sqlalchemy

from metricflow.dataflow.sql_table import SqlTable
from metricflow.protocols.sql_client import SupportedSqlEngine, SqlEngineAttributes
from metricflow.sql.render.sql_plan_renderer import SqlQueryPlanRenderer
from metricflow.sql.render.sqlite import SqliteSqlQueryPlanRenderer
from metricflow.sql_clients.common_client import SqlDialect
from metricflow.sql_clients.sqlalchemy_dialect import SqlAlchemySqlClient


class SQLiteEngineAttributes:
    """Engine-specific attributes for the SQLite query engine

    This is an implementation of the SqlEngineAttributes protocol for SQLite
    """

    sql_engine_type: ClassVar[SupportedSqlEngine] = SupportedSqlEngine.SQLITE

    # SQL Engine capabilities
    date_trunc_supported: ClassVar[bool] = False
    full_outer_joins_supported: ClassVar[bool] = False
    indexes_supported: ClassVar[bool] = True
    multi_threading_supported: ClassVar[bool] = False
    timestamp_type_supported: ClassVar[bool] = False
    timestamp_to_string_comparison_supported: ClassVar[bool] = False

    # SQL Dialect replacement strings
    double_data_type_name: ClassVar[str] = "DOUBLE"
    timestamp_type_name: ClassVar[Optional[str]] = None

    # MetricFlow attributes
    sql_query_plan_renderer: ClassVar[SqlQueryPlanRenderer] = SqliteSqlQueryPlanRenderer()


# TODO: add implementation of SQLite-compatible SqlClient
logger = logging.getLogger(__name__)


class SqliteSqlClient(SqlAlchemySqlClient):
    """Generally useful for testing ... not recommended for persistent use cases"""

    # Name of the schema created automatically.
    DEFAULT_DB = "main"

    @staticmethod
    def from_connection_details(url: str, password: Optional[str] = None) -> SqlAlchemySqlClient:  # noqa: D
        expected_url = "sqlite://"
        if url != expected_url:
            raise ValueError(f"URL was '{url}' but should be '{expected_url}'")
        if password:
            raise ValueError("Password should be empty")

        return SqliteSqlClient()

    def __init__(self) -> None:  # noqa: D
        super().__init__(sqlalchemy.create_engine(f"{SqlDialect.SQLITE.value}:///:memory:", pool_pre_ping=True))
        self._created_schemas: Set[str] = {self.DEFAULT_DB}

    @property
    def sql_engine_attributes(self) -> SqlEngineAttributes:
        """Collection of attributes and features specific to the SQLite SQL engine"""
        return SQLiteEngineAttributes()

    def create_schema(self, schema_name: str) -> None:  # noqa: D
        if schema_name not in self._created_schemas:
            self.execute(
                stmt=f"ATTACH DATABASE '/tmp/{schema_name}.db' AS {schema_name}",
            )
            self._created_schemas.add(schema_name)
        else:
            logger.info(f"create_schema() called with schema that exists '{schema_name}', so not actually creating it.")

    def list_tables(self, schema_name: str) -> List[str]:  # noqa:D
        return self.query(
            f"""\
            SELECT
              name
            FROM {schema_name}.sqlite_master
            WHERE type = 'table'
            """,
        )["name"].tolist()

    def table_exists(self, sql_table: SqlTable) -> bool:  # noqa :D
        return self.query(
            jinja2.Template(
                textwrap.dedent(
                    """\
                    SELECT EXISTS (
                      SELECT name FROM {{ schema_name }}.sqlite_master
                      WHERE type='table' AND name = '{{ table_name }}'
                    )
                    """,
                ),
                undefined=jinja2.StrictUndefined,
            ).render(
                schema_name=sql_table.schema_name,
                table_name=sql_table.table_name,
            )
        ).squeeze()

    def drop_schema(self, schema_name: str, cascade: bool = True) -> None:  # noqa: D
        logger.info(f"Not dropping schema '{schema_name}' since this SQLite DB is ephemeral")
