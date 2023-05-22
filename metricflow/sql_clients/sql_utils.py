from __future__ import annotations

import datetime
import pathlib
from typing import List, Tuple

import pandas as pd
from sqlalchemy.engine import make_url

from metricflow.configuration.constants import (
    CONFIG_DWH_ACCESS_TOKEN,
    CONFIG_DWH_CREDS_PATH,
    CONFIG_DWH_DB,
    CONFIG_DWH_DIALECT,
    CONFIG_DWH_HOST,
    CONFIG_DWH_HTTP_PATH,
    CONFIG_DWH_PASSWORD,
    CONFIG_DWH_PORT,
    CONFIG_DWH_PROJECT_ID,
    CONFIG_DWH_USER,
    CONFIG_DWH_WAREHOUSE,
)
from metricflow.configuration.yaml_handler import YamlFileHandler
from metricflow.filters.time_constraint import TimeRangeConstraint
from metricflow.plan_conversion.time_spine import TimeSpineSource
from metricflow.protocols.sql_client import SqlClient
from metricflow.sql_clients.big_query import BigQuerySqlClient
from metricflow.sql_clients.common_client import SqlDialect, not_empty
from metricflow.sql_clients.databricks import DatabricksSqlClient
from metricflow.sql_clients.duckdb import DuckDbSqlClient
from metricflow.sql_clients.postgres import PostgresSqlClient
from metricflow.sql_clients.redshift import RedshiftSqlClient
from metricflow.sql_clients.snowflake import SnowflakeSqlClient
from metricflow.time.time_granularity import TimeGranularity


def create_time_spine_table_if_necessary(time_spine_source: TimeSpineSource, sql_client: SqlClient) -> None:
    """Creates a time spine table for the given time spine source.

    Note this covers a broader-than-necessary time range to ensure test updates work as expected.
    """
    if sql_client.table_exists(time_spine_source.spine_table):
        return
    assert (
        time_spine_source.time_column_granularity is TimeGranularity.DAY
    ), f"A time granularity of {time_spine_source.time_column_granularity} is not yet supported."
    current_period = TimeRangeConstraint.ALL_TIME_BEGIN()
    # Using a union type throws a type error for some reason, so going with this approach
    time_spine_table_data: List[Tuple[datetime.datetime]] = []

    while current_period <= TimeRangeConstraint.ALL_TIME_END():
        time_spine_table_data.append((current_period,))
        current_period = current_period + datetime.timedelta(days=1)

    sql_client.drop_table(time_spine_source.spine_table)
    len(time_spine_table_data)

    sql_client.create_table_from_dataframe(
        sql_table=time_spine_source.spine_table,
        df=pd.DataFrame(
            columns=[time_spine_source.time_column_name],
            data=time_spine_table_data,
        ),
        chunk_size=1000,
    )


def dialect_from_url(url: str) -> SqlDialect:
    """Return the SQL dialect specified in the URL in the configuration."""
    dialect_protocol = make_url(url.split(";")[0]).drivername.split("+")
    if len(dialect_protocol) > 2:
        raise ValueError(f"Invalid # of +'s in {url}")
    return SqlDialect(dialect_protocol[0])


def make_sql_client(url: str, password: str) -> SqlClient:
    """Build SQL client based on env configs. Used only in tests."""
    dialect = dialect_from_url(url)

    if dialect == SqlDialect.REDSHIFT:
        return RedshiftSqlClient.from_connection_details(url, password)
    elif dialect == SqlDialect.SNOWFLAKE:
        return SnowflakeSqlClient.from_connection_details(url, password)
    elif dialect == SqlDialect.BIGQUERY:
        return BigQuerySqlClient.from_connection_details(url, password)
    elif dialect == SqlDialect.POSTGRESQL:
        return PostgresSqlClient.from_connection_details(url, password)
    elif dialect == SqlDialect.DUCKDB:
        return DuckDbSqlClient.from_connection_details(url, password)
    elif dialect == SqlDialect.DATABRICKS:
        return DatabricksSqlClient.from_connection_details(url, password)
    else:
        raise ValueError(f"Unknown dialect: `{dialect}` in URL {url}")


def make_sql_client_from_config(handler: YamlFileHandler) -> SqlClient:
    """Construct a SqlClient given a yaml file config."""
    url = handler.url
    dialect = not_empty(handler.get_value(CONFIG_DWH_DIALECT), CONFIG_DWH_DIALECT, url).lower()
    if dialect == SqlDialect.BIGQUERY.value:
        path_to_creds = handler.get_value(CONFIG_DWH_CREDS_PATH)
        project_id = handler.get_value(CONFIG_DWH_PROJECT_ID) or ""
        creds = None

        if not any([path_to_creds, project_id]):
            raise ValueError(f"One of `{CONFIG_DWH_CREDS_PATH}` or `{CONFIG_DWH_PROJECT_ID}` should be filled.")

        if path_to_creds:
            # Load json credential (Only for service accounts auth)
            if not pathlib.Path(path_to_creds).exists():
                raise ValueError(f"`{path_to_creds}` does not exist.")
            with open(path_to_creds, "r") as cred_file:
                creds = cred_file.read()
        return BigQuerySqlClient(project_id=project_id, password=creds)
    elif dialect == SqlDialect.SNOWFLAKE.value:
        host = not_empty(handler.get_value(CONFIG_DWH_HOST), CONFIG_DWH_HOST, url)
        user = not_empty(handler.get_value(CONFIG_DWH_USER), CONFIG_DWH_USER, url)
        password = not_empty(handler.get_value(CONFIG_DWH_PASSWORD), CONFIG_DWH_PASSWORD, url)
        database = not_empty(handler.get_value(CONFIG_DWH_DB), CONFIG_DWH_DB, url)
        warehouse = not_empty(handler.get_value(CONFIG_DWH_WAREHOUSE), CONFIG_DWH_WAREHOUSE, url)
        return SnowflakeSqlClient(
            host=host,
            username=user,
            password=password,
            database=database,
            url_query_params={"warehouse": warehouse},
            client_session_keep_alive=False,
        )
    elif dialect == SqlDialect.REDSHIFT.value:
        host = not_empty(handler.get_value(CONFIG_DWH_HOST), CONFIG_DWH_HOST, url)
        port = int(not_empty(handler.get_value(CONFIG_DWH_PORT), CONFIG_DWH_PORT, url))
        user = not_empty(handler.get_value(CONFIG_DWH_USER), CONFIG_DWH_USER, url)
        password = not_empty(handler.get_value(CONFIG_DWH_PASSWORD), CONFIG_DWH_PASSWORD, url)
        database = not_empty(handler.get_value(CONFIG_DWH_DB), CONFIG_DWH_DB, url)
        return RedshiftSqlClient(
            host=host,
            port=port,
            username=user,
            password=password,
            database=database,
        )
    elif dialect == SqlDialect.DUCKDB.value:
        database = not_empty(handler.get_value(CONFIG_DWH_DB), CONFIG_DWH_DB, url)
        return DuckDbSqlClient(file_path=database)
    elif dialect == SqlDialect.POSTGRESQL.value:
        host = not_empty(handler.get_value(CONFIG_DWH_HOST), CONFIG_DWH_HOST, url)
        port = int(not_empty(handler.get_value(CONFIG_DWH_PORT), CONFIG_DWH_PORT, url))
        user = not_empty(handler.get_value(CONFIG_DWH_USER), CONFIG_DWH_USER, url)
        password = not_empty(handler.get_value(CONFIG_DWH_PASSWORD), CONFIG_DWH_PASSWORD, url)
        database = not_empty(handler.get_value(CONFIG_DWH_DB), CONFIG_DWH_DB, url)
        return PostgresSqlClient(
            host=host,
            port=port,
            username=user,
            password=password,
            database=database,
        )
    elif dialect == SqlDialect.DATABRICKS.value:
        host = not_empty(handler.get_value(CONFIG_DWH_HOST), CONFIG_DWH_HOST, url)
        access_token = not_empty(handler.get_value(CONFIG_DWH_ACCESS_TOKEN), CONFIG_DWH_ACCESS_TOKEN, url)
        http_path = not_empty(handler.get_value(CONFIG_DWH_HTTP_PATH), CONFIG_DWH_HTTP_PATH, url)
        return DatabricksSqlClient(host=host, access_token=access_token, http_path=http_path)
    else:
        supported_dialects = [x.value for x in SqlDialect]
        raise ValueError(f"Invalid dialect '{dialect}', must be one of {supported_dialects} in {url}")
