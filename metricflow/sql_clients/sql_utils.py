import dateutil.parser
import pandas as pd
import pathlib

from sqlalchemy.engine import make_url
from typing import List, Any, Optional, Set

from metricflow.configuration.constants import (
    CONFIG_DWH_DB,
    CONFIG_DWH_DIALECT,
    CONFIG_DWH_HOST,
    CONFIG_DWH_PASSWORD,
    CONFIG_DWH_PORT,
    CONFIG_DWH_USER,
    CONFIG_DWH_WAREHOUSE,
)
from metricflow.configuration.yaml_handler import YamlFileHandler
from metricflow.protocols.sql_client import SqlClient, SupportedSqlEngine
from metricflow.sql_clients.big_query import BigQuerySqlClient
from metricflow.sql_clients.common_client import not_empty, SqlDialect
from metricflow.sql_clients.redshift import RedshiftSqlClient
from metricflow.sql_clients.snowflake import SnowflakeSqlClient
from metricflow.sql_clients.sqlite import SqliteSqlClient


def make_df(  # type: ignore [misc]
    sql_client: SqlClient, columns: List[str], data: Any, time_columns: Optional[Set[str]] = None
) -> pd.DataFrame:
    """Helper to make a dataframe, converting the time columns to appropriate types."""
    time_columns = time_columns or set()
    # Should only be used in testing, so sql_client should be set.
    assert sql_client

    if sql_client.sql_engine_attributes.timestamp_type_supported:
        new_rows = []
        for row in data:
            new_row = []
            # Change the type of the column if it's in time_columns
            for i, column in enumerate(columns):
                if column in time_columns:
                    # ts_suffix = " 00:00:00" if ":" not in row[i] else ""
                    # ts_input = row[i] + ts_suffix
                    new_row.append(dateutil.parser.parse(row[i]))

                else:
                    new_row.append(row[i])
            new_rows.append(new_row)
        data = new_rows

    return pd.DataFrame(
        columns=columns,
        data=data,
    )


def make_sql_client(url: str, password: str) -> SqlClient:  # noqa: D
    dialect_protocol = make_url(url).drivername.split("+")
    dialect = SqlDialect(dialect_protocol[0])
    if len(dialect_protocol) > 2:
        raise ValueError(f"Invalid # of +'s in {url}")

    if dialect == SqlDialect.REDSHIFT:
        return RedshiftSqlClient.from_connection_details(url, password)
    elif dialect == SqlDialect.SNOWFLAKE:
        return SnowflakeSqlClient.from_connection_details(url, password)
    elif dialect == SqlDialect.BIGQUERY:
        return BigQuerySqlClient.from_connection_details(url, password)
    elif dialect == SqlDialect.SQLITE:
        return SqliteSqlClient.from_connection_details(url, password)
    else:
        raise ValueError(f"Unknown dialect: `{dialect}` in URL {url}")


def make_sql_client_from_config(handler: YamlFileHandler) -> SqlClient:
    """Construct a SqlClient given a yaml file config."""

    url = handler.url
    dialect = not_empty(handler.get_value(CONFIG_DWH_DIALECT), CONFIG_DWH_DIALECT, url).upper()
    if dialect == SupportedSqlEngine.BIGQUERY.name:
        path_to_creds = not_empty(handler.get_value(CONFIG_DWH_PASSWORD), CONFIG_DWH_PASSWORD, url)
        if not pathlib.Path(path_to_creds).exists:
            raise ValueError(f"`{path_to_creds}` does not contain the BigQuery credential file.")
        with open(path_to_creds, "r") as cred_file:
            creds = cred_file.read()
        return BigQuerySqlClient(password=creds)
    elif dialect == SupportedSqlEngine.SNOWFLAKE.name:
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
    elif dialect == SupportedSqlEngine.REDSHIFT.name:
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
    else:
        raise ValueError(f"Invalid dialect `{dialect}`, must be one of `bigquery`, `snowflake`, `redshift` in {url}")
