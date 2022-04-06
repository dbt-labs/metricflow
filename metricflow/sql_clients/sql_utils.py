from typing import List, Any, Optional, Set

import ciso8601  # type: ignore
import pandas as pd
from sqlalchemy.engine import make_url

from metricflow.protocols.sql_client import SqlClient
from metricflow.sql_clients.big_query import BigQuerySqlClient
from metricflow.sql_clients.common_client import SqlDialect
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
                    new_row.append(ciso8601.parse_datetime(row[i]))

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
