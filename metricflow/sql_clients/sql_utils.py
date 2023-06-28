from __future__ import annotations

import datetime
from typing import List, Tuple

import pandas as pd

from metricflow.filters.time_constraint import TimeRangeConstraint
from metricflow.plan_conversion.time_spine import TimeSpineSource
from metricflow.protocols.sql_client import SqlClient
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
