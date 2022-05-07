from __future__ import annotations

import datetime
import logging
import threading
from dataclasses import dataclass
from typing import List, Tuple

import pandas as pd

from metricflow.protocols.sql_client import SqlClient
from metricflow.constraints.time_constraint import TimeRangeConstraint
from metricflow.dataflow.sql_table import SqlTable
from metricflow.time.time_constants import ISO8601_PYTHON_FORMAT
from metricflow.time.time_granularity import TimeGranularity

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class TimeSpineSource:
    """Defines a source table containing all timestamps to use for computing cumulative metrics."""

    schema_name: str
    table_name: str = "mf_time_spine"
    # Name of the column in the table that contains the dates.
    time_column_name: str = "ds"
    # The time granularity of the dates in the spine table.
    time_column_granularity: TimeGranularity = TimeGranularity.DAY

    @property
    def spine_table(self) -> SqlTable:
        """Table containing all dates"""
        return SqlTable(schema_name=self.schema_name, table_name=self.table_name)


class TimeSpineTableBuilder:
    """Helps to build the time spine table based on the definition in a TimeSpineSource."""

    def __init__(  # noqa: D
        self,
        time_spine_source: TimeSpineSource,
        sql_client: SqlClient,
    ) -> None:
        self._time_spine_source = time_spine_source
        self._sql_client = sql_client
        self._verified_spine_table_exists = False
        self._create_table_lock = threading.Lock()

    @property
    def time_spine_source(self) -> TimeSpineSource:  # noqa: D
        return self._time_spine_source

    def create_if_necessary(self) -> None:  # noqa: D
        """Creates the spine table if it doesn't already exist."""
        logger.info("Waiting to get the lock for the time spine table")
        with self._create_table_lock:
            spine_table = self.time_spine_source.spine_table
            logger.info("Got the lock for the time spine table")
            if self._verified_spine_table_exists:
                logger.info(f"Previously verified that the spine table {spine_table.sql} exists.")
                return
            logger.info(f"Checking if the spine table {spine_table.sql} exists")
            if self._sql_client.table_exists(spine_table):
                logger.info(f"Spine table {spine_table.sql} exists")
                self._verified_spine_table_exists = True
                return
            logger.info(f"Spine table {spine_table.sql} does not exist")
            start_date = TimeRangeConstraint.ALL_TIME_BEGIN()
            end_date = TimeRangeConstraint.ALL_TIME_END()

            current_date = start_date
            # Using a union type throws a type error for some reason, so going with this approach
            date_spine_table_datetime_data: List[Tuple[datetime.datetime]] = []
            date_spine_table_str_data: List[Tuple[str]] = []

            if self.time_spine_source.time_column_granularity != TimeGranularity.DAY:
                raise RuntimeError(
                    f"A time spine source with a granularity {self.time_spine_source.time_column_granularity} is not "
                    f"yet supported."
                )

            if self._sql_client.sql_engine_attributes.timestamp_type_supported:
                while current_date <= end_date:
                    date_spine_table_datetime_data.append((current_date,))
                    current_date = current_date + datetime.timedelta(days=1)
            else:
                while current_date <= end_date:
                    date_spine_table_str_data.append((current_date.strftime(ISO8601_PYTHON_FORMAT),))
                    current_date = current_date + datetime.timedelta(days=1)

            self._sql_client.drop_table(spine_table)
            num_rows = (
                len(date_spine_table_datetime_data)
                if date_spine_table_datetime_data
                else len(date_spine_table_str_data)
            )

            logger.info(f"Creating date spine table {spine_table.sql} with {num_rows} rows")
            self._sql_client.create_table_from_dataframe(
                sql_table=spine_table,
                df=pd.DataFrame(
                    columns=[self._time_spine_source.time_column_name],
                    data=date_spine_table_datetime_data or date_spine_table_str_data,
                ),
                chunk_size=1000,
            )
            logger.info(f"Created date spine table {spine_table.sql}")
            self._verified_spine_table_exists = True
