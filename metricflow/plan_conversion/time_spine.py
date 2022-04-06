from __future__ import annotations

import datetime
import logging
import threading
from typing import List, Tuple

import pandas as pd

from metricflow.protocols.sql_client import SqlClient
from metricflow.constraints.time_constraint import TimeRangeConstraint
from metricflow.dataflow.sql_table import SqlTable
from metricflow.time.time_constants import ISO8601_PYTHON_FORMAT
from metricflow.time.time_granularity import TimeGranularity

logger = logging.getLogger(__name__)


class TimeSpineSource:
    """Generates a source table containing all timestamps to use for cumulative metrics."""

    _SPINE_TABLE_NAME = "mf_time_spine"

    def __init__(self, sql_client: SqlClient, schema_name: str) -> None:  # noqa
        self._sql_client = sql_client
        self._schema_name = schema_name
        self._verified_spine_table_exists = False
        self._create_table_lock = threading.Lock()

    @property
    def spine_table(self) -> SqlTable:
        """Table containing all dates"""
        return SqlTable(schema_name=self._schema_name, table_name=TimeSpineSource._SPINE_TABLE_NAME)

    @property
    def time_column_name(self) -> str:
        """Name of the column"""
        return "ds"

    @property
    def time_column_granularity(self) -> TimeGranularity:  # noqa: D
        return TimeGranularity.DAY

    def create_if_necessary(self) -> None:
        """Creates the spine table if necessary."""
        logger.info("Waiting to get the lock for the time spine table")
        with self._create_table_lock:
            logger.info("Got the lock for the time spine table")
            if self._verified_spine_table_exists:
                return
            logger.info(f"Checking if the spine table {self.spine_table.sql} exists")
            if self._sql_client.table_exists(self.spine_table):
                logger.info(f"Spine table {self.spine_table.sql} exists")
                self._verified_spine_table_exists = True
                return
            logger.info(f"Spine table {self.spine_table.sql} does not exist")
            start_date = TimeRangeConstraint.ALL_TIME_BEGIN()
            end_date = TimeRangeConstraint.ALL_TIME_END()

            current_date = start_date
            # Using a union type throws a type error for some reason, so going with this approach
            date_spine_table_datetime_data: List[Tuple[datetime.datetime]] = []
            date_spine_table_str_data: List[Tuple[str]] = []

            if self._sql_client.sql_engine_attributes.timestamp_type_supported:
                while current_date <= end_date:
                    date_spine_table_datetime_data.append((current_date,))
                    current_date = current_date + datetime.timedelta(days=1)
            else:
                while current_date <= end_date:
                    date_spine_table_str_data.append((current_date.strftime(ISO8601_PYTHON_FORMAT),))
                    current_date = current_date + datetime.timedelta(days=1)

            self._sql_client.drop_table(self.spine_table)
            num_rows = (
                len(date_spine_table_datetime_data)
                if date_spine_table_datetime_data
                else len(date_spine_table_str_data)
            )

            logger.info(f"Creating date spine table {self.spine_table.sql} with {num_rows} rows")
            self._sql_client.create_table_from_dataframe(
                sql_table=self.spine_table,
                df=pd.DataFrame(
                    columns=[self.time_column_name],
                    data=date_spine_table_datetime_data or date_spine_table_str_data,
                ),
                chunk_size=1000,
            )
            logger.info(f"Created date spine table {self.spine_table.sql}")
            self._verified_spine_table_exists = True
