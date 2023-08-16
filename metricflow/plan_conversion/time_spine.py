from __future__ import annotations

import logging
from dataclasses import dataclass

from dbt_semantic_interfaces.type_enums.time_granularity import TimeGranularity

from metricflow.dataflow.sql_table import SqlTable

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
        """Table containing all dates."""
        return SqlTable(schema_name=self.schema_name, table_name=self.table_name)
