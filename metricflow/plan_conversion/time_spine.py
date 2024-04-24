from __future__ import annotations

import logging
from dataclasses import dataclass

from dbt_semantic_interfaces.protocols import SemanticManifest
from dbt_semantic_interfaces.type_enums.time_granularity import TimeGranularity
from metricflow_semantics.mf_logging.pretty_print import mf_pformat

from metricflow.sql.sql_table import SqlTable

logger = logging.getLogger(__name__)

TIME_SPINE_DATA_SET_DESCRIPTION = "Time Spine"


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

    @staticmethod
    def create_from_manifest(semantic_manifest: SemanticManifest) -> TimeSpineSource:
        """Creates a time spine source based on what's in the manifest."""
        time_spine_table_configurations = semantic_manifest.project_configuration.time_spine_table_configurations

        if not (
            len(time_spine_table_configurations) == 1
            and time_spine_table_configurations[0].grain == TimeGranularity.DAY
        ):
            raise NotImplementedError(
                f"Only a single time spine table configuration with {TimeGranularity.DAY} is currently "
                f"supported. Got:\n"
                f"{mf_pformat(time_spine_table_configurations)}"
            )

        time_spine_table_configuration = time_spine_table_configurations[0]
        time_spine_table = SqlTable.from_string(time_spine_table_configuration.location)
        return TimeSpineSource(
            schema_name=time_spine_table.schema_name,
            table_name=time_spine_table.table_name,
            time_column_name=time_spine_table_configuration.column_name,
            time_column_granularity=time_spine_table_configuration.grain,
        )
