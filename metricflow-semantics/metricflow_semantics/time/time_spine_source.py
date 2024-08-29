from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import Dict, Optional, Sequence

from dbt_semantic_interfaces.protocols import SemanticManifest
from dbt_semantic_interfaces.type_enums.time_granularity import TimeGranularity

from metricflow_semantics.specs.time_dimension_spec import DEFAULT_TIME_GRANULARITY
from metricflow_semantics.sql.sql_table import SqlTable

logger = logging.getLogger(__name__)

TIME_SPINE_DATA_SET_DESCRIPTION = "Time Spine"


@dataclass(frozen=True)
class TimeSpineSource:
    """A calendar table. Should contain at least one column with dates/times that map to a standard granularity.

    Dates should be contiguous. May also contain custom granularity columns.
    """

    schema_name: str
    table_name: str = "mf_time_spine"
    # Name of the column in the table that contains date/time values that map to a standard granularity.
    base_column: str = "ds"
    # The time granularity of the base column.
    base_granularity: TimeGranularity = DEFAULT_TIME_GRANULARITY
    db_name: Optional[str] = None
    custom_granularity_columns: Sequence[str] = ()

    @property
    def spine_table(self) -> SqlTable:
        """Table containing all dates."""
        return SqlTable(schema_name=self.schema_name, table_name=self.table_name, db_name=self.db_name)

    @staticmethod
    def build_standard_time_spine_sources(
        semantic_manifest: SemanticManifest,
    ) -> Dict[TimeGranularity, TimeSpineSource]:
        """Creates a time spine source based on what's in the manifest."""
        time_spine_sources = {
            time_spine.primary_column.time_granularity: TimeSpineSource(
                schema_name=time_spine.node_relation.schema_name,
                table_name=time_spine.node_relation.alias,
                db_name=time_spine.node_relation.database,
                base_column=time_spine.primary_column.name,
                base_granularity=time_spine.primary_column.time_granularity,
                custom_granularity_columns=[column.name for column in time_spine.custom_granularities],
            )
            for time_spine in semantic_manifest.project_configuration.time_spines
        }

        # For backward compatibility: if legacy time spine config exists in the manifest, add that time spine here for
        # backward compatibility. Ignore it if there is a new time spine config with the same granularity.
        legacy_time_spines = semantic_manifest.project_configuration.time_spine_table_configurations
        for legacy_time_spine in legacy_time_spines:
            if legacy_time_spine.grain not in time_spine_sources:
                time_spine_table = SqlTable.from_string(legacy_time_spine.location)
                time_spine_sources[legacy_time_spine.grain] = TimeSpineSource(
                    schema_name=time_spine_table.schema_name,
                    table_name=time_spine_table.table_name,
                    db_name=time_spine_table.db_name,
                    base_column=legacy_time_spine.column_name,
                    base_granularity=legacy_time_spine.grain,
                )

        # Sanity check: this should have been validated during manifest parsing.
        if not time_spine_sources:
            raise RuntimeError(
                "At least one time spine must be configured to use the semantic layer, but none were found."
            )

        return time_spine_sources
