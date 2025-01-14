from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import Dict, Optional, Sequence

from dbt_semantic_interfaces.implementations.time_spine import PydanticTimeSpineCustomGranularityColumn
from dbt_semantic_interfaces.protocols import SemanticManifest
from dbt_semantic_interfaces.type_enums.time_granularity import TimeGranularity

from metricflow_semantics.collection_helpers.lru_cache import typed_lru_cache
from metricflow_semantics.specs.time_dimension_spec import DEFAULT_TIME_GRANULARITY, TimeDimensionSpec
from metricflow_semantics.sql.sql_table import SqlTable
from metricflow_semantics.time.granularity import ExpandedTimeGranularity

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class TimeSpineSource:
    """A calendar table. Should contain at least one column with dates/times that map to a standard granularity.

    Dates should be contiguous. May also contain custom granularity columns.
    """

    schema_name: Optional[str]
    table_name: str = "mf_time_spine"
    # Name of the column in the table that contains date/time values that map to a standard granularity.
    base_column: str = "ds"
    # The time granularity of the base column.
    base_granularity: TimeGranularity = DEFAULT_TIME_GRANULARITY
    db_name: Optional[str] = None
    custom_granularities: Sequence[PydanticTimeSpineCustomGranularityColumn] = ()

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
                custom_granularities=tuple(
                    [
                        PydanticTimeSpineCustomGranularityColumn(
                            name=custom_granularity.name, column_name=custom_granularity.column_name
                        )
                        for custom_granularity in time_spine.custom_granularities
                    ]
                ),
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

        return time_spine_sources

    @staticmethod
    @typed_lru_cache
    def build_custom_time_spine_sources(time_spine_sources: Sequence[TimeSpineSource]) -> Dict[str, TimeSpineSource]:
        """Creates a set of time spine sources with custom granularities based on what's in the manifest."""
        return {
            custom_granularity.name: time_spine_source
            for time_spine_source in time_spine_sources
            for custom_granularity in time_spine_source.custom_granularities
        }

    @staticmethod
    def build_custom_granularities(time_spine_sources: Sequence[TimeSpineSource]) -> Dict[str, ExpandedTimeGranularity]:
        """Creates a set of supported custom granularities based on what's in the manifest."""
        return {
            custom_granularity.name: ExpandedTimeGranularity(
                name=custom_granularity.name, base_granularity=time_spine_source.base_granularity
            )
            for time_spine_source in time_spine_sources
            for custom_granularity in time_spine_source.custom_granularities
        }

    @staticmethod
    def choose_time_spine_source(
        required_time_spine_specs: Sequence[TimeDimensionSpec],
        time_spine_sources: Dict[TimeGranularity, TimeSpineSource],
    ) -> TimeSpineSource:
        """Determine which time spine sources to use to satisfy the given specs.

        Custom grains can only use the time spine where they are defined. For standard grains, this will choose the time
        spine with the largest granularity that is compatible with all required standard grains. This ensures max efficiency
        for the query by minimizing the number of time spine joins and the amount of aggregation required. Example:
        - Time spines available: SECOND, MINUTE, DAY
        - Time granularities needed for request: HOUR, DAY
        --> Selected time spine: MINUTE
        """
        assert required_time_spine_specs, (
            "Choosing time spine source requires time spine specs, but the `required_time_spine_specs` param is empty. "
            "This indicates internal misconfiguration."
        )

        # Each custom grain can only be satisfied by one time spine.
        custom_time_spines = TimeSpineSource.build_custom_time_spine_sources(tuple(time_spine_sources.values()))
        required_time_spines = {
            custom_time_spines[spec.time_granularity.name]
            for spec in required_time_spine_specs
            if spec.time_granularity.is_custom_granularity
        }

        # Standard grains can be satisfied by any time spine with a base grain that's <= the standard grain.
        smallest_required_standard_grain = min(
            spec.time_granularity.base_granularity for spec in required_time_spine_specs
        )
        compatible_time_spines_for_standard_grains = {
            grain: time_spine_source
            for grain, time_spine_source in time_spine_sources.items()
            if grain.to_int() <= smallest_required_standard_grain.to_int()
        }
        if len(compatible_time_spines_for_standard_grains) == 0:
            raise RuntimeError(
                f"This query requires a time spine with granularity {smallest_required_standard_grain.name} or smaller, which is not configured. "
                f"The smallest available time spine granularity is {min(time_spine_sources).name}, which is too large."
                "See documentation for how to configure a new time spine: https://docs.getdbt.com/docs/build/metricflow-time-spine"
            )

        # If the standard grains can't be satisfied by the same time spines as the custom grains, add the largest compatible one.
        if not required_time_spines.intersection(set(compatible_time_spines_for_standard_grains.values())):
            required_time_spines.add(time_spine_sources[max(compatible_time_spines_for_standard_grains)])

        if len(required_time_spines) != 1:
            raise RuntimeError(
                "Multiple time spines are required to satisfy the specs, but only one is supported per query currently. "
                f"Multiple will be supported in the future. Time spines required: {required_time_spines}."
            )

        return required_time_spines.pop()

    @property
    def data_set_description(self) -> str:
        """Description to be displayed when this time spine is used in a data set."""
        return f"Read From Time Spine '{self.table_name}'"
