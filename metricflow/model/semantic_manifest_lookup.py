from __future__ import annotations

import logging

from dbt_semantic_interfaces.pretty_print import pformat_big_objects
from dbt_semantic_interfaces.protocols.semantic_manifest import SemanticManifest
from dbt_semantic_interfaces.type_enums import TimeGranularity

from metricflow.dataflow.sql_table import SqlTable
from metricflow.model.semantics.metric_lookup import MetricLookup
from metricflow.model.semantics.semantic_model_lookup import SemanticModelLookup
from metricflow.plan_conversion.time_spine import TimeSpineSource
from metricflow.protocols.semantics import SemanticModelAccessor

logger = logging.getLogger(__name__)


class SemanticManifestLookup:
    """Adds semantics information to the user configured model."""

    def __init__(self, semantic_manifest: SemanticManifest) -> None:  # noqa: D
        self._semantic_manifest = semantic_manifest
        self._semantic_model_lookup = SemanticModelLookup(semantic_manifest)
        self._metric_lookup = MetricLookup(self._semantic_manifest, self._semantic_model_lookup)

    @property
    def semantic_manifest(self) -> SemanticManifest:  # noqa: D
        return self._semantic_manifest

    @property
    def semantic_model_lookup(self) -> SemanticModelAccessor:  # noqa: D
        return self._semantic_model_lookup

    @property
    def metric_lookup(self) -> MetricLookup:  # noqa: D
        return self._metric_lookup

    @property
    def time_spine_source(self) -> TimeSpineSource:  # noqa: D
        time_spine_table_configurations = self._semantic_manifest.project_configuration.time_spine_table_configurations

        if not (
            len(time_spine_table_configurations) == 1
            and time_spine_table_configurations[0].grain == TimeGranularity.DAY
        ):
            raise NotImplementedError(
                f"Only a single time spine table configuration with {TimeGranularity.DAY} is currently "
                f"supported. Got:\n"
                f"{pformat_big_objects(time_spine_table_configurations)}"
            )

        time_spine_table_configuration = time_spine_table_configurations[0]
        time_spine_table = SqlTable.from_string(time_spine_table_configuration.location)
        return TimeSpineSource(
            schema_name=time_spine_table.schema_name,
            table_name=time_spine_table.table_name,
            time_column_name=time_spine_table_configuration.column_name,
            time_column_granularity=time_spine_table_configuration.grain,
        )
