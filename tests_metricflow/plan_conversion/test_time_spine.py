from __future__ import annotations

import textwrap

from dbt_semantic_interfaces.type_enums import TimeGranularity
from metricflow_semantics.filters.time_constraint import TimeRangeConstraint
from metricflow_semantics.model.semantic_manifest_lookup import SemanticManifestLookup
from metricflow_semantics.time.time_spine_source import TimeSpineSource

from metricflow.protocols.sql_client import SqlClient


def test_date_spine_date_range(  # noqa: D103
    sql_client: SqlClient,
    simple_semantic_manifest_lookup: SemanticManifestLookup,
    create_source_tables: None,
) -> None:
    time_spine_source = TimeSpineSource.build_standard_time_spine_sources(
        simple_semantic_manifest_lookup.semantic_manifest
    )[TimeGranularity.DAY]
    range_df = sql_client.query(
        textwrap.dedent(
            f"""\
            SELECT
                MIN({time_spine_source.base_column})
                , MAX({time_spine_source.base_column})
            FROM {time_spine_source.sql_table.sql}
            """,
        )
    )

    assert range_df.row_count == 1
    assert range_df.column_count == 2
    assert range_df.rows[0] == (TimeRangeConstraint.ALL_TIME_BEGIN(), TimeRangeConstraint.ALL_TIME_END())
