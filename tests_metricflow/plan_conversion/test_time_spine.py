from __future__ import annotations

import textwrap

from metricflow_semantics.filters.time_constraint import TimeRangeConstraint
from metricflow_semantics.model.semantic_manifest_lookup import SemanticManifestLookup

from metricflow.plan_conversion.time_spine import TimeSpineSource
from metricflow.protocols.sql_client import SqlClient


def test_date_spine_date_range(  # noqa: D103
    sql_client: SqlClient,
    simple_semantic_manifest_lookup: SemanticManifestLookup,
    create_source_tables: None,
) -> None:
    time_spine_source = TimeSpineSource.create_from_manifest(simple_semantic_manifest_lookup.semantic_manifest)
    range_df = sql_client.query(
        textwrap.dedent(
            f"""\
            SELECT
                MIN({time_spine_source.time_column_name})
                , MAX({time_spine_source.time_column_name})
            FROM {time_spine_source.spine_table.sql}
            """,
        )
    )

    assert range_df.row_count == 1
    assert range_df.column_count == 2
    assert range_df.rows[0] == (TimeRangeConstraint.ALL_TIME_BEGIN(), TimeRangeConstraint.ALL_TIME_END())
