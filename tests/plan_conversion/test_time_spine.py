from __future__ import annotations

from metricflow_semantics.filters.time_constraint import TimeRangeConstraint
from metricflow_semantics.model.semantic_manifest_lookup import SemanticManifestLookup
from metricflow_semantics.time.time_constants import ISO8601_PYTHON_TS_FORMAT
from pandas import DataFrame

from metricflow.plan_conversion.time_spine import TimeSpineSource
from metricflow.protocols.sql_client import SqlClient


def test_date_spine_date_range(  # noqa: D103
    sql_client: SqlClient,
    simple_semantic_manifest_lookup: SemanticManifestLookup,
    create_source_tables: None,
) -> None:
    time_spine_source = TimeSpineSource.create_from_manifest(simple_semantic_manifest_lookup.semantic_manifest)
    range_df: DataFrame = sql_client.query(
        f"""\
        SELECT
            MIN({time_spine_source.time_column_name})
            , MAX({time_spine_source.time_column_name})
        FROM {time_spine_source.spine_table.sql}
        """,
    )
    assert range_df.shape == (1, 2), f"Expected 1 row with 2 columns in range dataframe, got {range_df}"
    date_range = tuple(range_df.squeeze())

    assert tuple(map(lambda x: x.strftime(ISO8601_PYTHON_TS_FORMAT), date_range)) == (
        TimeRangeConstraint.ALL_TIME_BEGIN().strftime(ISO8601_PYTHON_TS_FORMAT),
        TimeRangeConstraint.ALL_TIME_END().strftime(ISO8601_PYTHON_TS_FORMAT),
    )
