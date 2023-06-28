from __future__ import annotations

from pandas import DataFrame

from metricflow.filters.time_constraint import TimeRangeConstraint
from metricflow.model.semantic_manifest_lookup import SemanticManifestLookup
from metricflow.protocols.sql_client import SqlClient
from metricflow.time.time_constants import ISO8601_PYTHON_TS_FORMAT


def test_date_spine_date_range(  # noqa: D
    sql_client: SqlClient,
    simple_semantic_manifest_lookup: SemanticManifestLookup,
    create_source_tables: None,
) -> None:
    time_spine_source = simple_semantic_manifest_lookup.time_spine_source
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
