from pandas import DataFrame

from metricflow.constraints.time_constraint import TimeRangeConstraint
from metricflow.plan_conversion.time_spine import TimeSpineSource
from metricflow.protocols.sql_client import SqlClient
from metricflow.time.time_constants import ISO8601_PYTHON_FORMAT, ISO8601_PYTHON_TS_FORMAT


def test_date_spine_date_range(sql_client: SqlClient, time_spine_source: TimeSpineSource) -> None:  # noqa: D
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

    if sql_client.sql_engine_attributes.timestamp_type_supported:
        assert tuple(map(lambda x: x.strftime(ISO8601_PYTHON_TS_FORMAT), date_range)) == (
            TimeRangeConstraint.ALL_TIME_BEGIN().strftime(ISO8601_PYTHON_TS_FORMAT),
            TimeRangeConstraint.ALL_TIME_END().strftime(ISO8601_PYTHON_TS_FORMAT),
        )

    else:
        assert date_range == (
            TimeRangeConstraint.ALL_TIME_BEGIN().strftime(ISO8601_PYTHON_FORMAT),
            TimeRangeConstraint.ALL_TIME_END().strftime(ISO8601_PYTHON_FORMAT),
        )
