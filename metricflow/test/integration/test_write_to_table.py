import textwrap

from metricflow.dataflow.sql_table import SqlTable
from metricflow.engine.metricflow_engine import MetricFlowQueryRequest
from metricflow.object_utils import random_id
from metricflow.test.compare_df import assert_dataframes_equal
from metricflow.test.integration.conftest import IntegrationTestHelpers


def test_write_to_table(it_helpers: IntegrationTestHelpers) -> None:  # noqa: D
    output_table = SqlTable(schema_name=it_helpers.mf_system_schema, table_name=f"test_table_{random_id()}")
    try:
        it_helpers.mf_engine.query(
            MetricFlowQueryRequest.create_with_random_request_id(
                metric_names=["bookings"], group_by_names=["ds"], output_table=output_table.sql
            )
        )
        expected = it_helpers.sql_client.query(
            textwrap.dedent(
                f"""\
                SELECT
                  SUM(1) AS bookings
                  , ds
                FROM {it_helpers.source_schema}.fct_bookings
                GROUP BY ds
                """
            )
        )
        actual = it_helpers.sql_client.query(
            textwrap.dedent(
                f"""\
                SELECT
                  bookings
                  , ds
                FROM {output_table.sql}
                """
            )
        )
        assert_dataframes_equal(actual=actual, expected=expected)
    finally:
        it_helpers.sql_client.drop_table(output_table)
