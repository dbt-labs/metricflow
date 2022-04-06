from _pytest.fixtures import FixtureRequest

from metricflow.dataflow.sql_table import SqlTable
from metricflow.engine.metricflow_engine import MetricFlowQueryRequest
from metricflow.test.fixtures.setup_fixtures import MetricFlowTestSessionState
from metricflow.test.integration.conftest import IntegrationTestHelpers
from metricflow.test.plan_utils import make_schema_replacement_function, assert_snapshot_text_equal

# Needed as the table alias can vary from run to run.
_EXCLUDE_TABLE_ALIAS_REGEX = "^.*_src.*$"


def test_render_query(  # noqa: D
    request: FixtureRequest, mf_test_session_state: MetricFlowTestSessionState, it_helpers: IntegrationTestHelpers
) -> None:
    result = it_helpers.mf_engine.explain(
        MetricFlowQueryRequest.create_with_random_request_id(
            metric_names=["bookings"],
            group_by_names=["ds"],
        )
    )

    assert_snapshot_text_equal(
        request=request,
        mf_test_session_state=mf_test_session_state,
        group_id=result.__class__.__name__,
        snapshot_id="query0",
        snapshot_text=result.rendered_sql.sql_query,
        snapshot_file_extension=".sql",
        incomparable_strings_replacement_function=make_schema_replacement_function(
            system_schema=it_helpers.mf_system_schema, source_schema=it_helpers.source_schema
        ),
        exclude_line_regex=_EXCLUDE_TABLE_ALIAS_REGEX,
        additional_sub_directories_for_snapshots=(it_helpers.sql_client.__class__.__name__,),
    )


def test_render_write_to_table_query(  # noqa: D
    request: FixtureRequest, mf_test_session_state: MetricFlowTestSessionState, it_helpers: IntegrationTestHelpers
) -> None:
    output_table = SqlTable(schema_name=it_helpers.mf_system_schema, table_name="test_table")

    result = it_helpers.mf_engine.explain(
        MetricFlowQueryRequest.create_with_random_request_id(
            metric_names=["bookings"], group_by_names=["ds"], output_table=output_table.sql
        )
    )

    assert_snapshot_text_equal(
        request=request,
        mf_test_session_state=mf_test_session_state,
        group_id=result.__class__.__name__,
        snapshot_id="query0",
        snapshot_text=result.rendered_sql.sql_query,
        snapshot_file_extension=".sql",
        incomparable_strings_replacement_function=make_schema_replacement_function(
            system_schema=it_helpers.mf_system_schema, source_schema=it_helpers.source_schema
        ),
        exclude_line_regex=_EXCLUDE_TABLE_ALIAS_REGEX,
        additional_sub_directories_for_snapshots=(it_helpers.sql_client.__class__.__name__,),
    )
