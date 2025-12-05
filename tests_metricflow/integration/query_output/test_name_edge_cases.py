from __future__ import annotations

import pytest
from _pytest.fixtures import FixtureRequest
from metricflow_semantics.test_helpers.config_helpers import MetricFlowTestConfiguration

from metricflow.engine.metricflow_engine import MetricFlowQueryRequest
from metricflow.protocols.sql_client import SqlClient
from tests_metricflow.integration.conftest import IntegrationTestHelpers
from tests_metricflow.snapshot_utils import assert_str_snapshot_equal


@pytest.mark.sql_engine_snapshot
def test_metric_name_same_as_dimension_name(  # noqa: D103
    request: FixtureRequest,
    mf_test_configuration: MetricFlowTestConfiguration,
    sql_client: SqlClient,
    name_edge_case_it_helpers: IntegrationTestHelpers,
) -> None:
    """Check a soon-to-be-deprecated use case where a manifest contains a metric with the same name as a dimension."""
    query_result = name_edge_case_it_helpers.mf_engine.query(
        MetricFlowQueryRequest.create_with_random_request_id(
            metric_names=["homonymous_metric_and_dimension"],
            group_by_names=["booking__homonymous_metric_and_dimension"],
            order_by_names=["booking__homonymous_metric_and_dimension"],
        )
    )
    assert query_result.result_df is not None, "Unexpected empty result."

    assert_str_snapshot_equal(
        request=request,
        mf_test_configuration=mf_test_configuration,
        snapshot_id="query_output",
        snapshot_str=query_result.result_df.text_format(),
        sql_engine=sql_client.sql_engine_type,
    )


@pytest.mark.sql_engine_snapshot
def test_homonymous_metric_and_entity(  # noqa: D103
    request: FixtureRequest,
    mf_test_configuration: MetricFlowTestConfiguration,
    sql_client: SqlClient,
    name_edge_case_it_helpers: IntegrationTestHelpers,
) -> None:
    """Check a soon-to-be-deprecated use case where a manifest contains a metric with the same name as an entity."""
    query_result = name_edge_case_it_helpers.mf_engine.query(
        MetricFlowQueryRequest.create_with_random_request_id(
            metric_names=["homonymous_metric_and_entity"],
            group_by_names=["metric_time"],
            order_by_names=["metric_time", "homonymous_metric_and_entity"],
            where_constraints=["homonymous_metric_and_entity IS NOT NULL"],
        )
    )
    assert query_result.result_df is not None, "Unexpected empty result."

    assert_str_snapshot_equal(
        request=request,
        mf_test_configuration=mf_test_configuration,
        snapshot_id="query_output",
        snapshot_str=query_result.result_df.text_format(),
        sql_engine=sql_client.sql_engine_type,
    )


@pytest.mark.sql_engine_snapshot
def test_filter_by_metric_name_with_2_measures_from_same_source_node(
    request: FixtureRequest,
    mf_test_configuration: MetricFlowTestConfiguration,
    sql_client: SqlClient,
    name_edge_case_it_helpers: IntegrationTestHelpers,
) -> None:
    """Check a soon-to-be-deprecated use case of filtering by a metric name with 2 metrics from the same source node."""
    query_result = name_edge_case_it_helpers.mf_engine.query(
        MetricFlowQueryRequest.create_with_random_request_id(
            metric_names=["homonymous_metric_and_entity", "homonymous_metric_and_dimension"],
            group_by_names=["metric_time"],
            order_by_names=["metric_time", "homonymous_metric_and_entity"],
            where_constraints=["homonymous_metric_and_entity IS NOT NULL"],
        )
    )
    assert query_result.result_df is not None, "Unexpected empty result."

    assert_str_snapshot_equal(
        request=request,
        mf_test_configuration=mf_test_configuration,
        snapshot_id="query_output",
        snapshot_str=query_result.result_df.text_format(),
        sql_engine=sql_client.sql_engine_type,
    )
