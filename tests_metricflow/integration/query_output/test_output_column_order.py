from __future__ import annotations

import logging

import pytest
from metricflow_semantics.test_helpers.config_helpers import MetricFlowTestConfiguration
from metricflow_semantics.toolkit.mf_type_aliases import AnyLengthTuple

from metricflow.engine.metricflow_engine import MetricFlowQueryRequest, MetricFlowQueryResult
from metricflow.protocols.sql_client import SqlClient
from metricflow.sql.optimizer.optimization_levels import SqlOptimizationLevel
from tests_metricflow.integration.conftest import IntegrationTestHelpers

logger = logging.getLogger(__name__)


@pytest.mark.duckdb_only
def test_output_column_order(
    mf_test_configuration: MetricFlowTestConfiguration,
    sql_client: SqlClient,
    it_helpers: IntegrationTestHelpers,
) -> None:
    """Test the order of output columns when `order_output_columns_by_input_order=True`."""
    metric_names: AnyLengthTuple[str] = ("bookings", "listings")
    group_by_names: AnyLengthTuple[str] = ("metric_time__day", "listing", "listing__country_latest")
    query_result: MetricFlowQueryResult = it_helpers.mf_engine.query(
        MetricFlowQueryRequest.create_with_random_request_id(
            metric_names=metric_names,
            group_by_names=group_by_names,
            order_output_columns_by_input_order=True,
        )
    )
    assert query_result.result_df is not None
    output_column_names = query_result.result_df.column_names
    assert tuple(output_column_names) == (
        "metric_time__day",
        "listing",
        "listing__country_latest",
        "bookings",
        "listings",
    )

    # Try out the reversed version at different optimization levels to check CTEs.
    reversed_metric_names = tuple(reversed(metric_names))
    reversed_group_by_names = tuple(reversed(group_by_names))
    reversed_expected_output_column_names = (
        "listing__country_latest",
        "listing",
        "metric_time__day",
        "listings",
        "bookings",
    )

    for optimization_level in SqlOptimizationLevel:
        query_result = it_helpers.mf_engine.query(
            MetricFlowQueryRequest.create_with_random_request_id(
                metric_names=reversed_metric_names,
                group_by_names=reversed_group_by_names,
                order_output_columns_by_input_order=True,
                sql_optimization_level=optimization_level,
            )
        )
        assert query_result.result_df is not None
        output_column_names = query_result.result_df.column_names
        assert tuple(output_column_names) == reversed_expected_output_column_names
