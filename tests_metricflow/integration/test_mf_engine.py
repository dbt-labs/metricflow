from __future__ import annotations

from _pytest.fixtures import FixtureRequest
from metricflow_semantics.test_helpers.config_helpers import MetricFlowTestConfiguration

from metricflow.engine.metricflow_engine import MetricFlowExplainResult, MetricFlowQueryRequest
from metricflow.sql.optimizer.optimization_levels import SqlQueryOptimizationLevel
from tests_metricflow.integration.conftest import IntegrationTestHelpers
from tests_metricflow.snapshot_utils import assert_object_snapshot_equal


def test_list_dimensions(  # noqa: D103
    request: FixtureRequest, mf_test_configuration: MetricFlowTestConfiguration, it_helpers: IntegrationTestHelpers
) -> None:
    assert_object_snapshot_equal(
        request=request,
        mf_test_configuration=mf_test_configuration,
        obj_id="result0",
        obj=sorted([dim.qualified_name for dim in it_helpers.mf_engine.list_dimensions()]),
    )


def test_sql_optimization_level(it_helpers: IntegrationTestHelpers) -> None:
    """Check that different SQL optimization levels produce different SQL."""
    assert (
        SqlQueryOptimizationLevel.default_level() != SqlQueryOptimizationLevel.O0
    ), "The default optimization level should be different from the lowest level."
    explain_result_at_default_level: MetricFlowExplainResult = it_helpers.mf_engine.explain(
        MetricFlowQueryRequest.create_with_random_request_id(
            metric_names=("bookings",),
            group_by_names=("metric_time",),
            sql_optimization_level=SqlQueryOptimizationLevel.default_level(),
        )
    )
    explain_result_at_level_0: MetricFlowExplainResult = it_helpers.mf_engine.explain(
        MetricFlowQueryRequest.create_with_random_request_id(
            metric_names=("bookings",),
            group_by_names=("metric_time",),
            sql_optimization_level=SqlQueryOptimizationLevel.O0,
        )
    )

    assert explain_result_at_default_level.rendered_sql.sql_query != explain_result_at_level_0.rendered_sql.sql_query
