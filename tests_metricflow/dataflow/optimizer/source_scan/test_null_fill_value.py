from __future__ import annotations

import logging

from _pytest.fixtures import FixtureRequest
from metricflow_semantics.query.query_parser import MetricFlowQueryParser
from metricflow_semantics.test_helpers.config_helpers import MetricFlowTestConfiguration

from metricflow.dataflow.builder.dataflow_plan_builder import DataflowPlanBuilder
from tests_metricflow.dataflow.optimizer.source_scan.source_scan_optimizer_helpers import check_source_scan_optimization

logger = logging.getLogger(__name__)


def test_metrics_with_and_without_null_fill_values(
    request: FixtureRequest,
    mf_test_configuration: MetricFlowTestConfiguration,
    query_parser: MetricFlowQueryParser,
    dataflow_plan_builder: DataflowPlanBuilder,
) -> None:
    """Tests combining 2 metrics from a single source where one uses a null-fill value."""
    check_source_scan_optimization(
        request=request,
        mf_test_configuration=mf_test_configuration,
        dataflow_plan_builder=dataflow_plan_builder,
        query_spec=query_parser.parse_and_validate_query(
            metric_names=["booking_value", "bookings_fill_nulls_with_0_without_time_spine"],
            group_by_names=["metric_time"],
        ).query_spec,
        expected_num_sources_in_unoptimized=2,
        # `expected_num_sources_in_optimized` will be updated after the fix.
        expected_num_sources_in_optimized=2,
    )
