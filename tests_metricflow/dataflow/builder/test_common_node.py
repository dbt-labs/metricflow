from __future__ import annotations

import logging

import pytest
from _pytest.fixtures import FixtureRequest
from metricflow_semantics.query.query_parser import MetricFlowQueryParser
from metricflow_semantics.test_helpers.config_helpers import MetricFlowTestConfiguration
from metricflow_semantics.test_helpers.snapshot_helpers import assert_plan_snapshot_text_equal

from metricflow.dataflow.builder.dataflow_plan_builder import DataflowPlanBuilder
from tests_metricflow.dataflow_plan_to_svg import display_graph_if_requested

logger = logging.getLogger(__name__)


@pytest.mark.skip
def test_common_node(
    request: FixtureRequest,
    mf_test_configuration: MetricFlowTestConfiguration,
    dataflow_plan_builder: DataflowPlanBuilder,
    query_parser: MetricFlowQueryParser,
    create_source_tables: bool,
) -> None:
    """Test querying a derived metric with a cumulative input metric using non-default granularity."""
    query_spec = query_parser.parse_and_validate_query(
        metric_names=("instant_booking_value_ratio",), group_by_names=("listing__country_latest",)
    ).query_spec
    dataflow_plan = dataflow_plan_builder.build_plan(query_spec)

    assert_plan_snapshot_text_equal(
        request=request,
        mf_test_configuration=mf_test_configuration,
        plan=dataflow_plan,
        plan_snapshot_text=dataflow_plan.structure_text(),
    )

    display_graph_if_requested(
        request=request,
        mf_test_configuration=mf_test_configuration,
        dag_graph=dataflow_plan,
    )
