from __future__ import annotations

import pytest
from _pytest.fixtures import FixtureRequest
from metricflow_semantics.query.query_parser import MetricFlowQueryParser
from metricflow_semantics.test_helpers.config_helpers import MetricFlowTestConfiguration

from metricflow.dataflow.builder.dataflow_plan_builder import DataflowPlanBuilder
from metricflow.plan_conversion.to_sql_plan.dataflow_to_sql import DataflowToSqlPlanConverter
from metricflow.protocols.sql_client import SqlClient
from tests_metricflow.plan_conversion.test_dataflow_to_sql_plan import convert_and_check


@pytest.mark.sql_engine_snapshot
@pytest.mark.duckdb_only
def test_input_metric_with_null_fill_value(
    request: FixtureRequest,
    mf_test_configuration: MetricFlowTestConfiguration,
    dataflow_plan_builder: DataflowPlanBuilder,
    dataflow_to_sql_converter: DataflowToSqlPlanConverter,
    query_parser: MetricFlowQueryParser,
    sql_client: SqlClient,
) -> None:
    """Test rendering of a derived metric with an input metric that sets `fill_nulls_with`."""
    parse_result = query_parser.parse_and_validate_query(metric_names=["test_metric"], group_by_names=["metric_time"])

    dataflow_plan = dataflow_plan_builder.build_plan(
        query_spec=parse_result.query_spec,
    )

    convert_and_check(
        request=request,
        mf_test_configuration=mf_test_configuration,
        dataflow_to_sql_converter=dataflow_to_sql_converter,
        sql_client=sql_client,
        node=dataflow_plan.sink_node,
    )
