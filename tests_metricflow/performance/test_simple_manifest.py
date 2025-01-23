from __future__ import annotations

from dbt_semantic_interfaces.references import EntityReference
from metricflow_semantics.specs.dimension_spec import DimensionSpec
from metricflow_semantics.specs.metric_spec import MetricSpec
from metricflow_semantics.specs.query_spec import MetricFlowQuerySpec
from metricflow_semantics.test_helpers.config_helpers import MetricFlowTestConfiguration

from metricflow.dataflow.builder.dataflow_plan_builder import DataflowPlanBuilder
from metricflow.plan_conversion.to_sql_plan.dataflow_to_sql import DataflowToSqlPlanConverter
from metricflow.protocols.sql_client import SqlClient
from tests_metricflow.performance.conftest import MeasureFixture


def test_simple_query(
    measure_compilation_performance: MeasureFixture,
    mf_test_configuration: MetricFlowTestConfiguration,
    dataflow_plan_builder: DataflowPlanBuilder,
    dataflow_to_sql_converter: DataflowToSqlPlanConverter,
    sql_client: SqlClient,
) -> None:
    """Tests converting a dataflow plan to a SQL query plan where there is a join between 1 measure and 2 dimensions."""
    query_spec = MetricFlowQuerySpec(
        metric_specs=(MetricSpec(element_name="identity_verifications"),),
        dimension_specs=(
            DimensionSpec(
                element_name="home_state",
                entity_links=(EntityReference(element_name="user"),),
            ),
        ),
    )

    measure_compilation_performance(
        query_spec=query_spec,
        dataflow_plan_builder=dataflow_plan_builder,
        dataflow_to_sql_converter=dataflow_to_sql_converter,
        sql_client=sql_client,
    )


def test_simple_query_2(
    measure_compilation_performance: MeasureFixture,
    mf_test_configuration: MetricFlowTestConfiguration,
    dataflow_plan_builder: DataflowPlanBuilder,
    dataflow_to_sql_converter: DataflowToSqlPlanConverter,
    sql_client: SqlClient,
) -> None:
    """Tests converting a dataflow plan to a SQL query plan where there is a join between 1 measure and 2 dimensions."""
    query_spec = MetricFlowQuerySpec(
        metric_specs=(MetricSpec(element_name="identity_verifications"),),
        dimension_specs=(
            DimensionSpec(
                element_name="home_state",
                entity_links=(EntityReference(element_name="user"),),
            ),
        ),
    )

    measure_compilation_performance(
        query_spec=query_spec,
        dataflow_plan_builder=dataflow_plan_builder,
        dataflow_to_sql_converter=dataflow_to_sql_converter,
        sql_client=sql_client,
    )
