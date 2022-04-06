from _pytest.fixtures import FixtureRequest

from metricflow.dataflow.builder.dataflow_plan_builder import DataflowPlanBuilder
from metricflow.model.semantic_model import SemanticModel
from metricflow.plan_conversion.column_resolver import DefaultColumnAssociationResolver
from metricflow.plan_conversion.dataflow_to_execution import DataflowToExecutionPlanConverter
from metricflow.plan_conversion.dataflow_to_sql import DataflowToSqlQueryPlanConverter
from metricflow.plan_conversion.time_spine import TimeSpineSource
from metricflow.protocols.sql_client import SqlClient
from metricflow.specs import (
    MetricFlowQuerySpec,
    MetricSpec,
    DimensionSpec,
    LinklessIdentifierSpec,
    TimeDimensionSpec,
)
from metricflow.sql.render.sql_plan_renderer import DefaultSqlQueryPlanRenderer
from metricflow.dataset.data_source_adapter import DataSourceDataSet
from metricflow.test.fixtures.setup_fixtures import MetricFlowTestSessionState
from metricflow.test.plan_utils import assert_execution_plan_text_equal


def make_execution_plan_converter(  # noqa: D
    semantic_model: SemanticModel,
    sql_client: SqlClient,
    time_spine_source: TimeSpineSource,
) -> DataflowToExecutionPlanConverter:
    return DataflowToExecutionPlanConverter[DataSourceDataSet](
        sql_plan_converter=DataflowToSqlQueryPlanConverter[DataSourceDataSet](
            column_association_resolver=DefaultColumnAssociationResolver(semantic_model),
            semantic_model=semantic_model,
            time_spine_source=time_spine_source,
        ),
        sql_plan_renderer=DefaultSqlQueryPlanRenderer(),
        sql_client=sql_client,
    )


def test_joined_plan(  # noqa: D
    request: FixtureRequest,
    mf_test_session_state: MetricFlowTestSessionState,
    dataflow_plan_builder: DataflowPlanBuilder[DataSourceDataSet],
    simple_semantic_model: SemanticModel,
    sql_client: SqlClient,
    time_spine_source: TimeSpineSource,
) -> None:
    dataflow_plan = dataflow_plan_builder.build_plan(
        MetricFlowQuerySpec(
            metric_specs=(MetricSpec(element_name="bookings"),),
            dimension_specs=(
                DimensionSpec(
                    element_name="is_instant",
                    identifier_links=(),
                ),
                DimensionSpec(
                    element_name="country_latest",
                    identifier_links=(LinklessIdentifierSpec.from_element_name("listing"),),
                ),
            ),
        )
    )

    to_execution_plan_converter = make_execution_plan_converter(simple_semantic_model, sql_client, time_spine_source)

    execution_plan = to_execution_plan_converter.convert_to_execution_plan(dataflow_plan)

    assert_execution_plan_text_equal(
        request=request,
        mf_test_session_state=mf_test_session_state,
        sql_client=sql_client,
        execution_plan=execution_plan,
    )


def test_small_combined_metrics_plan(  # noqa: D
    request: FixtureRequest,
    mf_test_session_state: MetricFlowTestSessionState,
    sql_client: SqlClient,
    dataflow_plan_builder: DataflowPlanBuilder[DataSourceDataSet],
    simple_semantic_model: SemanticModel,
    time_spine_source: TimeSpineSource,
) -> None:
    dataflow_plan = dataflow_plan_builder.build_plan(
        MetricFlowQuerySpec(
            metric_specs=(
                MetricSpec(element_name="bookings"),
                MetricSpec(element_name="booking_value"),
            ),
            dimension_specs=(
                DimensionSpec(
                    element_name="is_instant",
                    identifier_links=(),
                ),
            ),
        )
    )

    to_execution_plan_converter = make_execution_plan_converter(
        semantic_model=simple_semantic_model,
        sql_client=sql_client,
        time_spine_source=time_spine_source,
    )
    execution_plan = to_execution_plan_converter.convert_to_execution_plan(dataflow_plan)

    assert_execution_plan_text_equal(
        request=request,
        mf_test_session_state=mf_test_session_state,
        sql_client=sql_client,
        execution_plan=execution_plan,
    )


def test_combined_metrics_plan(  # noqa: D
    request: FixtureRequest,
    mf_test_session_state: MetricFlowTestSessionState,
    sql_client: SqlClient,
    dataflow_plan_builder: DataflowPlanBuilder[DataSourceDataSet],
    simple_semantic_model: SemanticModel,
    time_spine_source: TimeSpineSource,
) -> None:
    dataflow_plan = dataflow_plan_builder.build_plan(
        MetricFlowQuerySpec(
            metric_specs=(
                MetricSpec(element_name="bookings"),
                MetricSpec(element_name="instant_bookings"),
                MetricSpec(element_name="booking_value"),
            ),
            dimension_specs=(
                DimensionSpec(
                    element_name="is_instant",
                    identifier_links=(),
                ),
            ),
            time_dimension_specs=(TimeDimensionSpec(element_name="ds", identifier_links=()),),
        )
    )

    to_execution_plan_converter = make_execution_plan_converter(
        semantic_model=simple_semantic_model,
        sql_client=sql_client,
        time_spine_source=time_spine_source,
    )
    execution_plan = to_execution_plan_converter.convert_to_execution_plan(dataflow_plan)

    assert_execution_plan_text_equal(
        request=request,
        mf_test_session_state=mf_test_session_state,
        sql_client=sql_client,
        execution_plan=execution_plan,
    )


def test_multihop_joined_plan(  # noqa: D
    request: FixtureRequest,
    mf_test_session_state: MetricFlowTestSessionState,
    multihop_dataflow_plan_builder: DataflowPlanBuilder[DataSourceDataSet],
    multi_hop_join_semantic_model: SemanticModel,
    sql_client: SqlClient,
    time_spine_source: TimeSpineSource,
) -> None:
    """Tests a plan getting a measure and a joined dimension."""
    dataflow_plan = multihop_dataflow_plan_builder.build_plan(
        MetricFlowQuerySpec(
            metric_specs=(MetricSpec(element_name="txn_count"),),
            dimension_specs=(
                DimensionSpec(
                    element_name="customer_name",
                    identifier_links=(
                        LinklessIdentifierSpec.from_element_name(element_name="account_id"),
                        LinklessIdentifierSpec.from_element_name(element_name="customer_id"),
                    ),
                ),
            ),
        )
    )

    to_execution_plan_converter = DataflowToExecutionPlanConverter(
        sql_plan_converter=DataflowToSqlQueryPlanConverter[DataSourceDataSet](
            column_association_resolver=DefaultColumnAssociationResolver(multi_hop_join_semantic_model),
            semantic_model=multi_hop_join_semantic_model,
            time_spine_source=time_spine_source,
        ),
        sql_plan_renderer=DefaultSqlQueryPlanRenderer(),
        sql_client=sql_client,
    )

    execution_plan = to_execution_plan_converter.convert_to_execution_plan(dataflow_plan)

    assert_execution_plan_text_equal(
        request=request,
        mf_test_session_state=mf_test_session_state,
        sql_client=sql_client,
        execution_plan=execution_plan,
    )
