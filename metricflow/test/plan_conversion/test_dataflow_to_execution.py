from __future__ import annotations

from _pytest.fixtures import FixtureRequest

from metricflow.dataflow.builder.dataflow_plan_builder import DataflowPlanBuilder
from metricflow.dataset.semantic_model_adapter import SemanticModelDataSet
from metricflow.model.semantic_manifest_lookup import SemanticManifestLookup
from metricflow.plan_conversion.column_resolver import DunderColumnAssociationResolver
from metricflow.plan_conversion.dataflow_to_execution import DataflowToExecutionPlanConverter
from metricflow.plan_conversion.dataflow_to_sql import DataflowToSqlQueryPlanConverter
from metricflow.plan_conversion.time_spine import TimeSpineSource
from metricflow.protocols.async_sql_client import AsyncSqlClient
from metricflow.specs.specs import (
    DimensionSpec,
    EntityReference,
    MetricFlowQuerySpec,
    MetricSpec,
    TimeDimensionSpec,
)
from metricflow.sql.render.sql_plan_renderer import DefaultSqlQueryPlanRenderer
from metricflow.test.fixtures.setup_fixtures import MetricFlowTestSessionState
from metricflow.test.plan_utils import assert_execution_plan_text_equal


def make_execution_plan_converter(  # noqa: D
    semantic_manifest_lookup: SemanticManifestLookup,
    sql_client: AsyncSqlClient,
    time_spine_source: TimeSpineSource,
) -> DataflowToExecutionPlanConverter:
    return DataflowToExecutionPlanConverter[SemanticModelDataSet](
        sql_plan_converter=DataflowToSqlQueryPlanConverter[SemanticModelDataSet](
            column_association_resolver=DunderColumnAssociationResolver(semantic_manifest_lookup),
            semantic_manifest_lookup=semantic_manifest_lookup,
            time_spine_source=time_spine_source,
        ),
        sql_plan_renderer=DefaultSqlQueryPlanRenderer(),
        sql_client=sql_client,
    )


def test_joined_plan(  # noqa: D
    request: FixtureRequest,
    mf_test_session_state: MetricFlowTestSessionState,
    dataflow_plan_builder: DataflowPlanBuilder[SemanticModelDataSet],
    simple_semantic_manifest_lookup: SemanticManifestLookup,
    async_sql_client: AsyncSqlClient,
    time_spine_source: TimeSpineSource,
) -> None:
    dataflow_plan = dataflow_plan_builder.build_plan(
        MetricFlowQuerySpec(
            metric_specs=(MetricSpec(element_name="bookings"),),
            dimension_specs=(
                DimensionSpec(
                    element_name="is_instant",
                    entity_links=(),
                ),
                DimensionSpec(
                    element_name="country_latest",
                    entity_links=(EntityReference("listing"),),
                ),
            ),
        )
    )

    to_execution_plan_converter = make_execution_plan_converter(
        simple_semantic_manifest_lookup, async_sql_client, time_spine_source
    )

    execution_plan = to_execution_plan_converter.convert_to_execution_plan(dataflow_plan)

    assert_execution_plan_text_equal(
        request=request,
        mf_test_session_state=mf_test_session_state,
        sql_client=async_sql_client,
        execution_plan=execution_plan,
    )


def test_small_combined_metrics_plan(  # noqa: D
    request: FixtureRequest,
    mf_test_session_state: MetricFlowTestSessionState,
    async_sql_client: AsyncSqlClient,
    dataflow_plan_builder: DataflowPlanBuilder[SemanticModelDataSet],
    simple_semantic_manifest_lookup: SemanticManifestLookup,
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
                    entity_links=(),
                ),
            ),
        )
    )

    to_execution_plan_converter = make_execution_plan_converter(
        semantic_manifest_lookup=simple_semantic_manifest_lookup,
        sql_client=async_sql_client,
        time_spine_source=time_spine_source,
    )
    execution_plan = to_execution_plan_converter.convert_to_execution_plan(dataflow_plan)

    assert_execution_plan_text_equal(
        request=request,
        mf_test_session_state=mf_test_session_state,
        sql_client=async_sql_client,
        execution_plan=execution_plan,
    )


def test_combined_metrics_plan(  # noqa: D
    request: FixtureRequest,
    mf_test_session_state: MetricFlowTestSessionState,
    async_sql_client: AsyncSqlClient,
    dataflow_plan_builder: DataflowPlanBuilder[SemanticModelDataSet],
    simple_semantic_manifest_lookup: SemanticManifestLookup,
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
                    entity_links=(),
                ),
            ),
            time_dimension_specs=(TimeDimensionSpec(element_name="ds", entity_links=()),),
        )
    )

    to_execution_plan_converter = make_execution_plan_converter(
        semantic_manifest_lookup=simple_semantic_manifest_lookup,
        sql_client=async_sql_client,
        time_spine_source=time_spine_source,
    )
    execution_plan = to_execution_plan_converter.convert_to_execution_plan(dataflow_plan)

    assert_execution_plan_text_equal(
        request=request,
        mf_test_session_state=mf_test_session_state,
        sql_client=async_sql_client,
        execution_plan=execution_plan,
    )


def test_multihop_joined_plan(  # noqa: D
    request: FixtureRequest,
    mf_test_session_state: MetricFlowTestSessionState,
    multihop_dataflow_plan_builder: DataflowPlanBuilder[SemanticModelDataSet],
    multi_hop_join_semantic_manifest_lookup: SemanticManifestLookup,
    async_sql_client: AsyncSqlClient,
    time_spine_source: TimeSpineSource,
) -> None:
    """Tests a plan getting a measure and a joined dimension."""
    dataflow_plan = multihop_dataflow_plan_builder.build_plan(
        MetricFlowQuerySpec(
            metric_specs=(MetricSpec(element_name="txn_count"),),
            dimension_specs=(
                DimensionSpec(
                    element_name="customer_name",
                    entity_links=(
                        EntityReference(element_name="account_id"),
                        EntityReference(element_name="customer_id"),
                    ),
                ),
            ),
        )
    )

    to_execution_plan_converter = DataflowToExecutionPlanConverter(
        sql_plan_converter=DataflowToSqlQueryPlanConverter[SemanticModelDataSet](
            column_association_resolver=DunderColumnAssociationResolver(multi_hop_join_semantic_manifest_lookup),
            semantic_manifest_lookup=multi_hop_join_semantic_manifest_lookup,
            time_spine_source=time_spine_source,
        ),
        sql_plan_renderer=DefaultSqlQueryPlanRenderer(),
        sql_client=async_sql_client,
    )

    execution_plan = to_execution_plan_converter.convert_to_execution_plan(dataflow_plan)

    assert_execution_plan_text_equal(
        request=request,
        mf_test_session_state=mf_test_session_state,
        sql_client=async_sql_client,
        execution_plan=execution_plan,
    )
