from __future__ import annotations

import pytest
from _pytest.fixtures import FixtureRequest
from dbt_semantic_interfaces.references import EntityReference
from dbt_semantic_interfaces.type_enums.time_granularity import TimeGranularity
from metricflow_semantics.model.semantic_manifest_lookup import SemanticManifestLookup
from metricflow_semantics.specs.dimension_spec import DimensionSpec
from metricflow_semantics.specs.dunder_column_association_resolver import DunderColumnAssociationResolver
from metricflow_semantics.specs.metric_spec import MetricSpec
from metricflow_semantics.specs.query_spec import MetricFlowQuerySpec
from metricflow_semantics.specs.time_dimension_spec import TimeDimensionSpec
from metricflow_semantics.test_helpers.config_helpers import MetricFlowTestConfiguration
from metricflow_semantics.time.granularity import ExpandedTimeGranularity

from metricflow.dataflow.builder.dataflow_plan_builder import DataflowPlanBuilder
from metricflow.execution.dataflow_to_execution import DataflowToExecutionPlanConverter
from metricflow.plan_conversion.to_sql_plan.dataflow_to_sql import DataflowToSqlPlanConverter
from metricflow.protocols.sql_client import SqlClient
from metricflow.sql.optimizer.optimization_levels import SqlOptimizationLevel
from metricflow.sql.render.sql_plan_renderer import DefaultSqlPlanRenderer
from tests_metricflow.snapshot_utils import assert_execution_plan_text_equal


def make_execution_plan_converter(  # noqa: D103
    semantic_manifest_lookup: SemanticManifestLookup,
    sql_client: SqlClient,
) -> DataflowToExecutionPlanConverter:
    return DataflowToExecutionPlanConverter(
        sql_plan_converter=DataflowToSqlPlanConverter(
            column_association_resolver=DunderColumnAssociationResolver(),
            semantic_manifest_lookup=semantic_manifest_lookup,
        ),
        sql_plan_renderer=DefaultSqlPlanRenderer(),
        sql_client=sql_client,
        sql_optimization_level=SqlOptimizationLevel.default_level(),
    )


@pytest.mark.sql_engine_snapshot
@pytest.mark.duckdb_only
def test_joined_plan(  # noqa: D103
    request: FixtureRequest,
    mf_test_configuration: MetricFlowTestConfiguration,
    dataflow_plan_builder: DataflowPlanBuilder,
    simple_semantic_manifest_lookup: SemanticManifestLookup,
    sql_client: SqlClient,
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
        semantic_manifest_lookup=simple_semantic_manifest_lookup,
        sql_client=sql_client,
    )

    execution_plan = to_execution_plan_converter.convert_to_execution_plan(dataflow_plan).execution_plan

    assert_execution_plan_text_equal(
        request=request,
        mf_test_configuration=mf_test_configuration,
        sql_client=sql_client,
        execution_plan=execution_plan,
    )


@pytest.mark.sql_engine_snapshot
@pytest.mark.duckdb_only
def test_small_combined_metrics_plan(  # noqa: D103
    request: FixtureRequest,
    mf_test_configuration: MetricFlowTestConfiguration,
    sql_client: SqlClient,
    dataflow_plan_builder: DataflowPlanBuilder,
    simple_semantic_manifest_lookup: SemanticManifestLookup,
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
        sql_client=sql_client,
    )
    execution_plan = to_execution_plan_converter.convert_to_execution_plan(dataflow_plan).execution_plan

    assert_execution_plan_text_equal(
        request=request,
        mf_test_configuration=mf_test_configuration,
        sql_client=sql_client,
        execution_plan=execution_plan,
    )


@pytest.mark.sql_engine_snapshot
@pytest.mark.duckdb_only
def test_combined_metrics_plan(  # noqa: D103
    request: FixtureRequest,
    mf_test_configuration: MetricFlowTestConfiguration,
    sql_client: SqlClient,
    dataflow_plan_builder: DataflowPlanBuilder,
    simple_semantic_manifest_lookup: SemanticManifestLookup,
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
            time_dimension_specs=(
                TimeDimensionSpec(
                    element_name="ds",
                    entity_links=(),
                    time_granularity=ExpandedTimeGranularity.from_time_granularity(TimeGranularity.DAY),
                ),
            ),
        )
    )

    to_execution_plan_converter = make_execution_plan_converter(
        semantic_manifest_lookup=simple_semantic_manifest_lookup,
        sql_client=sql_client,
    )
    execution_plan = to_execution_plan_converter.convert_to_execution_plan(dataflow_plan).execution_plan

    assert_execution_plan_text_equal(
        request=request,
        mf_test_configuration=mf_test_configuration,
        sql_client=sql_client,
        execution_plan=execution_plan,
    )


@pytest.mark.sql_engine_snapshot
@pytest.mark.duckdb_only
def test_multihop_joined_plan(
    request: FixtureRequest,
    mf_test_configuration: MetricFlowTestConfiguration,
    multihop_dataflow_plan_builder: DataflowPlanBuilder,
    partitioned_multi_hop_join_semantic_manifest_lookup: SemanticManifestLookup,
    sql_client: SqlClient,
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

    to_execution_plan_converter = make_execution_plan_converter(
        semantic_manifest_lookup=partitioned_multi_hop_join_semantic_manifest_lookup,
        sql_client=sql_client,
    )
    execution_plan = to_execution_plan_converter.convert_to_execution_plan(dataflow_plan).execution_plan

    assert_execution_plan_text_equal(
        request=request,
        mf_test_configuration=mf_test_configuration,
        sql_client=sql_client,
        execution_plan=execution_plan,
    )
