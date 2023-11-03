"""Tests base query rendering behavior by comparing rendered output against snapshot files.

This module is meant to start with a MetricFlowQuerySpec or equivalent representation of
a MetricFlow query input and end up with a query rendered for execution against a the
target engine. This will depend on test semantic manifests and engine-specific rendering
logic as propagated via the SqlClient input.
"""
from __future__ import annotations

import pytest
from _pytest.fixtures import FixtureRequest
from dbt_semantic_interfaces.implementations.filters.where_filter import PydanticWhereFilter
from dbt_semantic_interfaces.references import EntityReference
from dbt_semantic_interfaces.test_utils import as_datetime
from dbt_semantic_interfaces.type_enums.time_granularity import TimeGranularity

from metricflow.dataflow.builder.dataflow_plan_builder import DataflowPlanBuilder
from metricflow.dataset.dataset import DataSet
from metricflow.filters.time_constraint import TimeRangeConstraint
from metricflow.model.semantic_manifest_lookup import SemanticManifestLookup
from metricflow.plan_conversion.column_resolver import DunderColumnAssociationResolver
from metricflow.plan_conversion.dataflow_to_sql import DataflowToSqlQueryPlanConverter
from metricflow.protocols.sql_client import SqlClient
from metricflow.specs.column_assoc import ColumnAssociationResolver
from metricflow.specs.specs import (
    DimensionSpec,
    MetricFlowQuerySpec,
    MetricSpec,
    OrderBySpec,
    TimeDimensionSpec,
)
from metricflow.specs.where_filter_transform import WhereSpecFactory
from metricflow.test.fixtures.model_fixtures import ConsistentIdObjectRepository
from metricflow.test.fixtures.setup_fixtures import MetricFlowTestSessionState
from metricflow.test.query_rendering.compare_rendered_query import convert_and_check
from metricflow.test.time.metric_time_dimension import MTD_SPEC_DAY


@pytest.fixture(scope="session")
def multihop_dataflow_to_sql_converter(  # noqa: D
    multi_hop_join_semantic_manifest_lookup: SemanticManifestLookup,
) -> DataflowToSqlQueryPlanConverter:
    return DataflowToSqlQueryPlanConverter(
        column_association_resolver=DunderColumnAssociationResolver(multi_hop_join_semantic_manifest_lookup),
        semantic_manifest_lookup=multi_hop_join_semantic_manifest_lookup,
    )


@pytest.fixture(scope="session")
def scd_dataflow_to_sql_converter(  # noqa: D
    scd_semantic_manifest_lookup: SemanticManifestLookup,
) -> DataflowToSqlQueryPlanConverter:
    return DataflowToSqlQueryPlanConverter(
        column_association_resolver=DunderColumnAssociationResolver(scd_semantic_manifest_lookup),
        semantic_manifest_lookup=scd_semantic_manifest_lookup,
    )


@pytest.mark.sql_engine_snapshot
def test_multihop_node(
    request: FixtureRequest,
    mf_test_session_state: MetricFlowTestSessionState,
    multihop_dataflow_plan_builder: DataflowPlanBuilder,
    multihop_dataflow_to_sql_converter: DataflowToSqlQueryPlanConverter,
    sql_client: SqlClient,
) -> None:
    """Tests converting a dataflow plan to a SQL query plan where there is a join between 1 measure and 2 dimensions."""
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

    convert_and_check(
        request=request,
        mf_test_session_state=mf_test_session_state,
        dataflow_to_sql_converter=multihop_dataflow_to_sql_converter,
        sql_client=sql_client,
        node=dataflow_plan.sink_output_nodes[0].parent_node,
    )


@pytest.mark.sql_engine_snapshot
def test_filter_with_where_constraint_on_join_dim(
    request: FixtureRequest,
    mf_test_session_state: MetricFlowTestSessionState,
    column_association_resolver: ColumnAssociationResolver,
    dataflow_plan_builder: DataflowPlanBuilder,
    dataflow_to_sql_converter: DataflowToSqlQueryPlanConverter,
    consistent_id_object_repository: ConsistentIdObjectRepository,
    sql_client: SqlClient,
) -> None:
    """Tests converting a dataflow plan to a SQL query plan where there is a join between 1 measure and 2 dimensions."""
    dataflow_plan = dataflow_plan_builder.build_plan(
        MetricFlowQuerySpec(
            metric_specs=(MetricSpec(element_name="bookings"),),
            dimension_specs=(
                DimensionSpec(
                    element_name="is_instant",
                    entity_links=(),
                ),
            ),
            where_constraint=(
                WhereSpecFactory(
                    column_association_resolver=column_association_resolver,
                ).create_from_where_filter(
                    PydanticWhereFilter(
                        where_sql_template="{{ Dimension('listing__country_latest') }} = 'us'",
                    )
                )
            ),
        )
    )

    convert_and_check(
        request=request,
        mf_test_session_state=mf_test_session_state,
        dataflow_to_sql_converter=dataflow_to_sql_converter,
        sql_client=sql_client,
        node=dataflow_plan.sink_output_nodes[0].parent_node,
    )


@pytest.mark.sql_engine_snapshot
def test_partitioned_join(
    request: FixtureRequest,
    mf_test_session_state: MetricFlowTestSessionState,
    dataflow_plan_builder: DataflowPlanBuilder,
    dataflow_to_sql_converter: DataflowToSqlQueryPlanConverter,
    consistent_id_object_repository: ConsistentIdObjectRepository,
    sql_client: SqlClient,
) -> None:
    """Tests converting a dataflow plan where there's a join on a partitioned dimension."""
    dataflow_plan = dataflow_plan_builder.build_plan(
        MetricFlowQuerySpec(
            metric_specs=(MetricSpec(element_name="identity_verifications"),),
            dimension_specs=(
                DimensionSpec(
                    element_name="home_state",
                    entity_links=(EntityReference(element_name="user"),),
                ),
            ),
        )
    )

    convert_and_check(
        request=request,
        mf_test_session_state=mf_test_session_state,
        dataflow_to_sql_converter=dataflow_to_sql_converter,
        sql_client=sql_client,
        node=dataflow_plan.sink_output_nodes[0].parent_node,
    )


@pytest.mark.sql_engine_snapshot
def test_limit_rows(  # noqa: D
    request: FixtureRequest,
    mf_test_session_state: MetricFlowTestSessionState,
    dataflow_plan_builder: DataflowPlanBuilder,
    dataflow_to_sql_converter: DataflowToSqlQueryPlanConverter,
    sql_client: SqlClient,
) -> None:
    """Tests a plan with a limit to the number of rows returned."""
    dataflow_plan = dataflow_plan_builder.build_plan(
        MetricFlowQuerySpec(
            metric_specs=(MetricSpec(element_name="bookings"),),
            time_dimension_specs=(
                TimeDimensionSpec(
                    element_name="ds",
                    entity_links=(),
                ),
            ),
            limit=1,
        )
    )

    convert_and_check(
        request=request,
        mf_test_session_state=mf_test_session_state,
        dataflow_to_sql_converter=dataflow_to_sql_converter,
        sql_client=sql_client,
        node=dataflow_plan.sink_output_nodes[0].parent_node,
    )


@pytest.mark.sql_engine_snapshot
def test_distinct_values(  # noqa: D
    request: FixtureRequest,
    mf_test_session_state: MetricFlowTestSessionState,
    dataflow_plan_builder: DataflowPlanBuilder,
    dataflow_to_sql_converter: DataflowToSqlQueryPlanConverter,
    column_association_resolver: ColumnAssociationResolver,
    sql_client: SqlClient,
) -> None:
    """Tests a plan to get distinct values for a dimension."""
    dataflow_plan = dataflow_plan_builder.build_plan_for_distinct_values(
        query_spec=MetricFlowQuerySpec(
            dimension_specs=(
                DimensionSpec(element_name="country_latest", entity_links=(EntityReference(element_name="listing"),)),
            ),
            where_constraint=(
                WhereSpecFactory(
                    column_association_resolver=column_association_resolver,
                ).create_from_where_filter(
                    PydanticWhereFilter(
                        where_sql_template="{{ Dimension('listing__country_latest') }} = 'us'",
                    )
                )
            ),
            order_by_specs=(
                OrderBySpec(
                    instance_spec=DimensionSpec(
                        element_name="country_latest", entity_links=(EntityReference(element_name="listing"),)
                    ),
                    descending=True,
                ),
            ),
            limit=100,
        )
    )

    convert_and_check(
        request=request,
        mf_test_session_state=mf_test_session_state,
        dataflow_to_sql_converter=dataflow_to_sql_converter,
        sql_client=sql_client,
        node=dataflow_plan.sink_output_nodes[0].parent_node,
    )


@pytest.mark.sql_engine_snapshot
def test_local_dimension_using_local_entity(  # noqa: D
    request: FixtureRequest,
    mf_test_session_state: MetricFlowTestSessionState,
    dataflow_plan_builder: DataflowPlanBuilder,
    dataflow_to_sql_converter: DataflowToSqlQueryPlanConverter,
    sql_client: SqlClient,
) -> None:
    dataflow_plan = dataflow_plan_builder.build_plan(
        MetricFlowQuerySpec(
            metric_specs=(MetricSpec(element_name="listings"),),
            dimension_specs=(
                DimensionSpec(
                    element_name="country_latest",
                    entity_links=(EntityReference(element_name="listing"),),
                ),
            ),
        )
    )

    convert_and_check(
        request=request,
        mf_test_session_state=mf_test_session_state,
        dataflow_to_sql_converter=dataflow_to_sql_converter,
        sql_client=sql_client,
        node=dataflow_plan.sink_output_nodes[0].parent_node,
    )


@pytest.mark.sql_engine_snapshot
def test_measure_constraint(  # noqa: D
    request: FixtureRequest,
    mf_test_session_state: MetricFlowTestSessionState,
    dataflow_plan_builder: DataflowPlanBuilder,
    dataflow_to_sql_converter: DataflowToSqlQueryPlanConverter,
    sql_client: SqlClient,
) -> None:
    dataflow_plan = dataflow_plan_builder.build_plan(
        MetricFlowQuerySpec(
            metric_specs=(MetricSpec(element_name="lux_booking_value_rate_expr"),),
            time_dimension_specs=(MTD_SPEC_DAY,),
        )
    )

    convert_and_check(
        request=request,
        mf_test_session_state=mf_test_session_state,
        dataflow_to_sql_converter=dataflow_to_sql_converter,
        sql_client=sql_client,
        node=dataflow_plan.sink_output_nodes[0].parent_node,
    )


@pytest.mark.sql_engine_snapshot
def test_measure_constraint_with_reused_measure(  # noqa: D
    request: FixtureRequest,
    mf_test_session_state: MetricFlowTestSessionState,
    dataflow_plan_builder: DataflowPlanBuilder,
    dataflow_to_sql_converter: DataflowToSqlQueryPlanConverter,
    sql_client: SqlClient,
) -> None:
    dataflow_plan = dataflow_plan_builder.build_plan(
        MetricFlowQuerySpec(
            metric_specs=(MetricSpec(element_name="instant_booking_value_ratio"),),
            time_dimension_specs=(MTD_SPEC_DAY,),
        )
    )

    convert_and_check(
        request=request,
        mf_test_session_state=mf_test_session_state,
        dataflow_to_sql_converter=dataflow_to_sql_converter,
        sql_client=sql_client,
        node=dataflow_plan.sink_output_nodes[0].parent_node,
    )


@pytest.mark.sql_engine_snapshot
def test_measure_constraint_with_single_expr_and_alias(  # noqa: D
    request: FixtureRequest,
    mf_test_session_state: MetricFlowTestSessionState,
    dataflow_plan_builder: DataflowPlanBuilder,
    dataflow_to_sql_converter: DataflowToSqlQueryPlanConverter,
    sql_client: SqlClient,
) -> None:
    dataflow_plan = dataflow_plan_builder.build_plan(
        MetricFlowQuerySpec(
            metric_specs=(MetricSpec(element_name="double_counted_delayed_bookings"),),
            time_dimension_specs=(MTD_SPEC_DAY,),
        )
    )

    convert_and_check(
        request=request,
        mf_test_session_state=mf_test_session_state,
        dataflow_to_sql_converter=dataflow_to_sql_converter,
        sql_client=sql_client,
        node=dataflow_plan.sink_output_nodes[0].parent_node,
    )


@pytest.mark.sql_engine_snapshot
def test_join_to_scd_dimension(
    request: FixtureRequest,
    mf_test_session_state: MetricFlowTestSessionState,
    scd_column_association_resolver: ColumnAssociationResolver,
    scd_dataflow_plan_builder: DataflowPlanBuilder,
    scd_dataflow_to_sql_converter: DataflowToSqlQueryPlanConverter,
    sql_client: SqlClient,
) -> None:
    """Tests conversion of a plan using a dimension with a validity window inside a measure constraint."""
    dataflow_plan = scd_dataflow_plan_builder.build_plan(
        MetricFlowQuerySpec(
            metric_specs=(
                MetricSpec(
                    element_name="family_bookings",
                    constraint=(
                        WhereSpecFactory(
                            column_association_resolver=scd_column_association_resolver,
                        ).create_from_where_filter(
                            PydanticWhereFilter(
                                where_sql_template="{{ Dimension('listing__capacity') }} > 2",
                            )
                        )
                    ),
                ),
            ),
            time_dimension_specs=(MTD_SPEC_DAY,),
        ),
    )

    convert_and_check(
        request=request,
        mf_test_session_state=mf_test_session_state,
        dataflow_to_sql_converter=scd_dataflow_to_sql_converter,
        sql_client=sql_client,
        node=dataflow_plan.sink_output_nodes[0].parent_node,
    )


@pytest.mark.sql_engine_snapshot
def test_multi_hop_through_scd_dimension(
    request: FixtureRequest,
    mf_test_session_state: MetricFlowTestSessionState,
    scd_dataflow_plan_builder: DataflowPlanBuilder,
    scd_dataflow_to_sql_converter: DataflowToSqlQueryPlanConverter,
    sql_client: SqlClient,
) -> None:
    """Tests conversion of a plan using a dimension that is reached through an SCD table."""
    dataflow_plan = scd_dataflow_plan_builder.build_plan(
        MetricFlowQuerySpec(
            metric_specs=(MetricSpec(element_name="bookings"),),
            time_dimension_specs=(MTD_SPEC_DAY,),
            dimension_specs=(DimensionSpec.from_name(name="listing__user__home_state_latest"),),
        )
    )

    convert_and_check(
        request=request,
        mf_test_session_state=mf_test_session_state,
        dataflow_to_sql_converter=scd_dataflow_to_sql_converter,
        sql_client=sql_client,
        node=dataflow_plan.sink_output_nodes[0].parent_node,
    )


@pytest.mark.sql_engine_snapshot
def test_multi_hop_to_scd_dimension(
    request: FixtureRequest,
    mf_test_session_state: MetricFlowTestSessionState,
    scd_dataflow_plan_builder: DataflowPlanBuilder,
    scd_dataflow_to_sql_converter: DataflowToSqlQueryPlanConverter,
    sql_client: SqlClient,
) -> None:
    """Tests conversion of a plan using an SCD dimension that is reached through another table."""
    dataflow_plan = scd_dataflow_plan_builder.build_plan(
        MetricFlowQuerySpec(
            metric_specs=(MetricSpec(element_name="bookings"),),
            time_dimension_specs=(MTD_SPEC_DAY,),
            dimension_specs=(DimensionSpec.from_name(name="listing__lux_listing__is_confirmed_lux"),),
        )
    )

    convert_and_check(
        request=request,
        mf_test_session_state=mf_test_session_state,
        dataflow_to_sql_converter=scd_dataflow_to_sql_converter,
        sql_client=sql_client,
        node=dataflow_plan.sink_output_nodes[0].parent_node,
    )


@pytest.mark.sql_engine_snapshot
def test_multiple_metrics_no_dimensions(  # noqa: D
    request: FixtureRequest,
    mf_test_session_state: MetricFlowTestSessionState,
    dataflow_plan_builder: DataflowPlanBuilder,
    dataflow_to_sql_converter: DataflowToSqlQueryPlanConverter,
    sql_client: SqlClient,
) -> None:
    dataflow_plan = dataflow_plan_builder.build_plan(
        MetricFlowQuerySpec(
            metric_specs=(MetricSpec(element_name="bookings"), MetricSpec(element_name="listings")),
            time_range_constraint=TimeRangeConstraint(
                start_time=as_datetime("2020-01-01"), end_time=as_datetime("2020-01-01")
            ),
        )
    )

    convert_and_check(
        request=request,
        mf_test_session_state=mf_test_session_state,
        dataflow_to_sql_converter=dataflow_to_sql_converter,
        sql_client=sql_client,
        node=dataflow_plan.sink_output_nodes[0].parent_node,
    )


@pytest.mark.sql_engine_snapshot
def test_metric_with_measures_from_multiple_sources_no_dimensions(  # noqa: D
    request: FixtureRequest,
    mf_test_session_state: MetricFlowTestSessionState,
    dataflow_plan_builder: DataflowPlanBuilder,
    dataflow_to_sql_converter: DataflowToSqlQueryPlanConverter,
    sql_client: SqlClient,
) -> None:
    dataflow_plan = dataflow_plan_builder.build_plan(
        MetricFlowQuerySpec(
            metric_specs=(MetricSpec(element_name="bookings_per_listing"),),
        )
    )

    convert_and_check(
        request=request,
        mf_test_session_state=mf_test_session_state,
        dataflow_to_sql_converter=dataflow_to_sql_converter,
        sql_client=sql_client,
        node=dataflow_plan.sink_output_nodes[0].parent_node,
    )


@pytest.mark.sql_engine_snapshot
def test_common_semantic_model(  # noqa: D
    request: FixtureRequest,
    mf_test_session_state: MetricFlowTestSessionState,
    dataflow_plan_builder: DataflowPlanBuilder,
    dataflow_to_sql_converter: DataflowToSqlQueryPlanConverter,
    sql_client: SqlClient,
) -> None:
    dataflow_plan = dataflow_plan_builder.build_plan(
        MetricFlowQuerySpec(
            metric_specs=(MetricSpec(element_name="bookings"), MetricSpec(element_name="booking_value")),
            dimension_specs=(DataSet.metric_time_dimension_spec(TimeGranularity.DAY),),
        ),
    )

    convert_and_check(
        request=request,
        mf_test_session_state=mf_test_session_state,
        dataflow_to_sql_converter=dataflow_to_sql_converter,
        sql_client=sql_client,
        node=dataflow_plan.sink_output_nodes[0].parent_node,
    )
