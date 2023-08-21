from __future__ import annotations

import logging

import pytest
from _pytest.fixtures import FixtureRequest
from dbt_semantic_interfaces.implementations.filters.where_filter import PydanticWhereFilter
from dbt_semantic_interfaces.type_enums.time_granularity import TimeGranularity

from metricflow.dataflow.builder.dataflow_plan_builder import DataflowPlanBuilder
from metricflow.dataflow.dataflow_plan_to_text import dataflow_plan_as_text
from metricflow.dataset.dataset import DataSet
from metricflow.errors.errors import UnableToSatisfyQueryError
from metricflow.specs.column_assoc import ColumnAssociationResolver
from metricflow.specs.specs import (
    DimensionSpec,
    EntityReference,
    MetricFlowQuerySpec,
    MetricSpec,
    OrderBySpec,
)
from metricflow.specs.where_filter_transform import WhereSpecFactory
from metricflow.test.dataflow_plan_to_svg import display_graph_if_requested
from metricflow.test.fixtures.setup_fixtures import MetricFlowTestSessionState
from metricflow.test.snapshot_utils import assert_plan_snapshot_text_equal
from metricflow.test.time.metric_time_dimension import MTD_SPEC_DAY, MTD_SPEC_MONTH, MTD_SPEC_QUARTER

logger = logging.getLogger(__name__)


def test_simple_plan(  # noqa: D
    request: FixtureRequest,
    mf_test_session_state: MetricFlowTestSessionState,
    dataflow_plan_builder: DataflowPlanBuilder,
) -> None:
    """Tests a simple plan getting a metric and a local dimension."""
    dataflow_plan = dataflow_plan_builder.build_plan(
        MetricFlowQuerySpec(
            metric_specs=(MetricSpec(element_name="bookings"),),
            dimension_specs=(
                DimensionSpec(
                    element_name="is_instant",
                    entity_links=(EntityReference("booking"),),
                ),
            ),
        )
    )

    assert_plan_snapshot_text_equal(
        request=request,
        mf_test_session_state=mf_test_session_state,
        plan=dataflow_plan,
        plan_snapshot_text=dataflow_plan_as_text(dataflow_plan),
    )

    display_graph_if_requested(
        request=request,
        mf_test_session_state=mf_test_session_state,
        dag_graph=dataflow_plan,
    )


def test_primary_entity_dimension(  # noqa: D
    request: FixtureRequest,
    mf_test_session_state: MetricFlowTestSessionState,
    dataflow_plan_builder: DataflowPlanBuilder,
) -> None:
    """Tests a simple plan getting a metric and a local dimension."""
    dataflow_plan = dataflow_plan_builder.build_plan(
        MetricFlowQuerySpec(
            metric_specs=(MetricSpec(element_name="bookings"),),
            dimension_specs=(
                DimensionSpec(
                    element_name="is_instant",
                    entity_links=(EntityReference(element_name="booking"),),
                ),
            ),
        )
    )

    assert_plan_snapshot_text_equal(
        request=request,
        mf_test_session_state=mf_test_session_state,
        plan=dataflow_plan,
        plan_snapshot_text=dataflow_plan_as_text(dataflow_plan),
    )

    display_graph_if_requested(
        request=request,
        mf_test_session_state=mf_test_session_state,
        dag_graph=dataflow_plan,
    )


def test_joined_plan(  # noqa: D
    request: FixtureRequest,
    mf_test_session_state: MetricFlowTestSessionState,
    column_association_resolver: ColumnAssociationResolver,
    dataflow_plan_builder: DataflowPlanBuilder,
) -> None:
    """Tests a plan getting a measure and a joined dimension."""
    dataflow_plan = dataflow_plan_builder.build_plan(
        MetricFlowQuerySpec(
            metric_specs=(MetricSpec(element_name="bookings"),),
            dimension_specs=(
                DimensionSpec(
                    element_name="is_instant",
                    entity_links=(EntityReference("booking"),),
                ),
                DimensionSpec(
                    element_name="country_latest",
                    entity_links=(EntityReference(element_name="listing"),),
                ),
            ),
        )
    )

    assert_plan_snapshot_text_equal(
        request=request,
        mf_test_session_state=mf_test_session_state,
        plan=dataflow_plan,
        plan_snapshot_text=dataflow_plan_as_text(dataflow_plan),
    )

    display_graph_if_requested(
        request=request,
        mf_test_session_state=mf_test_session_state,
        dag_graph=dataflow_plan,
    )


def test_order_by_plan(  # noqa: D
    request: FixtureRequest,
    mf_test_session_state: MetricFlowTestSessionState,
    dataflow_plan_builder: DataflowPlanBuilder,
) -> None:
    """Tests a plan with an order by."""
    dataflow_plan = dataflow_plan_builder.build_plan(
        MetricFlowQuerySpec(
            metric_specs=(MetricSpec(element_name="bookings"),),
            time_dimension_specs=(MTD_SPEC_DAY,),
            order_by_specs=(
                OrderBySpec(
                    instance_spec=MTD_SPEC_DAY,
                    descending=False,
                ),
                OrderBySpec(
                    instance_spec=MetricSpec(element_name="bookings"),
                    descending=True,
                ),
            ),
        )
    )

    assert_plan_snapshot_text_equal(
        request=request,
        mf_test_session_state=mf_test_session_state,
        plan=dataflow_plan,
        plan_snapshot_text=dataflow_plan_as_text(dataflow_plan),
    )

    display_graph_if_requested(
        request=request,
        mf_test_session_state=mf_test_session_state,
        dag_graph=dataflow_plan,
    )


def test_limit_rows_plan(  # noqa: D
    request: FixtureRequest,
    mf_test_session_state: MetricFlowTestSessionState,
    dataflow_plan_builder: DataflowPlanBuilder,
) -> None:
    """Tests a plan with a limit to the number of rows returned."""
    dataflow_plan = dataflow_plan_builder.build_plan(
        MetricFlowQuerySpec(
            metric_specs=(MetricSpec(element_name="bookings"),),
            time_dimension_specs=(MTD_SPEC_DAY,),
            limit=1,
        )
    )

    assert_plan_snapshot_text_equal(
        request=request,
        mf_test_session_state=mf_test_session_state,
        plan=dataflow_plan,
        plan_snapshot_text=dataflow_plan_as_text(dataflow_plan),
    )

    display_graph_if_requested(
        request=request,
        mf_test_session_state=mf_test_session_state,
        dag_graph=dataflow_plan,
    )


def test_multiple_metrics_plan(  # noqa: D
    request: FixtureRequest,
    mf_test_session_state: MetricFlowTestSessionState,
    dataflow_plan_builder: DataflowPlanBuilder,
) -> None:
    """Tests a plan to retrieve multiple metrics."""
    dataflow_plan = dataflow_plan_builder.build_plan(
        MetricFlowQuerySpec(
            metric_specs=(MetricSpec(element_name="bookings"), MetricSpec(element_name="booking_value")),
            dimension_specs=(
                DimensionSpec(
                    element_name="is_instant",
                    entity_links=(EntityReference("booking"),),
                ),
            ),
            time_dimension_specs=(MTD_SPEC_DAY,),
        )
    )

    assert_plan_snapshot_text_equal(
        request=request,
        mf_test_session_state=mf_test_session_state,
        plan=dataflow_plan,
        plan_snapshot_text=dataflow_plan_as_text(dataflow_plan),
    )

    display_graph_if_requested(
        request=request,
        mf_test_session_state=mf_test_session_state,
        dag_graph=dataflow_plan,
    )


def test_single_semantic_model_ratio_metrics_plan(
    request: FixtureRequest,
    mf_test_session_state: MetricFlowTestSessionState,
    dataflow_plan_builder: DataflowPlanBuilder,
) -> None:
    """Tests a plan to retrieve a ratio where both measures come from one semantic model."""
    dataflow_plan = dataflow_plan_builder.build_plan(
        MetricFlowQuerySpec(
            metric_specs=(MetricSpec(element_name="bookings_per_booker"),),
            dimension_specs=(
                DimensionSpec(
                    element_name="country_latest",
                    entity_links=(EntityReference(element_name="listing"),),
                ),
            ),
            time_dimension_specs=(MTD_SPEC_DAY,),
        )
    )

    assert_plan_snapshot_text_equal(
        request=request,
        mf_test_session_state=mf_test_session_state,
        plan=dataflow_plan,
        plan_snapshot_text=dataflow_plan_as_text(dataflow_plan),
    )

    display_graph_if_requested(
        request=request,
        mf_test_session_state=mf_test_session_state,
        dag_graph=dataflow_plan,
    )


def test_multi_semantic_model_ratio_metrics_plan(
    request: FixtureRequest,
    mf_test_session_state: MetricFlowTestSessionState,
    dataflow_plan_builder: DataflowPlanBuilder,
) -> None:
    """Tests a plan to retrieve a ratio where both measures come from one semantic model."""
    dataflow_plan = dataflow_plan_builder.build_plan(
        MetricFlowQuerySpec(
            metric_specs=(MetricSpec(element_name="bookings_per_view"),),
            dimension_specs=(
                DimensionSpec(
                    element_name="country_latest",
                    entity_links=(EntityReference(element_name="listing"),),
                ),
            ),
            time_dimension_specs=(MTD_SPEC_DAY,),
        )
    )

    assert_plan_snapshot_text_equal(
        request=request,
        mf_test_session_state=mf_test_session_state,
        plan=dataflow_plan,
        plan_snapshot_text=dataflow_plan_as_text(dataflow_plan),
    )

    display_graph_if_requested(
        request=request,
        mf_test_session_state=mf_test_session_state,
        dag_graph=dataflow_plan,
    )


def test_multihop_join_plan(  # noqa: D
    request: FixtureRequest,
    mf_test_session_state: MetricFlowTestSessionState,
    multihop_dataflow_plan_builder: DataflowPlanBuilder,
) -> None:
    """Tests a plan with an order by."""
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

    assert_plan_snapshot_text_equal(
        request=request,
        mf_test_session_state=mf_test_session_state,
        plan=dataflow_plan,
        plan_snapshot_text=dataflow_plan_as_text(dataflow_plan),
    )

    display_graph_if_requested(
        request=request,
        mf_test_session_state=mf_test_session_state,
        dag_graph=dataflow_plan,
    )


def test_where_constrained_plan(  # noqa: D
    request: FixtureRequest,
    mf_test_session_state: MetricFlowTestSessionState,
    column_association_resolver: ColumnAssociationResolver,
    dataflow_plan_builder: DataflowPlanBuilder,
) -> None:
    """Tests a simple plan getting a metric and a local dimension."""
    dataflow_plan = dataflow_plan_builder.build_plan(
        MetricFlowQuerySpec(
            metric_specs=(MetricSpec(element_name="bookings"),),
            dimension_specs=(
                DimensionSpec(
                    element_name="is_instant",
                    entity_links=(EntityReference("booking"),),
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

    assert_plan_snapshot_text_equal(
        request=request,
        mf_test_session_state=mf_test_session_state,
        plan=dataflow_plan,
        plan_snapshot_text=dataflow_plan_as_text(dataflow_plan),
    )

    display_graph_if_requested(
        request=request,
        mf_test_session_state=mf_test_session_state,
        dag_graph=dataflow_plan,
    )


def test_where_constrained_plan_time_dimension(  # noqa: D
    request: FixtureRequest,
    mf_test_session_state: MetricFlowTestSessionState,
    dataflow_plan_builder: DataflowPlanBuilder,
    column_association_resolver: ColumnAssociationResolver,
) -> None:
    """Tests a simple plan getting a metric and a local dimension."""
    dataflow_plan = dataflow_plan_builder.build_plan(
        MetricFlowQuerySpec(
            metric_specs=(MetricSpec(element_name="bookings"),),
            dimension_specs=(
                DimensionSpec(
                    element_name="is_instant",
                    entity_links=(EntityReference("booking"),),
                ),
            ),
            where_constraint=(
                WhereSpecFactory(
                    column_association_resolver=column_association_resolver,
                ).create_from_where_filter(
                    PydanticWhereFilter(
                        where_sql_template="{{ TimeDimension('metric_time', 'day') }} >= '2020-01-01'",
                    )
                )
            ),
        )
    )

    assert_plan_snapshot_text_equal(
        request=request,
        mf_test_session_state=mf_test_session_state,
        plan=dataflow_plan,
        plan_snapshot_text=dataflow_plan_as_text(dataflow_plan),
    )

    display_graph_if_requested(
        request=request,
        mf_test_session_state=mf_test_session_state,
        dag_graph=dataflow_plan,
    )


def test_where_constrained_with_common_linkable_plan(  # noqa: D
    request: FixtureRequest,
    mf_test_session_state: MetricFlowTestSessionState,
    column_association_resolver: ColumnAssociationResolver,
    dataflow_plan_builder: DataflowPlanBuilder,
) -> None:
    """Tests a dataflow plan where the where clause has a common linkable with the query."""
    dataflow_plan = dataflow_plan_builder.build_plan(
        MetricFlowQuerySpec(
            metric_specs=(MetricSpec(element_name="bookings"),),
            dimension_specs=(
                DimensionSpec(
                    element_name="country_latest",
                    entity_links=(EntityReference(element_name="listing"),),
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

    assert_plan_snapshot_text_equal(
        request=request,
        mf_test_session_state=mf_test_session_state,
        plan=dataflow_plan,
        plan_snapshot_text=dataflow_plan_as_text(dataflow_plan),
    )

    display_graph_if_requested(
        request=request,
        mf_test_session_state=mf_test_session_state,
        dag_graph=dataflow_plan,
    )


def test_multihop_join_plan_ambiguous_dim(  # noqa: D
    mf_test_session_state: MetricFlowTestSessionState,
    dataflow_plan_builder: DataflowPlanBuilder,
) -> None:
    """Checks that an exception is thrown when trying to build a plan with an ambiguous dimension."""
    with pytest.raises(UnableToSatisfyQueryError):
        dataflow_plan_builder.build_plan(
            MetricFlowQuerySpec(
                metric_specs=(MetricSpec(element_name="views"),),
                dimension_specs=(
                    DimensionSpec(
                        element_name="home_country",
                        entity_links=(
                            EntityReference(element_name="listing"),
                            EntityReference(element_name="user"),
                        ),
                    ),
                ),
            )
        )


def test_cumulative_metric_with_window(  # noqa: D
    request: FixtureRequest,
    mf_test_session_state: MetricFlowTestSessionState,
    dataflow_plan_builder: DataflowPlanBuilder,
) -> None:
    """Tests a plan to compute a cumulative metric."""
    dataflow_plan = dataflow_plan_builder.build_plan(
        MetricFlowQuerySpec(
            metric_specs=(MetricSpec(element_name="trailing_2_months_revenue"),),
            dimension_specs=(),
            time_dimension_specs=(MTD_SPEC_MONTH,),
        )
    )

    assert_plan_snapshot_text_equal(
        request=request,
        mf_test_session_state=mf_test_session_state,
        plan=dataflow_plan,
        plan_snapshot_text=dataflow_plan_as_text(dataflow_plan),
    )

    display_graph_if_requested(
        request=request,
        mf_test_session_state=mf_test_session_state,
        dag_graph=dataflow_plan,
    )


def test_cumulative_metric_no_window_or_grain_with_metric_time(  # noqa: D
    request: FixtureRequest,
    mf_test_session_state: MetricFlowTestSessionState,
    dataflow_plan_builder: DataflowPlanBuilder,
) -> None:
    dataflow_plan = dataflow_plan_builder.build_plan(
        MetricFlowQuerySpec(
            metric_specs=(MetricSpec(element_name="revenue_all_time"),),
            dimension_specs=(),
            time_dimension_specs=(MTD_SPEC_QUARTER,),
        )
    )

    assert_plan_snapshot_text_equal(
        request=request,
        mf_test_session_state=mf_test_session_state,
        plan=dataflow_plan,
        plan_snapshot_text=dataflow_plan_as_text(dataflow_plan),
    )

    display_graph_if_requested(
        request=request,
        mf_test_session_state=mf_test_session_state,
        dag_graph=dataflow_plan,
    )


def test_cumulative_metric_no_window_or_grain_without_metric_time(  # noqa: D
    request: FixtureRequest,
    mf_test_session_state: MetricFlowTestSessionState,
    dataflow_plan_builder: DataflowPlanBuilder,
) -> None:
    dataflow_plan = dataflow_plan_builder.build_plan(
        MetricFlowQuerySpec(
            metric_specs=(MetricSpec(element_name="revenue_all_time"),),
            dimension_specs=(),
            time_dimension_specs=(),
        )
    )

    assert_plan_snapshot_text_equal(
        request=request,
        mf_test_session_state=mf_test_session_state,
        plan=dataflow_plan,
        plan_snapshot_text=dataflow_plan_as_text(dataflow_plan),
    )

    display_graph_if_requested(
        request=request,
        mf_test_session_state=mf_test_session_state,
        dag_graph=dataflow_plan,
    )


def test_distinct_values_plan(  # noqa: D
    request: FixtureRequest,
    mf_test_session_state: MetricFlowTestSessionState,
    dataflow_plan_builder: DataflowPlanBuilder,
) -> None:
    """Tests a plan to get distinct values of a dimension."""
    dataflow_plan = dataflow_plan_builder.build_plan_for_distinct_values(
        metric_specs=(MetricSpec(element_name="bookings"),),
        dimension_spec=DimensionSpec(
            element_name="country_latest",
            entity_links=(EntityReference(element_name="listing"),),
        ),
        limit=100,
    )

    assert_plan_snapshot_text_equal(
        request=request,
        mf_test_session_state=mf_test_session_state,
        plan=dataflow_plan,
        plan_snapshot_text=dataflow_plan_as_text(dataflow_plan),
    )

    display_graph_if_requested(
        request=request,
        mf_test_session_state=mf_test_session_state,
        dag_graph=dataflow_plan,
    )


def test_measure_constraint_plan(
    request: FixtureRequest,
    mf_test_session_state: MetricFlowTestSessionState,
    dataflow_plan_builder: DataflowPlanBuilder,
) -> None:
    """Tests a plan for querying a metric with a constraint on one or more of its input measures."""
    dataflow_plan = dataflow_plan_builder.build_plan(
        MetricFlowQuerySpec(
            metric_specs=(MetricSpec(element_name="lux_booking_value_rate_expr"),),
            dimension_specs=(),
            time_dimension_specs=(MTD_SPEC_DAY,),
        ),
    )

    assert_plan_snapshot_text_equal(
        request=request,
        mf_test_session_state=mf_test_session_state,
        plan=dataflow_plan,
        plan_snapshot_text=dataflow_plan_as_text(dataflow_plan),
    )

    display_graph_if_requested(
        request=request,
        mf_test_session_state=mf_test_session_state,
        dag_graph=dataflow_plan,
    )


def test_measure_constraint_with_reused_measure_plan(
    request: FixtureRequest,
    mf_test_session_state: MetricFlowTestSessionState,
    dataflow_plan_builder: DataflowPlanBuilder,
) -> None:
    """Tests a plan for querying a metric with a constraint on one or more of its input measures."""
    dataflow_plan = dataflow_plan_builder.build_plan(
        MetricFlowQuerySpec(
            metric_specs=(MetricSpec(element_name="instant_booking_value_ratio"),),
            dimension_specs=(),
            time_dimension_specs=(MTD_SPEC_DAY,),
        ),
    )

    assert_plan_snapshot_text_equal(
        request=request,
        mf_test_session_state=mf_test_session_state,
        plan=dataflow_plan,
        plan_snapshot_text=dataflow_plan_as_text(dataflow_plan),
    )

    display_graph_if_requested(
        request=request,
        mf_test_session_state=mf_test_session_state,
        dag_graph=dataflow_plan,
    )


def test_common_semantic_model(  # noqa: D
    request: FixtureRequest,
    mf_test_session_state: MetricFlowTestSessionState,
    dataflow_plan_builder: DataflowPlanBuilder,
) -> None:
    """Tests a simple plan getting a metric and a local dimension."""
    dataflow_plan = dataflow_plan_builder.build_plan(
        MetricFlowQuerySpec(
            metric_specs=(MetricSpec(element_name="bookings"), MetricSpec(element_name="booking_value")),
            dimension_specs=(
                DataSet.metric_time_dimension_spec(TimeGranularity.DAY),
                DimensionSpec(element_name="country_latest", entity_links=(EntityReference("listing"),)),
            ),
        )
    )

    assert_plan_snapshot_text_equal(
        request=request,
        mf_test_session_state=mf_test_session_state,
        plan=dataflow_plan,
        plan_snapshot_text=dataflow_plan_as_text(dataflow_plan),
    )

    display_graph_if_requested(
        request=request,
        mf_test_session_state=mf_test_session_state,
        dag_graph=dataflow_plan,
    )


def test_derived_metric_offset_window(  # noqa: D
    request: FixtureRequest,
    mf_test_session_state: MetricFlowTestSessionState,
    dataflow_plan_builder: DataflowPlanBuilder,
) -> None:
    """Tests a simple plan getting a metric and a local dimension."""
    dataflow_plan = dataflow_plan_builder.build_plan(
        MetricFlowQuerySpec(
            metric_specs=(MetricSpec(element_name="bookings_5_day_lag"),),
            time_dimension_specs=(DataSet.metric_time_dimension_spec(TimeGranularity.DAY),),
        )
    )

    assert_plan_snapshot_text_equal(
        request=request,
        mf_test_session_state=mf_test_session_state,
        plan=dataflow_plan,
        plan_snapshot_text=dataflow_plan_as_text(dataflow_plan),
    )

    display_graph_if_requested(
        request=request,
        mf_test_session_state=mf_test_session_state,
        dag_graph=dataflow_plan,
    )


def test_derived_metric_offset_to_grain(  # noqa: D
    request: FixtureRequest,
    mf_test_session_state: MetricFlowTestSessionState,
    dataflow_plan_builder: DataflowPlanBuilder,
) -> None:
    """Tests a simple plan getting a metric and a local dimension."""
    dataflow_plan = dataflow_plan_builder.build_plan(
        MetricFlowQuerySpec(
            metric_specs=(MetricSpec(element_name="bookings_growth_since_start_of_month"),),
            time_dimension_specs=(DataSet.metric_time_dimension_spec(TimeGranularity.DAY),),
        )
    )

    assert_plan_snapshot_text_equal(
        request=request,
        mf_test_session_state=mf_test_session_state,
        plan=dataflow_plan,
        plan_snapshot_text=dataflow_plan_as_text(dataflow_plan),
    )

    display_graph_if_requested(
        request=request,
        mf_test_session_state=mf_test_session_state,
        dag_graph=dataflow_plan,
    )


def test_derived_metric_offset_with_granularity(  # noqa: D
    request: FixtureRequest,
    mf_test_session_state: MetricFlowTestSessionState,
    dataflow_plan_builder: DataflowPlanBuilder,
) -> None:
    dataflow_plan = dataflow_plan_builder.build_plan(
        MetricFlowQuerySpec(
            metric_specs=(MetricSpec(element_name="bookings_5_day_lag"),),
            time_dimension_specs=(DataSet.metric_time_dimension_spec(TimeGranularity.MONTH),),
        )
    )

    assert_plan_snapshot_text_equal(
        request=request,
        mf_test_session_state=mf_test_session_state,
        plan=dataflow_plan,
        plan_snapshot_text=dataflow_plan_as_text(dataflow_plan),
    )

    display_graph_if_requested(
        request=request,
        mf_test_session_state=mf_test_session_state,
        dag_graph=dataflow_plan,
    )


def test_derived_offset_cumulative_metric(  # noqa: D
    request: FixtureRequest,
    mf_test_session_state: MetricFlowTestSessionState,
    dataflow_plan_builder: DataflowPlanBuilder,
) -> None:
    dataflow_plan = dataflow_plan_builder.build_plan(
        MetricFlowQuerySpec(
            metric_specs=(MetricSpec(element_name="every_2_days_bookers_2_days_ago"),),
            time_dimension_specs=(DataSet.metric_time_dimension_spec(TimeGranularity.DAY),),
        )
    )

    assert_plan_snapshot_text_equal(
        request=request,
        mf_test_session_state=mf_test_session_state,
        plan=dataflow_plan,
        plan_snapshot_text=dataflow_plan_as_text(dataflow_plan),
    )

    display_graph_if_requested(
        request=request,
        mf_test_session_state=mf_test_session_state,
        dag_graph=dataflow_plan,
    )
