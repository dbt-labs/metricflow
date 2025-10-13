from __future__ import annotations

import datetime
import logging
import string
from collections.abc import Mapping

import pytest
from _pytest.fixtures import FixtureRequest
from dbt_semantic_interfaces.implementations.filters.where_filter import PydanticWhereFilter
from dbt_semantic_interfaces.naming.keywords import METRIC_TIME_ELEMENT_NAME
from dbt_semantic_interfaces.references import EntityReference, MetricReference
from dbt_semantic_interfaces.type_enums.time_granularity import TimeGranularity
from metricflow_semantics.errors.error_classes import UnableToSatisfyQueryError
from metricflow_semantics.filters.time_constraint import TimeRangeConstraint
from metricflow_semantics.model.linkable_element_property import GroupByItemProperty
from metricflow_semantics.model.semantics.element_filter import GroupByItemSetFilter
from metricflow_semantics.query.query_parser import MetricFlowQueryParser
from metricflow_semantics.specs.column_assoc import ColumnAssociationResolver
from metricflow_semantics.specs.dimension_spec import DimensionSpec
from metricflow_semantics.specs.metric_spec import MetricSpec
from metricflow_semantics.specs.order_by_spec import OrderBySpec
from metricflow_semantics.specs.query_spec import MetricFlowQuerySpec
from metricflow_semantics.specs.spec_set import group_specs_by_type
from metricflow_semantics.specs.time_dimension_spec import TimeDimensionSpec
from metricflow_semantics.test_helpers.config_helpers import MetricFlowTestConfiguration
from metricflow_semantics.test_helpers.metric_time_dimension import (
    MTD_SPEC_DAY,
    MTD_SPEC_MONTH,
    MTD_SPEC_QUARTER,
    MTD_SPEC_WEEK,
)
from metricflow_semantics.test_helpers.snapshot_helpers import assert_plan_snapshot_text_equal
from metricflow_semantics.time.granularity import ExpandedTimeGranularity

from metricflow.dataflow.builder.dataflow_plan_builder import DataflowPlanBuilder
from tests_metricflow.dataflow_plan_to_svg import display_graph_if_requested
from tests_metricflow.fixtures.manifest_fixtures import MetricFlowEngineTestFixture, SemanticManifestSetup

logger = logging.getLogger(__name__)


@pytest.mark.sql_engine_snapshot
def test_simple_plan(
    request: FixtureRequest,
    mf_test_configuration: MetricFlowTestConfiguration,
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
        snapshot_configuration=mf_test_configuration,
        plan=dataflow_plan,
        plan_snapshot_text=dataflow_plan.structure_text(),
    )

    display_graph_if_requested(
        request=request,
        mf_test_configuration=mf_test_configuration,
        dag_graph=dataflow_plan,
    )


@pytest.mark.sql_engine_snapshot
def test_primary_entity_dimension(
    request: FixtureRequest,
    mf_test_configuration: MetricFlowTestConfiguration,
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
        snapshot_configuration=mf_test_configuration,
        plan=dataflow_plan,
        plan_snapshot_text=dataflow_plan.structure_text(),
    )

    display_graph_if_requested(
        request=request,
        mf_test_configuration=mf_test_configuration,
        dag_graph=dataflow_plan,
    )


@pytest.mark.sql_engine_snapshot
def test_joined_plan(
    request: FixtureRequest,
    mf_test_configuration: MetricFlowTestConfiguration,
    dataflow_plan_builder: DataflowPlanBuilder,
) -> None:
    """Tests a plan getting a simple-metric input and a joined dimension."""
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
        snapshot_configuration=mf_test_configuration,
        plan=dataflow_plan,
        plan_snapshot_text=dataflow_plan.structure_text(),
    )

    display_graph_if_requested(
        request=request,
        mf_test_configuration=mf_test_configuration,
        dag_graph=dataflow_plan,
    )


@pytest.mark.sql_engine_snapshot
def test_order_by_plan(
    request: FixtureRequest,
    mf_test_configuration: MetricFlowTestConfiguration,
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
        snapshot_configuration=mf_test_configuration,
        plan=dataflow_plan,
        plan_snapshot_text=dataflow_plan.structure_text(),
    )

    display_graph_if_requested(
        request=request,
        mf_test_configuration=mf_test_configuration,
        dag_graph=dataflow_plan,
    )


@pytest.mark.sql_engine_snapshot
def test_limit_rows_plan(
    request: FixtureRequest,
    mf_test_configuration: MetricFlowTestConfiguration,
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
        snapshot_configuration=mf_test_configuration,
        plan=dataflow_plan,
        plan_snapshot_text=dataflow_plan.structure_text(),
    )

    display_graph_if_requested(
        request=request,
        mf_test_configuration=mf_test_configuration,
        dag_graph=dataflow_plan,
    )


@pytest.mark.sql_engine_snapshot
def test_multiple_metrics_plan(
    request: FixtureRequest,
    mf_test_configuration: MetricFlowTestConfiguration,
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
        snapshot_configuration=mf_test_configuration,
        plan=dataflow_plan,
        plan_snapshot_text=dataflow_plan.structure_text(),
    )

    display_graph_if_requested(
        request=request,
        mf_test_configuration=mf_test_configuration,
        dag_graph=dataflow_plan,
    )


@pytest.mark.sql_engine_snapshot
def test_single_semantic_model_ratio_metrics_plan(
    request: FixtureRequest,
    mf_test_configuration: MetricFlowTestConfiguration,
    dataflow_plan_builder: DataflowPlanBuilder,
) -> None:
    """Tests a plan to retrieve a ratio where both simple-metric inputs come from one semantic model."""
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
        snapshot_configuration=mf_test_configuration,
        plan=dataflow_plan,
        plan_snapshot_text=dataflow_plan.structure_text(),
    )

    display_graph_if_requested(
        request=request,
        mf_test_configuration=mf_test_configuration,
        dag_graph=dataflow_plan,
    )


@pytest.mark.sql_engine_snapshot
def test_multi_semantic_model_ratio_metrics_plan(
    request: FixtureRequest,
    mf_test_configuration: MetricFlowTestConfiguration,
    dataflow_plan_builder: DataflowPlanBuilder,
) -> None:
    """Tests a plan to retrieve a ratio where both simple-metric inputs come from one semantic model."""
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
        snapshot_configuration=mf_test_configuration,
        plan=dataflow_plan,
        plan_snapshot_text=dataflow_plan.structure_text(),
    )

    display_graph_if_requested(
        request=request,
        mf_test_configuration=mf_test_configuration,
        dag_graph=dataflow_plan,
    )


@pytest.mark.sql_engine_snapshot
def test_multihop_join_plan(
    request: FixtureRequest,
    mf_test_configuration: MetricFlowTestConfiguration,
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
        snapshot_configuration=mf_test_configuration,
        plan=dataflow_plan,
        plan_snapshot_text=dataflow_plan.structure_text(),
    )

    display_graph_if_requested(
        request=request,
        mf_test_configuration=mf_test_configuration,
        dag_graph=dataflow_plan,
    )


@pytest.mark.sql_engine_snapshot
def test_where_constrained_plan(
    request: FixtureRequest,
    mf_test_configuration: MetricFlowTestConfiguration,
    column_association_resolver: ColumnAssociationResolver,
    dataflow_plan_builder: DataflowPlanBuilder,
    query_parser: MetricFlowQueryParser,
) -> None:
    """Tests a simple plan getting a metric and a local dimension."""
    query_spec = query_parser.parse_and_validate_query(
        metric_names=("bookings",),
        group_by_names=("booking__is_instant",),
        where_constraint_strs=["{{ Dimension('listing__country_latest') }} = 'us'"],
    ).query_spec
    dataflow_plan = dataflow_plan_builder.build_plan(query_spec)

    assert_plan_snapshot_text_equal(
        request=request,
        snapshot_configuration=mf_test_configuration,
        plan=dataflow_plan,
        plan_snapshot_text=dataflow_plan.structure_text(),
    )

    display_graph_if_requested(
        request=request,
        mf_test_configuration=mf_test_configuration,
        dag_graph=dataflow_plan,
    )


@pytest.mark.sql_engine_snapshot
def test_where_constrained_plan_time_dimension(
    request: FixtureRequest,
    mf_test_configuration: MetricFlowTestConfiguration,
    dataflow_plan_builder: DataflowPlanBuilder,
    query_parser: MetricFlowQueryParser,
) -> None:
    """Tests a simple plan getting a metric and a local dimension."""
    query_spec = query_parser.parse_and_validate_query(
        metric_names=("bookings",),
        group_by_names=("booking__is_instant",),
        where_constraint_strs=["{{ TimeDimension('metric_time', 'day') }} >= '2020-01-01'"],
    ).query_spec
    dataflow_plan = dataflow_plan_builder.build_plan(query_spec)

    assert_plan_snapshot_text_equal(
        request=request,
        snapshot_configuration=mf_test_configuration,
        plan=dataflow_plan,
        plan_snapshot_text=dataflow_plan.structure_text(),
    )

    display_graph_if_requested(
        request=request,
        mf_test_configuration=mf_test_configuration,
        dag_graph=dataflow_plan,
    )


@pytest.mark.sql_engine_snapshot
def test_where_constrained_with_common_linkable_plan(
    request: FixtureRequest,
    mf_test_configuration: MetricFlowTestConfiguration,
    column_association_resolver: ColumnAssociationResolver,
    dataflow_plan_builder: DataflowPlanBuilder,
    query_parser: MetricFlowQueryParser,
) -> None:
    """Tests a dataflow plan where the where clause has a common linkable with the query."""
    query_spec = query_parser.parse_and_validate_query(
        metric_names=("bookings",),
        group_by_names=("listing__country_latest",),
        where_constraint_strs=["{{ Dimension('listing__country_latest') }} = 'us'"],
    ).query_spec
    dataflow_plan = dataflow_plan_builder.build_plan(query_spec)

    assert_plan_snapshot_text_equal(
        request=request,
        snapshot_configuration=mf_test_configuration,
        plan=dataflow_plan,
        plan_snapshot_text=dataflow_plan.structure_text(),
    )

    display_graph_if_requested(
        request=request,
        mf_test_configuration=mf_test_configuration,
        dag_graph=dataflow_plan,
    )


@pytest.mark.sql_engine_snapshot
def test_multihop_join_plan_ambiguous_dim(
    mf_test_configuration: MetricFlowTestConfiguration,
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


@pytest.mark.sql_engine_snapshot
def test_cumulative_metric_with_window(
    request: FixtureRequest,
    mf_test_configuration: MetricFlowTestConfiguration,
    dataflow_plan_builder: DataflowPlanBuilder,
) -> None:
    """Tests a plan to compute a cumulative metric."""
    dataflow_plan = dataflow_plan_builder.build_plan(
        MetricFlowQuerySpec(
            metric_specs=(MetricSpec(element_name="trailing_2_months_revenue"),),
            dimension_specs=(),
            time_dimension_specs=(MTD_SPEC_DAY,),
        )
    )

    assert_plan_snapshot_text_equal(
        request=request,
        snapshot_configuration=mf_test_configuration,
        plan=dataflow_plan,
        plan_snapshot_text=dataflow_plan.structure_text(),
    )

    display_graph_if_requested(
        request=request,
        mf_test_configuration=mf_test_configuration,
        dag_graph=dataflow_plan,
    )


@pytest.mark.sql_engine_snapshot
def test_cumulative_metric_no_window_or_grain_with_metric_time(  # noqa: D103
    request: FixtureRequest,
    mf_test_configuration: MetricFlowTestConfiguration,
    dataflow_plan_builder: DataflowPlanBuilder,
) -> None:
    dataflow_plan = dataflow_plan_builder.build_plan(
        MetricFlowQuerySpec(
            metric_specs=(MetricSpec(element_name="revenue_all_time"),),
            dimension_specs=(),
            time_dimension_specs=(MTD_SPEC_DAY,),
        )
    )

    assert_plan_snapshot_text_equal(
        request=request,
        snapshot_configuration=mf_test_configuration,
        plan=dataflow_plan,
        plan_snapshot_text=dataflow_plan.structure_text(),
    )

    display_graph_if_requested(
        request=request,
        mf_test_configuration=mf_test_configuration,
        dag_graph=dataflow_plan,
    )


@pytest.mark.sql_engine_snapshot
def test_cumulative_metric_no_window_or_grain_without_metric_time(  # noqa: D103
    request: FixtureRequest,
    mf_test_configuration: MetricFlowTestConfiguration,
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
        snapshot_configuration=mf_test_configuration,
        plan=dataflow_plan,
        plan_snapshot_text=dataflow_plan.structure_text(),
    )

    display_graph_if_requested(
        request=request,
        mf_test_configuration=mf_test_configuration,
        dag_graph=dataflow_plan,
    )


@pytest.mark.sql_engine_snapshot
def test_distinct_values_plan(
    request: FixtureRequest,
    mf_test_configuration: MetricFlowTestConfiguration,
    dataflow_plan_builder: DataflowPlanBuilder,
    query_parser: MetricFlowQueryParser,
) -> None:
    """Tests a plan to get distinct values of a dimension."""
    query_spec = query_parser.parse_and_validate_query(
        metric_names=(),
        group_by_names=("listing__country_latest",),
        where_constraint_strs=["{{ Dimension('listing__country_latest') }} = 'us'"],
        order_by_names=("-listing__country_latest",),
        limit=100,
    ).query_spec
    dataflow_plan = dataflow_plan_builder.build_plan_for_distinct_values(query_spec)

    assert_plan_snapshot_text_equal(
        request=request,
        snapshot_configuration=mf_test_configuration,
        plan=dataflow_plan,
        plan_snapshot_text=dataflow_plan.structure_text(),
    )

    display_graph_if_requested(
        request=request,
        mf_test_configuration=mf_test_configuration,
        dag_graph=dataflow_plan,
    )


@pytest.mark.sql_engine_snapshot
def test_distinct_values_plan_with_join(
    request: FixtureRequest,
    mf_test_configuration: MetricFlowTestConfiguration,
    dataflow_plan_builder: DataflowPlanBuilder,
    query_parser: MetricFlowQueryParser,
) -> None:
    """Tests a plan to get distinct values of 2 dimensions, where a join is required."""
    query_spec = query_parser.parse_and_validate_query(
        group_by_names=("user__home_state_latest", "listing__is_lux_latest"),
        where_constraint_strs=["{{ Dimension('listing__country_latest') }} = 'us'"],
        order_by_names=("-listing__is_lux_latest",),
        limit=100,
    ).query_spec
    dataflow_plan = dataflow_plan_builder.build_plan_for_distinct_values(query_spec)

    assert_plan_snapshot_text_equal(
        request=request,
        snapshot_configuration=mf_test_configuration,
        plan=dataflow_plan,
        plan_snapshot_text=dataflow_plan.structure_text(),
    )

    display_graph_if_requested(
        request=request,
        mf_test_configuration=mf_test_configuration,
        dag_graph=dataflow_plan,
    )


@pytest.mark.sql_engine_snapshot
def test_simple_metric_constraint_plan(
    request: FixtureRequest,
    mf_test_configuration: MetricFlowTestConfiguration,
    query_parser: MetricFlowQueryParser,
    dataflow_plan_builder: DataflowPlanBuilder,
) -> None:
    """Tests a plan for querying a simple metric with a constraint on its inputs."""
    query_spec = query_parser.parse_and_validate_query(
        metric_names=("lux_booking_value_rate_expr",),
        group_by_names=(METRIC_TIME_ELEMENT_NAME,),
    ).query_spec
    dataflow_plan = dataflow_plan_builder.build_plan(query_spec)

    assert_plan_snapshot_text_equal(
        request=request,
        snapshot_configuration=mf_test_configuration,
        plan=dataflow_plan,
        plan_snapshot_text=dataflow_plan.structure_text(),
    )

    display_graph_if_requested(
        request=request,
        mf_test_configuration=mf_test_configuration,
        dag_graph=dataflow_plan,
    )


@pytest.mark.sql_engine_snapshot
def test_simple_metric_constraint_with_reused_simple_metric_plan(
    request: FixtureRequest,
    mf_test_configuration: MetricFlowTestConfiguration,
    query_parser: MetricFlowQueryParser,
    dataflow_plan_builder: DataflowPlanBuilder,
) -> None:
    """Tests a plan for querying a derived metric with a constraint on its inputs and where an input is reused."""
    query_spec = query_parser.parse_and_validate_query(
        metric_names=("instant_booking_value_ratio",),
        group_by_names=(METRIC_TIME_ELEMENT_NAME,),
    ).query_spec
    dataflow_plan = dataflow_plan_builder.build_plan(query_spec)

    assert_plan_snapshot_text_equal(
        request=request,
        snapshot_configuration=mf_test_configuration,
        plan=dataflow_plan,
        plan_snapshot_text=dataflow_plan.structure_text(),
    )

    display_graph_if_requested(
        request=request,
        mf_test_configuration=mf_test_configuration,
        dag_graph=dataflow_plan,
    )


@pytest.mark.sql_engine_snapshot
def test_common_semantic_model(
    request: FixtureRequest,
    mf_test_configuration: MetricFlowTestConfiguration,
    dataflow_plan_builder: DataflowPlanBuilder,
) -> None:
    """Tests a simple plan getting a metric and a local dimension."""
    dataflow_plan = dataflow_plan_builder.build_plan(
        MetricFlowQuerySpec(
            metric_specs=(MetricSpec(element_name="bookings"), MetricSpec(element_name="booking_value")),
            dimension_specs=(
                MTD_SPEC_DAY,
                DimensionSpec(element_name="country_latest", entity_links=(EntityReference("listing"),)),
            ),
        )
    )

    assert_plan_snapshot_text_equal(
        request=request,
        snapshot_configuration=mf_test_configuration,
        plan=dataflow_plan,
        plan_snapshot_text=dataflow_plan.structure_text(),
    )

    display_graph_if_requested(
        request=request,
        mf_test_configuration=mf_test_configuration,
        dag_graph=dataflow_plan,
    )


@pytest.mark.sql_engine_snapshot
def test_derived_metric_offset_window(
    request: FixtureRequest,
    mf_test_configuration: MetricFlowTestConfiguration,
    dataflow_plan_builder: DataflowPlanBuilder,
) -> None:
    """Tests a simple plan getting a metric and a local dimension."""
    dataflow_plan = dataflow_plan_builder.build_plan(
        MetricFlowQuerySpec(
            metric_specs=(MetricSpec(element_name="bookings_5_day_lag"),),
            time_dimension_specs=(MTD_SPEC_DAY,),
        )
    )

    assert_plan_snapshot_text_equal(
        request=request,
        snapshot_configuration=mf_test_configuration,
        plan=dataflow_plan,
        plan_snapshot_text=dataflow_plan.structure_text(),
    )

    display_graph_if_requested(
        request=request,
        mf_test_configuration=mf_test_configuration,
        dag_graph=dataflow_plan,
    )


@pytest.mark.sql_engine_snapshot
def test_derived_metric_offset_to_grain(
    request: FixtureRequest,
    mf_test_configuration: MetricFlowTestConfiguration,
    dataflow_plan_builder: DataflowPlanBuilder,
) -> None:
    """Tests a simple plan getting a metric and a local dimension."""
    dataflow_plan = dataflow_plan_builder.build_plan(
        MetricFlowQuerySpec(
            metric_specs=(MetricSpec(element_name="bookings_growth_since_start_of_month"),),
            time_dimension_specs=(MTD_SPEC_DAY,),
        )
    )

    assert_plan_snapshot_text_equal(
        request=request,
        snapshot_configuration=mf_test_configuration,
        plan=dataflow_plan,
        plan_snapshot_text=dataflow_plan.structure_text(),
    )

    display_graph_if_requested(
        request=request,
        mf_test_configuration=mf_test_configuration,
        dag_graph=dataflow_plan,
    )


@pytest.mark.sql_engine_snapshot
def test_derived_metric_offset_with_granularity(  # noqa: D103
    request: FixtureRequest,
    mf_test_configuration: MetricFlowTestConfiguration,
    dataflow_plan_builder: DataflowPlanBuilder,
) -> None:
    dataflow_plan = dataflow_plan_builder.build_plan(
        MetricFlowQuerySpec(
            metric_specs=(MetricSpec(element_name="bookings_5_day_lag"),), time_dimension_specs=(MTD_SPEC_MONTH,)
        )
    )

    assert_plan_snapshot_text_equal(
        request=request,
        snapshot_configuration=mf_test_configuration,
        plan=dataflow_plan,
        plan_snapshot_text=dataflow_plan.structure_text(),
    )

    display_graph_if_requested(
        request=request,
        mf_test_configuration=mf_test_configuration,
        dag_graph=dataflow_plan,
    )


@pytest.mark.sql_engine_snapshot
def test_derived_offset_cumulative_metric(  # noqa: D103
    request: FixtureRequest,
    mf_test_configuration: MetricFlowTestConfiguration,
    dataflow_plan_builder: DataflowPlanBuilder,
) -> None:
    dataflow_plan = dataflow_plan_builder.build_plan(
        MetricFlowQuerySpec(
            metric_specs=(MetricSpec(element_name="every_2_days_bookers_2_days_ago"),),
            time_dimension_specs=(MTD_SPEC_DAY,),
        )
    )

    assert_plan_snapshot_text_equal(
        request=request,
        snapshot_configuration=mf_test_configuration,
        plan=dataflow_plan,
        plan_snapshot_text=dataflow_plan.structure_text(),
    )

    display_graph_if_requested(
        request=request,
        mf_test_configuration=mf_test_configuration,
        dag_graph=dataflow_plan,
    )


def test_join_to_time_spine_with_metric_time(  # noqa: D103
    request: FixtureRequest,
    mf_test_configuration: MetricFlowTestConfiguration,
    dataflow_plan_builder: DataflowPlanBuilder,
) -> None:
    dataflow_plan = dataflow_plan_builder.build_plan(
        MetricFlowQuerySpec(
            metric_specs=(MetricSpec(element_name="bookings_fill_nulls_with_0"),),
            time_dimension_specs=(MTD_SPEC_DAY,),
        )
    )

    assert_plan_snapshot_text_equal(
        request=request,
        snapshot_configuration=mf_test_configuration,
        plan=dataflow_plan,
        plan_snapshot_text=dataflow_plan.structure_text(),
    )

    display_graph_if_requested(
        request=request,
        mf_test_configuration=mf_test_configuration,
        dag_graph=dataflow_plan,
    )


def test_join_to_time_spine_derived_metric(  # noqa: D103
    request: FixtureRequest,
    mf_test_configuration: MetricFlowTestConfiguration,
    dataflow_plan_builder: DataflowPlanBuilder,
) -> None:
    dataflow_plan = dataflow_plan_builder.build_plan(
        MetricFlowQuerySpec(
            metric_specs=(MetricSpec(element_name="bookings_growth_2_weeks_fill_nulls_with_0"),),
            time_dimension_specs=(MTD_SPEC_DAY,),
        )
    )

    assert_plan_snapshot_text_equal(
        request=request,
        snapshot_configuration=mf_test_configuration,
        plan=dataflow_plan,
        plan_snapshot_text=dataflow_plan.structure_text(),
    )

    display_graph_if_requested(
        request=request,
        mf_test_configuration=mf_test_configuration,
        dag_graph=dataflow_plan,
    )


def test_join_to_time_spine_with_non_metric_time(  # noqa: D103
    request: FixtureRequest,
    mf_test_configuration: MetricFlowTestConfiguration,
    dataflow_plan_builder: DataflowPlanBuilder,
) -> None:
    dataflow_plan = dataflow_plan_builder.build_plan(
        MetricFlowQuerySpec(
            metric_specs=(MetricSpec(element_name="bookings_fill_nulls_with_0"),),
            time_dimension_specs=(
                TimeDimensionSpec(
                    element_name="paid_at",
                    entity_links=(EntityReference("booking"),),
                    time_granularity=ExpandedTimeGranularity.from_time_granularity(TimeGranularity.DAY),
                ),
            ),
        )
    )

    assert_plan_snapshot_text_equal(
        request=request,
        snapshot_configuration=mf_test_configuration,
        plan=dataflow_plan,
        plan_snapshot_text=dataflow_plan.structure_text(),
    )

    display_graph_if_requested(
        request=request,
        mf_test_configuration=mf_test_configuration,
        dag_graph=dataflow_plan,
    )


def test_dont_join_to_time_spine_if_no_time_dimension_requested(  # noqa: D103
    request: FixtureRequest,
    mf_test_configuration: MetricFlowTestConfiguration,
    dataflow_plan_builder: DataflowPlanBuilder,
) -> None:
    dataflow_plan = dataflow_plan_builder.build_plan(
        MetricFlowQuerySpec(metric_specs=(MetricSpec(element_name="bookings_fill_nulls_with_0"),))
    )

    assert_plan_snapshot_text_equal(
        request=request,
        snapshot_configuration=mf_test_configuration,
        plan=dataflow_plan,
        plan_snapshot_text=dataflow_plan.structure_text(),
    )

    display_graph_if_requested(
        request=request,
        mf_test_configuration=mf_test_configuration,
        dag_graph=dataflow_plan,
    )


def test_nested_derived_metric_with_outer_offset(  # noqa: D103
    request: FixtureRequest,
    mf_test_configuration: MetricFlowTestConfiguration,
    dataflow_plan_builder: DataflowPlanBuilder,
) -> None:
    dataflow_plan = dataflow_plan_builder.build_plan(
        MetricFlowQuerySpec(
            metric_specs=(MetricSpec(element_name="bookings_offset_twice"),),
            time_dimension_specs=(MTD_SPEC_DAY,),
        )
    )

    assert_plan_snapshot_text_equal(
        request=request,
        snapshot_configuration=mf_test_configuration,
        plan=dataflow_plan,
        plan_snapshot_text=dataflow_plan.structure_text(),
    )

    display_graph_if_requested(
        request=request,
        mf_test_configuration=mf_test_configuration,
        dag_graph=dataflow_plan,
    )


def test_min_max_only_categorical(
    request: FixtureRequest,
    mf_test_configuration: MetricFlowTestConfiguration,
    dataflow_plan_builder: DataflowPlanBuilder,
) -> None:
    """Tests a plan to get the min & max distinct values of a categorical dimension."""
    dataflow_plan = dataflow_plan_builder.build_plan_for_distinct_values(
        query_spec=MetricFlowQuerySpec(
            dimension_specs=(
                DimensionSpec(element_name="country_latest", entity_links=(EntityReference(element_name="listing"),)),
            ),
            min_max_only=True,
        )
    )

    assert_plan_snapshot_text_equal(
        request=request,
        snapshot_configuration=mf_test_configuration,
        plan=dataflow_plan,
        plan_snapshot_text=dataflow_plan.structure_text(),
    )

    display_graph_if_requested(
        request=request,
        mf_test_configuration=mf_test_configuration,
        dag_graph=dataflow_plan,
    )


def test_min_max_only_time(
    request: FixtureRequest,
    mf_test_configuration: MetricFlowTestConfiguration,
    dataflow_plan_builder: DataflowPlanBuilder,
) -> None:
    """Tests a plan to get the min & max distinct values of a time dimension."""
    dataflow_plan = dataflow_plan_builder.build_plan_for_distinct_values(
        query_spec=MetricFlowQuerySpec(
            time_dimension_specs=(
                TimeDimensionSpec(
                    element_name="paid_at",
                    entity_links=(EntityReference("booking"),),
                    time_granularity=ExpandedTimeGranularity.from_time_granularity(TimeGranularity.DAY),
                ),
            ),
            min_max_only=True,
        )
    )

    assert_plan_snapshot_text_equal(
        request=request,
        snapshot_configuration=mf_test_configuration,
        plan=dataflow_plan,
        plan_snapshot_text=dataflow_plan.structure_text(),
    )

    display_graph_if_requested(
        request=request,
        mf_test_configuration=mf_test_configuration,
        dag_graph=dataflow_plan,
    )


def test_metric_time_only(  # noqa: D103
    request: FixtureRequest,
    mf_test_configuration: MetricFlowTestConfiguration,
    dataflow_plan_builder: DataflowPlanBuilder,
) -> None:
    dataflow_plan = dataflow_plan_builder.build_plan_for_distinct_values(
        MetricFlowQuerySpec(time_dimension_specs=(MTD_SPEC_DAY,))
    )

    assert_plan_snapshot_text_equal(
        request=request,
        snapshot_configuration=mf_test_configuration,
        plan=dataflow_plan,
        plan_snapshot_text=dataflow_plan.structure_text(),
    )

    display_graph_if_requested(
        request=request,
        mf_test_configuration=mf_test_configuration,
        dag_graph=dataflow_plan,
    )


def test_metric_time_quarter(  # noqa: D103
    request: FixtureRequest,
    mf_test_configuration: MetricFlowTestConfiguration,
    dataflow_plan_builder: DataflowPlanBuilder,
) -> None:
    dataflow_plan = dataflow_plan_builder.build_plan_for_distinct_values(
        MetricFlowQuerySpec(time_dimension_specs=(MTD_SPEC_QUARTER,))
    )

    assert_plan_snapshot_text_equal(
        request=request,
        snapshot_configuration=mf_test_configuration,
        plan=dataflow_plan,
        plan_snapshot_text=dataflow_plan.structure_text(),
    )

    display_graph_if_requested(
        request=request,
        mf_test_configuration=mf_test_configuration,
        dag_graph=dataflow_plan,
    )


def test_metric_time_with_other_dimensions(  # noqa: D103
    request: FixtureRequest,
    mf_test_configuration: MetricFlowTestConfiguration,
    dataflow_plan_builder: DataflowPlanBuilder,
) -> None:
    dataflow_plan = dataflow_plan_builder.build_plan_for_distinct_values(
        MetricFlowQuerySpec(
            time_dimension_specs=(MTD_SPEC_DAY, MTD_SPEC_MONTH),
            dimension_specs=(
                DimensionSpec(element_name="home_state_latest", entity_links=(EntityReference("user"),)),
                DimensionSpec(element_name="is_lux_latest", entity_links=(EntityReference("listing"),)),
            ),
        )
    )

    assert_plan_snapshot_text_equal(
        request=request,
        snapshot_configuration=mf_test_configuration,
        plan=dataflow_plan,
        plan_snapshot_text=dataflow_plan.structure_text(),
    )

    display_graph_if_requested(
        request=request,
        mf_test_configuration=mf_test_configuration,
        dag_graph=dataflow_plan,
    )


def test_dimensions_with_time_constraint(  # noqa: D103
    request: FixtureRequest,
    mf_test_configuration: MetricFlowTestConfiguration,
    dataflow_plan_builder: DataflowPlanBuilder,
) -> None:
    dataflow_plan = dataflow_plan_builder.build_plan_for_distinct_values(
        MetricFlowQuerySpec(
            time_dimension_specs=(MTD_SPEC_MONTH,),
            dimension_specs=(DimensionSpec(element_name="is_lux_latest", entity_links=(EntityReference("listing"),)),),
            time_range_constraint=TimeRangeConstraint(
                start_time=datetime.datetime(2020, 1, 1), end_time=datetime.datetime(2020, 1, 3)
            ),
        )
    )

    assert_plan_snapshot_text_equal(
        request=request,
        snapshot_configuration=mf_test_configuration,
        plan=dataflow_plan,
        plan_snapshot_text=dataflow_plan.structure_text(),
    )

    display_graph_if_requested(
        request=request,
        mf_test_configuration=mf_test_configuration,
        dag_graph=dataflow_plan,
    )


def test_min_max_only_time_year(
    request: FixtureRequest,
    mf_test_configuration: MetricFlowTestConfiguration,
    dataflow_plan_builder: DataflowPlanBuilder,
) -> None:
    """Tests a plan to get the min & max distinct values of a time dimension with year granularity."""
    dataflow_plan = dataflow_plan_builder.build_plan_for_distinct_values(
        query_spec=MetricFlowQuerySpec(
            time_dimension_specs=(
                TimeDimensionSpec(
                    element_name="paid_at",
                    entity_links=(EntityReference("booking"),),
                    time_granularity=ExpandedTimeGranularity.from_time_granularity(TimeGranularity.YEAR),
                ),
            ),
            min_max_only=True,
        )
    )

    assert_plan_snapshot_text_equal(
        request=request,
        snapshot_configuration=mf_test_configuration,
        plan=dataflow_plan,
        plan_snapshot_text=dataflow_plan.structure_text(),
    )

    display_graph_if_requested(
        request=request,
        mf_test_configuration=mf_test_configuration,
        dag_graph=dataflow_plan,
    )


def test_min_max_metric_time(
    request: FixtureRequest,
    mf_test_configuration: MetricFlowTestConfiguration,
    dataflow_plan_builder: DataflowPlanBuilder,
) -> None:
    """Tests a plan to get the min & max distinct values of metric_time."""
    dataflow_plan = dataflow_plan_builder.build_plan_for_distinct_values(
        query_spec=MetricFlowQuerySpec(
            time_dimension_specs=(MTD_SPEC_DAY,),
            min_max_only=True,
        )
    )

    assert_plan_snapshot_text_equal(
        request=request,
        snapshot_configuration=mf_test_configuration,
        plan=dataflow_plan,
        plan_snapshot_text=dataflow_plan.structure_text(),
    )

    display_graph_if_requested(
        request=request,
        mf_test_configuration=mf_test_configuration,
        dag_graph=dataflow_plan,
    )


def test_min_max_metric_time_week(
    request: FixtureRequest,
    mf_test_configuration: MetricFlowTestConfiguration,
    dataflow_plan_builder: DataflowPlanBuilder,
) -> None:
    """Tests a plan to get the min & max distinct values of metric_time with non-default granularity."""
    dataflow_plan = dataflow_plan_builder.build_plan_for_distinct_values(
        query_spec=MetricFlowQuerySpec(
            time_dimension_specs=(MTD_SPEC_WEEK,),
            min_max_only=True,
        )
    )

    assert_plan_snapshot_text_equal(
        request=request,
        snapshot_configuration=mf_test_configuration,
        plan=dataflow_plan,
        plan_snapshot_text=dataflow_plan.structure_text(),
    )

    display_graph_if_requested(
        request=request,
        mf_test_configuration=mf_test_configuration,
        dag_graph=dataflow_plan,
    )


def test_join_to_time_spine_with_filters(
    request: FixtureRequest,
    mf_test_configuration: MetricFlowTestConfiguration,
    dataflow_plan_builder: DataflowPlanBuilder,
    query_parser: MetricFlowQueryParser,
    create_source_tables: bool,
) -> None:
    """Test that filter is not applied until after time spine join."""
    query_spec = query_parser.parse_and_validate_query(
        metric_names=("bookings_fill_nulls_with_0",),
        group_by_names=("metric_time__day",),
        where_constraints=[
            PydanticWhereFilter(where_sql_template=("{{ TimeDimension('metric_time', 'day') }} = '2020-01-01'"))
        ],
        time_constraint_start=datetime.datetime(2020, 1, 3),
        time_constraint_end=datetime.datetime(2020, 1, 5),
    ).query_spec
    dataflow_plan = dataflow_plan_builder.build_plan(query_spec)

    assert_plan_snapshot_text_equal(
        request=request,
        snapshot_configuration=mf_test_configuration,
        plan=dataflow_plan,
        plan_snapshot_text=dataflow_plan.structure_text(),
    )

    display_graph_if_requested(
        request=request,
        mf_test_configuration=mf_test_configuration,
        dag_graph=dataflow_plan,
    )


def test_offset_window_metric_filter_and_query_have_different_granularities(
    request: FixtureRequest,
    mf_test_configuration: MetricFlowTestConfiguration,
    dataflow_plan_builder: DataflowPlanBuilder,
    query_parser: MetricFlowQueryParser,
    create_source_tables: bool,
) -> None:
    """Test a query where an offset window metric is queried with one granularity and filtered by a different one."""
    query_spec = query_parser.parse_and_validate_query(
        metric_names=("booking_fees_last_week_per_booker_this_week",),
        group_by_names=("metric_time__month",),
        where_constraints=[
            PydanticWhereFilter(where_sql_template=("{{ TimeDimension('metric_time', 'day') }} = '2020-01-01'"))
        ],
    ).query_spec
    dataflow_plan = dataflow_plan_builder.build_plan(query_spec)

    assert_plan_snapshot_text_equal(
        request=request,
        snapshot_configuration=mf_test_configuration,
        plan=dataflow_plan,
        plan_snapshot_text=dataflow_plan.structure_text(),
    )

    display_graph_if_requested(
        request=request,
        mf_test_configuration=mf_test_configuration,
        dag_graph=dataflow_plan,
    )


def test_offset_to_grain_metric_filter_and_query_have_different_granularities(
    request: FixtureRequest,
    mf_test_configuration: MetricFlowTestConfiguration,
    dataflow_plan_builder: DataflowPlanBuilder,
    query_parser: MetricFlowQueryParser,
    create_source_tables: bool,
) -> None:
    """Test a query where an offset to grain metric is queried with one granularity and filtered by a different one."""
    query_spec = query_parser.parse_and_validate_query(
        metric_names=("bookings_at_start_of_month",),
        group_by_names=("metric_time__month",),
        where_constraints=[
            PydanticWhereFilter(where_sql_template=("{{ TimeDimension('metric_time', 'day') }} = '2020-01-01'"))
        ],
    ).query_spec
    dataflow_plan = dataflow_plan_builder.build_plan(query_spec)

    assert_plan_snapshot_text_equal(
        request=request,
        snapshot_configuration=mf_test_configuration,
        plan=dataflow_plan,
        plan_snapshot_text=dataflow_plan.structure_text(),
    )

    display_graph_if_requested(
        request=request,
        mf_test_configuration=mf_test_configuration,
        dag_graph=dataflow_plan,
    )


def test_metric_in_query_where_filter(
    request: FixtureRequest,
    mf_test_configuration: MetricFlowTestConfiguration,
    dataflow_plan_builder: DataflowPlanBuilder,
    query_parser: MetricFlowQueryParser,
    create_source_tables: bool,
) -> None:
    """Test querying a metric that has a metric in its where filter."""
    query_spec = query_parser.parse_and_validate_query(
        metric_names=("listings",), where_constraint_strs=["{{ Metric('bookings', ['listing'])}} > 2"]
    ).query_spec
    dataflow_plan = dataflow_plan_builder.build_plan(query_spec)

    assert_plan_snapshot_text_equal(
        request=request,
        snapshot_configuration=mf_test_configuration,
        plan=dataflow_plan,
        plan_snapshot_text=dataflow_plan.structure_text(),
    )

    display_graph_if_requested(
        request=request,
        mf_test_configuration=mf_test_configuration,
        dag_graph=dataflow_plan,
    )


def test_metric_in_metric_where_filter(
    request: FixtureRequest,
    mf_test_configuration: MetricFlowTestConfiguration,
    dataflow_plan_builder: DataflowPlanBuilder,
    query_parser: MetricFlowQueryParser,
    create_source_tables: bool,
) -> None:
    """Test querying a metric that has a metric in its where filter."""
    query_spec = query_parser.parse_and_validate_query(
        metric_names=("active_listings",),
    ).query_spec
    dataflow_plan = dataflow_plan_builder.build_plan(query_spec)

    assert_plan_snapshot_text_equal(
        request=request,
        snapshot_configuration=mf_test_configuration,
        plan=dataflow_plan,
        plan_snapshot_text=dataflow_plan.structure_text(),
    )

    display_graph_if_requested(
        request=request,
        mf_test_configuration=mf_test_configuration,
        dag_graph=dataflow_plan,
    )


@pytest.mark.slow
def test_all_available_metric_filters(
    mf_engine_test_fixture_mapping: Mapping[SemanticManifestSetup, MetricFlowEngineTestFixture]
) -> None:
    """Checks that all allowed metric filters do not error when used in dataflow plan."""
    mf_engine_test_fixture = mf_engine_test_fixture_mapping[SemanticManifestSetup.SIMPLE_MANIFEST]
    query_parser = mf_engine_test_fixture.query_parser
    dataflow_plan_builder = mf_engine_test_fixture.dataflow_plan_builder
    metric_lookup = mf_engine_test_fixture.semantic_manifest_lookup.metric_lookup
    bookings_group_by_item_set = metric_lookup.get_common_group_by_items(
        metric_references=(MetricReference("bookings"),),
        set_filter=GroupByItemSetFilter.create(any_properties_allowlist=(GroupByItemProperty.METRIC,)),
    )
    for group_by_metric_spec in group_specs_by_type(bookings_group_by_item_set.specs).group_by_metric_specs:
        entity_spec = group_by_metric_spec.metric_subquery_entity_spec
        query_spec = query_parser.parse_and_validate_query(
            metric_names=("bookings",),
            where_constraints=[
                PydanticWhereFilter(
                    where_sql_template=string.Template("{{ Metric('$metric_name', ['$entity_name']) }} > 2").substitute(
                        metric_name=group_by_metric_spec.element_name, entity_name=entity_spec.dunder_name
                    ),
                )
            ],
        ).query_spec
        dataflow_plan_builder.build_plan(query_spec)


def test_cumulative_metric_with_non_default_grain(
    request: FixtureRequest,
    mf_test_configuration: MetricFlowTestConfiguration,
    dataflow_plan_builder: DataflowPlanBuilder,
    query_parser: MetricFlowQueryParser,
    create_source_tables: bool,
) -> None:
    """Test querying a cumulative metric using a granularity that is not the metric's default."""
    query_spec = query_parser.parse_and_validate_query(
        metric_names=("trailing_2_months_revenue",), group_by_names=("metric_time__year",)
    ).query_spec
    dataflow_plan = dataflow_plan_builder.build_plan(query_spec)

    assert_plan_snapshot_text_equal(
        request=request,
        snapshot_configuration=mf_test_configuration,
        plan=dataflow_plan,
        plan_snapshot_text=dataflow_plan.structure_text(),
    )

    display_graph_if_requested(
        request=request,
        mf_test_configuration=mf_test_configuration,
        dag_graph=dataflow_plan,
    )


def test_derived_cumulative_metric_with_non_default_grain(
    request: FixtureRequest,
    mf_test_configuration: MetricFlowTestConfiguration,
    dataflow_plan_builder: DataflowPlanBuilder,
    query_parser: MetricFlowQueryParser,
    create_source_tables: bool,
) -> None:
    """Test querying a derived metric with a cumulative input metric using non-default granularity."""
    query_spec = query_parser.parse_and_validate_query(
        metric_names=("trailing_2_months_revenue_sub_10",), group_by_names=("metric_time__month",)
    ).query_spec
    dataflow_plan = dataflow_plan_builder.build_plan(query_spec)

    assert_plan_snapshot_text_equal(
        request=request,
        snapshot_configuration=mf_test_configuration,
        plan=dataflow_plan,
        plan_snapshot_text=dataflow_plan.structure_text(),
    )

    display_graph_if_requested(
        request=request,
        mf_test_configuration=mf_test_configuration,
        dag_graph=dataflow_plan,
    )
