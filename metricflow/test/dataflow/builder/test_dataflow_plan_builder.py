import logging

import pytest
from _pytest.fixtures import FixtureRequest

from metricflow.dataflow.builder.dataflow_plan_builder import DataflowPlanBuilder
from metricflow.dataflow.dataflow_plan_to_text import dataflow_plan_as_text
from metricflow.errors.errors import UnableToSatisfyQueryError
from metricflow.specs import (
    MetricFlowQuerySpec,
    MetricSpec,
    DimensionSpec,
    LinklessIdentifierSpec,
    TimeDimensionSpec,
    SpecWhereClauseConstraint,
    LinkableSpecSet,
)
from metricflow.specs import (
    OrderBySpec,
)
from metricflow.sql.sql_bind_parameters import SqlBindParameters
from metricflow.time.time_granularity import TimeGranularity
from metricflow.dataset.data_source_adapter import DataSourceDataSet
from metricflow.test.fixtures.setup_fixtures import MetricFlowTestSessionState
from metricflow.test.dataflow_plan_to_svg import display_graph_as_svg
from metricflow.test.plan_utils import assert_plan_snapshot_text_equal

logger = logging.getLogger(__name__)


def test_simple_plan(  # noqa: D
    request: FixtureRequest,
    mf_test_session_state: MetricFlowTestSessionState,
    dataflow_plan_builder: DataflowPlanBuilder[DataSourceDataSet],
) -> None:
    """Tests a simple plan getting a metric and a local dimension."""
    dataflow_plan = dataflow_plan_builder.build_plan(
        MetricFlowQuerySpec(
            metric_specs=(MetricSpec(element_name="bookings"),),
            dimension_specs=(
                DimensionSpec(
                    element_name="is_instant",
                    identifier_links=(),
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

    display_graph_as_svg(
        request=request,
        mf_test_session_state=mf_test_session_state,
        dag_graph=dataflow_plan,
    )


def test_joined_plan(  # noqa: D
    request: FixtureRequest,
    mf_test_session_state: MetricFlowTestSessionState,
    dataflow_plan_builder: DataflowPlanBuilder[DataSourceDataSet],
) -> None:
    """Tests a plan getting a measure and a joined dimension."""
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

    assert_plan_snapshot_text_equal(
        request=request,
        mf_test_session_state=mf_test_session_state,
        plan=dataflow_plan,
        plan_snapshot_text=dataflow_plan_as_text(dataflow_plan),
    )

    display_graph_as_svg(
        request=request,
        mf_test_session_state=mf_test_session_state,
        dag_graph=dataflow_plan,
    )


def test_order_by_plan(  # noqa: D
    request: FixtureRequest,
    mf_test_session_state: MetricFlowTestSessionState,
    dataflow_plan_builder: DataflowPlanBuilder[DataSourceDataSet],
) -> None:
    """Tests a plan with an order by."""
    dataflow_plan = dataflow_plan_builder.build_plan(
        MetricFlowQuerySpec(
            metric_specs=(MetricSpec(element_name="bookings"),),
            time_dimension_specs=(
                TimeDimensionSpec(
                    element_name="ds",
                    identifier_links=(),
                ),
            ),
            order_by_specs=(
                OrderBySpec(
                    item=TimeDimensionSpec(
                        element_name="ds",
                        identifier_links=(),
                    ),
                    descending=False,
                ),
                OrderBySpec(
                    item=MetricSpec(element_name="bookings"),
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

    display_graph_as_svg(
        request=request,
        mf_test_session_state=mf_test_session_state,
        dag_graph=dataflow_plan,
    )


def test_limit_rows_plan(  # noqa: D
    request: FixtureRequest,
    mf_test_session_state: MetricFlowTestSessionState,
    dataflow_plan_builder: DataflowPlanBuilder[DataSourceDataSet],
) -> None:
    """Tests a plan with a limit to the number of rows returned."""
    dataflow_plan = dataflow_plan_builder.build_plan(
        MetricFlowQuerySpec(
            metric_specs=(MetricSpec(element_name="bookings"),),
            time_dimension_specs=(
                TimeDimensionSpec(
                    element_name="ds",
                    identifier_links=(),
                ),
            ),
            limit=1,
        )
    )

    assert_plan_snapshot_text_equal(
        request=request,
        mf_test_session_state=mf_test_session_state,
        plan=dataflow_plan,
        plan_snapshot_text=dataflow_plan_as_text(dataflow_plan),
    )

    display_graph_as_svg(
        request=request,
        mf_test_session_state=mf_test_session_state,
        dag_graph=dataflow_plan,
    )


def test_multiple_metrics_plan(  # noqa: D
    request: FixtureRequest,
    mf_test_session_state: MetricFlowTestSessionState,
    dataflow_plan_builder: DataflowPlanBuilder[DataSourceDataSet],
) -> None:
    """Tests a plan to retrieve multiple metrics."""
    dataflow_plan = dataflow_plan_builder.build_plan(
        MetricFlowQuerySpec(
            metric_specs=(MetricSpec(element_name="bookings"), MetricSpec(element_name="booking_value")),
            dimension_specs=(
                DimensionSpec(
                    element_name="is_instant",
                    identifier_links=(),
                ),
            ),
            time_dimension_specs=(
                TimeDimensionSpec(
                    element_name="ds",
                    identifier_links=(),
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

    display_graph_as_svg(
        request=request,
        mf_test_session_state=mf_test_session_state,
        dag_graph=dataflow_plan,
    )


def test_expr_metrics_plan(
    request: FixtureRequest,
    mf_test_session_state: MetricFlowTestSessionState,
    dataflow_plan_builder: DataflowPlanBuilder[DataSourceDataSet],
) -> None:
    """Tests a plan to retrieve expr metric types"""
    dataflow_plan = dataflow_plan_builder.build_plan(
        MetricFlowQuerySpec(
            metric_specs=(MetricSpec(element_name="booking_fees"),),
            dimension_specs=(
                DimensionSpec(
                    element_name="country_latest",
                    identifier_links=(LinklessIdentifierSpec.from_element_name(element_name="listing"),),
                ),
            ),
            time_dimension_specs=(
                TimeDimensionSpec(
                    element_name="ds",
                    identifier_links=(),
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

    display_graph_as_svg(
        request=request,
        mf_test_session_state=mf_test_session_state,
        dag_graph=dataflow_plan,
    )


def test_single_data_source_ratio_metrics_plan(
    request: FixtureRequest,
    mf_test_session_state: MetricFlowTestSessionState,
    dataflow_plan_builder: DataflowPlanBuilder[DataSourceDataSet],
) -> None:
    """Tests a plan to retrieve a ratio where both measures come from one data source"""
    dataflow_plan = dataflow_plan_builder.build_plan(
        MetricFlowQuerySpec(
            metric_specs=(MetricSpec(element_name="bookings_per_booker"),),
            dimension_specs=(
                DimensionSpec(
                    element_name="country_latest",
                    identifier_links=(LinklessIdentifierSpec.from_element_name(element_name="listing"),),
                ),
            ),
            time_dimension_specs=(
                TimeDimensionSpec(
                    element_name="ds",
                    identifier_links=(),
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

    display_graph_as_svg(
        request=request,
        mf_test_session_state=mf_test_session_state,
        dag_graph=dataflow_plan,
    )


def test_multi_data_source_ratio_metrics_plan(
    request: FixtureRequest,
    mf_test_session_state: MetricFlowTestSessionState,
    dataflow_plan_builder: DataflowPlanBuilder[DataSourceDataSet],
) -> None:
    """Tests a plan to retrieve a ratio where both measures come from one data source"""
    dataflow_plan = dataflow_plan_builder.build_plan(
        MetricFlowQuerySpec(
            metric_specs=(MetricSpec(element_name="bookings_per_view"),),
            dimension_specs=(
                DimensionSpec(
                    element_name="country_latest",
                    identifier_links=(LinklessIdentifierSpec.from_element_name(element_name="listing"),),
                ),
            ),
            time_dimension_specs=(
                TimeDimensionSpec(
                    element_name="ds",
                    identifier_links=(),
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

    display_graph_as_svg(
        request=request,
        mf_test_session_state=mf_test_session_state,
        dag_graph=dataflow_plan,
    )


def test_multihop_join_plan(  # noqa: D
    request: FixtureRequest,
    mf_test_session_state: MetricFlowTestSessionState,
    multihop_dataflow_plan_builder: DataflowPlanBuilder[DataSourceDataSet],
) -> None:
    """Tests a plan with an order by."""
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

    assert_plan_snapshot_text_equal(
        request=request,
        mf_test_session_state=mf_test_session_state,
        plan=dataflow_plan,
        plan_snapshot_text=dataflow_plan_as_text(dataflow_plan),
    )

    display_graph_as_svg(
        request=request,
        mf_test_session_state=mf_test_session_state,
        dag_graph=dataflow_plan,
    )


def test_where_constrained_plan(  # noqa: D
    request: FixtureRequest,
    mf_test_session_state: MetricFlowTestSessionState,
    dataflow_plan_builder: DataflowPlanBuilder[DataSourceDataSet],
) -> None:
    """Tests a simple plan getting a metric and a local dimension."""
    dataflow_plan = dataflow_plan_builder.build_plan(
        MetricFlowQuerySpec(
            metric_specs=(MetricSpec(element_name="bookings"),),
            dimension_specs=(
                DimensionSpec(
                    element_name="is_instant",
                    identifier_links=(),
                ),
            ),
            where_constraint=SpecWhereClauseConstraint(
                where_condition="listing__country_latest = 'us'",
                linkable_names=("listing__country_latest",),
                linkable_spec_set=LinkableSpecSet(
                    dimension_specs=(
                        DimensionSpec(
                            element_name="country_latest",
                            identifier_links=(LinklessIdentifierSpec.from_element_name("listing"),),
                        ),
                    )
                ),
                execution_parameters=SqlBindParameters(),
            ),
        )
    )

    assert_plan_snapshot_text_equal(
        request=request,
        mf_test_session_state=mf_test_session_state,
        plan=dataflow_plan,
        plan_snapshot_text=dataflow_plan_as_text(dataflow_plan),
    )

    display_graph_as_svg(
        request=request,
        mf_test_session_state=mf_test_session_state,
        dag_graph=dataflow_plan,
    )


def test_where_constrained_plan_time_dimension(  # noqa: D
    request: FixtureRequest,
    mf_test_session_state: MetricFlowTestSessionState,
    dataflow_plan_builder: DataflowPlanBuilder[DataSourceDataSet],
) -> None:
    """Tests a simple plan getting a metric and a local dimension."""
    dataflow_plan = dataflow_plan_builder.build_plan(
        MetricFlowQuerySpec(
            metric_specs=(MetricSpec(element_name="bookings"),),
            dimension_specs=(
                DimensionSpec(
                    element_name="is_instant",
                    identifier_links=(),
                ),
            ),
            where_constraint=SpecWhereClauseConstraint(
                where_condition="ds >= '2020-01-01'",
                linkable_names=("ds",),
                linkable_spec_set=LinkableSpecSet(
                    time_dimension_specs=(
                        TimeDimensionSpec(
                            element_name="ds",
                            identifier_links=(),
                        ),
                    )
                ),
                execution_parameters=SqlBindParameters(),
            ),
        )
    )

    assert_plan_snapshot_text_equal(
        request=request,
        mf_test_session_state=mf_test_session_state,
        plan=dataflow_plan,
        plan_snapshot_text=dataflow_plan_as_text(dataflow_plan),
    )

    display_graph_as_svg(
        request=request,
        mf_test_session_state=mf_test_session_state,
        dag_graph=dataflow_plan,
    )


def test_where_constrained_with_common_linkable_plan(  # noqa: D
    request: FixtureRequest,
    mf_test_session_state: MetricFlowTestSessionState,
    dataflow_plan_builder: DataflowPlanBuilder[DataSourceDataSet],
) -> None:
    """Tests a dataflow plan where the where clause has a common linkable with the query."""
    dataflow_plan = dataflow_plan_builder.build_plan(
        MetricFlowQuerySpec(
            metric_specs=(MetricSpec(element_name="bookings"),),
            dimension_specs=(
                DimensionSpec(
                    element_name="country_latest",
                    identifier_links=(LinklessIdentifierSpec.from_element_name("listing"),),
                ),
            ),
            where_constraint=SpecWhereClauseConstraint(
                where_condition="listing__country_latest = 'us'",
                linkable_names=("listing__country_latest",),
                linkable_spec_set=LinkableSpecSet(
                    dimension_specs=(
                        DimensionSpec(
                            element_name="country_latest",
                            identifier_links=(LinklessIdentifierSpec.from_element_name("listing"),),
                        ),
                    )
                ),
                execution_parameters=SqlBindParameters(),
            ),
        )
    )

    assert_plan_snapshot_text_equal(
        request=request,
        mf_test_session_state=mf_test_session_state,
        plan=dataflow_plan,
        plan_snapshot_text=dataflow_plan_as_text(dataflow_plan),
    )

    display_graph_as_svg(
        request=request,
        mf_test_session_state=mf_test_session_state,
        dag_graph=dataflow_plan,
    )


def test_multihop_join_plan_ambiguous_dim(  # noqa: D
    mf_test_session_state: MetricFlowTestSessionState,
    dataflow_plan_builder: DataflowPlanBuilder[DataSourceDataSet],
) -> None:
    """Checks that an exception is thrown when trying to build a plan with an ambiguous dimension."""
    with pytest.raises(UnableToSatisfyQueryError):
        dataflow_plan_builder.build_plan(
            MetricFlowQuerySpec(
                metric_specs=(MetricSpec(element_name="views"),),
                dimension_specs=(
                    DimensionSpec(
                        element_name="home_country",
                        identifier_links=(
                            LinklessIdentifierSpec.from_element_name(element_name="listing"),
                            LinklessIdentifierSpec.from_element_name(element_name="user"),
                        ),
                    ),
                ),
            )
        )


def test_cumulative_metric(  # noqa: D
    request: FixtureRequest,
    mf_test_session_state: MetricFlowTestSessionState,
    dataflow_plan_builder: DataflowPlanBuilder[DataSourceDataSet],
) -> None:
    """Tests a plan to compute a cumulative metric."""
    dataflow_plan = dataflow_plan_builder.build_plan(
        MetricFlowQuerySpec(
            metric_specs=(MetricSpec(element_name="trailing_2_months_revenue"),),
            dimension_specs=(),
            time_dimension_specs=(
                TimeDimensionSpec(
                    element_name="ds",
                    identifier_links=(),
                    time_granularity=TimeGranularity.MONTH,
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

    display_graph_as_svg(
        request=request,
        mf_test_session_state=mf_test_session_state,
        dag_graph=dataflow_plan,
    )


def test_distinct_values_plan(  # noqa: D
    request: FixtureRequest,
    mf_test_session_state: MetricFlowTestSessionState,
    dataflow_plan_builder: DataflowPlanBuilder[DataSourceDataSet],
) -> None:
    """Tests a plan to get distinct values of a dimension."""
    dataflow_plan = dataflow_plan_builder.build_plan_for_distinct_values(
        metric_specs=(MetricSpec(element_name="bookings"),),
        dimension_spec=DimensionSpec(
            element_name="country_latest",
            identifier_links=(LinklessIdentifierSpec.from_element_name("listing"),),
        ),
        limit=100,
    )

    assert_plan_snapshot_text_equal(
        request=request,
        mf_test_session_state=mf_test_session_state,
        plan=dataflow_plan,
        plan_snapshot_text=dataflow_plan_as_text(dataflow_plan),
    )

    display_graph_as_svg(
        request=request,
        mf_test_session_state=mf_test_session_state,
        dag_graph=dataflow_plan,
    )
