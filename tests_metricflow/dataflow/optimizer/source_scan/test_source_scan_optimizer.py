from __future__ import annotations

import logging
from typing import Sequence

import pytest
from _pytest.fixtures import FixtureRequest
from dbt_semantic_interfaces.naming.keywords import METRIC_TIME_ELEMENT_NAME
from dbt_semantic_interfaces.type_enums.time_granularity import TimeGranularity
from metricflow_semantics.query.query_parser import MetricFlowQueryParser
from metricflow_semantics.specs.column_assoc import ColumnAssociationResolver
from metricflow_semantics.specs.query_spec import MetricFlowQuerySpec
from metricflow_semantics.specs.spec_classes import (
    DimensionSpec,
    EntityReference,
    MetricSpec,
)
from metricflow_semantics.test_helpers.config_helpers import MetricFlowTestConfiguration
from metricflow_semantics.test_helpers.snapshot_helpers import assert_plan_snapshot_text_equal
from typing_extensions import override

from metricflow.dataflow.builder.dataflow_plan_builder import DataflowPlanBuilder
from metricflow.dataflow.dataflow_plan import (
    DataflowPlan,
    DataflowPlanNode,
)
from metricflow.dataflow.dfs_walker import DataflowDagWalker
from metricflow.dataflow.nodes.read_sql_source import ReadSqlSourceNode
from metricflow.dataflow.optimizer.source_scan.source_scan_optimizer import SourceScanOptimizer
from metricflow.dataset.dataset_classes import DataSet
from tests_metricflow.dataflow_plan_to_svg import display_graph_if_requested

logger = logging.getLogger(__name__)


class _ReadSqlSourceNodeCounter(DataflowDagWalker[int]):
    """Counts the number of ReadSqlSourceNodes in the dataflow plan."""

    @override
    def default_visit_action(self, current_node: DataflowPlanNode, inputs: Sequence[int]) -> int:
        return sum(inputs)

    def visit_source_node(self, node: ReadSqlSourceNode) -> int:  # noqa: D102
        return 1


class DataflowPlanLookup:
    """A lookup class to get assorted properties about the dataflow plan."""

    def __init__(self, dataflow_plan: DataflowPlan) -> None:  # noqa: D107
        self._dataflow_plan_sink_node = dataflow_plan.checked_sink_node

    def source_node_count(self) -> int:
        """Counts the number of `ReadSqlSourceNodes` in the dataflow plan."""
        return self._dataflow_plan_sink_node.accept(_ReadSqlSourceNodeCounter())


def check_optimization(  # noqa: D103
    request: FixtureRequest,
    mf_test_configuration: MetricFlowTestConfiguration,
    dataflow_plan_builder: DataflowPlanBuilder,
    query_spec: MetricFlowQuerySpec,
    expected_num_sources_in_unoptimized: int,
    expected_num_sources_in_optimized: int,
) -> None:
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

    dataflow_plan_lookup = DataflowPlanLookup(dataflow_plan)
    assert dataflow_plan_lookup.source_node_count() == expected_num_sources_in_unoptimized

    optimizer = SourceScanOptimizer()
    optimized_dataflow_plan = optimizer.optimize(dataflow_plan)

    assert_plan_snapshot_text_equal(
        request=request,
        mf_test_configuration=mf_test_configuration,
        plan=optimized_dataflow_plan,
        plan_snapshot_text=optimized_dataflow_plan.structure_text(),
    )

    display_graph_if_requested(
        request=request,
        mf_test_configuration=mf_test_configuration,
        dag_graph=optimized_dataflow_plan,
    )

    optimized_dataflow_plan_lookup = DataflowPlanLookup(optimized_dataflow_plan)
    assert optimized_dataflow_plan_lookup.source_node_count() == expected_num_sources_in_optimized


@pytest.mark.sql_engine_snapshot
def test_2_metrics_from_1_semantic_model(
    request: FixtureRequest,
    mf_test_configuration: MetricFlowTestConfiguration,
    dataflow_plan_builder: DataflowPlanBuilder,
) -> None:
    """Tests that optimizing the plan for 2 metrics from 2 measure semantic models results in half the number of scans.

    Each metric is computed from the same measure semantic model and the dimension semantic model.
    """
    check_optimization(
        request=request,
        mf_test_configuration=mf_test_configuration,
        dataflow_plan_builder=dataflow_plan_builder,
        query_spec=MetricFlowQuerySpec(
            metric_specs=(MetricSpec(element_name="bookings"), MetricSpec(element_name="booking_value")),
            dimension_specs=(
                DataSet.metric_time_dimension_spec(TimeGranularity.DAY),
                DimensionSpec(element_name="country_latest", entity_links=(EntityReference("listing"),)),
            ),
        ),
        expected_num_sources_in_unoptimized=4,
        expected_num_sources_in_optimized=2,
    )


@pytest.mark.sql_engine_snapshot
def test_2_metrics_from_2_semantic_models(
    request: FixtureRequest,
    mf_test_configuration: MetricFlowTestConfiguration,
    dataflow_plan_builder: DataflowPlanBuilder,
) -> None:
    """Tests that 2 metrics from the 2 semantic models results in 2 scans."""
    check_optimization(
        request=request,
        mf_test_configuration=mf_test_configuration,
        dataflow_plan_builder=dataflow_plan_builder,
        query_spec=MetricFlowQuerySpec(
            metric_specs=(MetricSpec(element_name="bookings"), MetricSpec(element_name="listings")),
            dimension_specs=(DataSet.metric_time_dimension_spec(TimeGranularity.DAY),),
        ),
        expected_num_sources_in_unoptimized=2,
        expected_num_sources_in_optimized=2,
    )


@pytest.mark.sql_engine_snapshot
def test_3_metrics_from_2_semantic_models(
    request: FixtureRequest,
    mf_test_configuration: MetricFlowTestConfiguration,
    dataflow_plan_builder: DataflowPlanBuilder,
) -> None:
    """Tests that 3 metrics from the 2 semantic models results in 2 scans."""
    check_optimization(
        request=request,
        mf_test_configuration=mf_test_configuration,
        dataflow_plan_builder=dataflow_plan_builder,
        query_spec=MetricFlowQuerySpec(
            metric_specs=(
                MetricSpec(element_name="bookings"),
                MetricSpec(element_name="booking_value"),
                MetricSpec(element_name="listings"),
            ),
            dimension_specs=(DataSet.metric_time_dimension_spec(TimeGranularity.DAY),),
        ),
        expected_num_sources_in_unoptimized=3,
        expected_num_sources_in_optimized=2,
    )


@pytest.mark.sql_engine_snapshot
def test_constrained_metric_not_combined(
    request: FixtureRequest,
    mf_test_configuration: MetricFlowTestConfiguration,
    column_association_resolver: ColumnAssociationResolver,
    dataflow_plan_builder: DataflowPlanBuilder,
    query_parser: MetricFlowQueryParser,
) -> None:
    """Tests that 2 metrics from the same semantic model but where 1 is constrained results in 2 scans.

    If there is a constraint for a metric, it needs to be handled in a separate query because the constraint applies to
    all rows.
    """
    query_spec = query_parser.parse_and_validate_query(
        metric_names=("booking_value", "instant_booking_value"),
        group_by_names=(METRIC_TIME_ELEMENT_NAME,),
    ).query_spec
    check_optimization(
        request=request,
        mf_test_configuration=mf_test_configuration,
        dataflow_plan_builder=dataflow_plan_builder,
        query_spec=query_spec,
        expected_num_sources_in_unoptimized=2,
        expected_num_sources_in_optimized=2,
    )


@pytest.mark.sql_engine_snapshot
def test_derived_metric(
    request: FixtureRequest,
    mf_test_configuration: MetricFlowTestConfiguration,
    dataflow_plan_builder: DataflowPlanBuilder,
) -> None:
    """Tests optimization of a query that use a derived metrics with measures coming from a single semantic model.

    non_referred_bookings_pct is a derived metric that uses measures [bookings, referred_bookings]
    """
    check_optimization(
        request=request,
        mf_test_configuration=mf_test_configuration,
        dataflow_plan_builder=dataflow_plan_builder,
        query_spec=MetricFlowQuerySpec(
            metric_specs=(MetricSpec(element_name="non_referred_bookings_pct"),),
            dimension_specs=(DataSet.metric_time_dimension_spec(TimeGranularity.DAY),),
        ),
        expected_num_sources_in_unoptimized=2,
        expected_num_sources_in_optimized=1,
    )


@pytest.mark.sql_engine_snapshot
def test_nested_derived_metric(
    request: FixtureRequest,
    mf_test_configuration: MetricFlowTestConfiguration,
    dataflow_plan_builder: DataflowPlanBuilder,
) -> None:
    """Tests optimization of a query that use a nested derived metric from a single semantic model.

    The optimal solution would reduce this to 1 source scan, but there are challenges with derived metrics e.g. aliases,
    so that is left as a future improvement.
    """
    check_optimization(
        request=request,
        mf_test_configuration=mf_test_configuration,
        dataflow_plan_builder=dataflow_plan_builder,
        query_spec=MetricFlowQuerySpec(
            metric_specs=(MetricSpec(element_name="instant_plus_non_referred_bookings_pct"),),
            dimension_specs=(DataSet.metric_time_dimension_spec(TimeGranularity.DAY),),
        ),
        expected_num_sources_in_unoptimized=4,
        expected_num_sources_in_optimized=2,
    )


@pytest.mark.sql_engine_snapshot
def test_derived_metric_with_non_derived_metric(
    request: FixtureRequest,
    mf_test_configuration: MetricFlowTestConfiguration,
    dataflow_plan_builder: DataflowPlanBuilder,
) -> None:
    """Tests optimization of queries that use derived metrics and non-derived metrics.

    non_referred_bookings_pct is a derived metric that uses measures [bookings, referred_bookings]
    booking_value is a proxy metric that uses measures [bookings]

    All these measures are from a single semantic model.

    Computation of non_referred_bookings_pct can be optimized to a single source, but isn't combined with the
    computation for booking_value as it's not yet supported e.g. alias needed to be handled.
    """
    check_optimization(
        request=request,
        mf_test_configuration=mf_test_configuration,
        dataflow_plan_builder=dataflow_plan_builder,
        query_spec=MetricFlowQuerySpec(
            metric_specs=(
                MetricSpec(element_name="booking_value"),
                MetricSpec(element_name="non_referred_bookings_pct"),
            ),
            dimension_specs=(DataSet.metric_time_dimension_spec(TimeGranularity.DAY),),
        ),
        expected_num_sources_in_unoptimized=3,
        expected_num_sources_in_optimized=2,
    )


@pytest.mark.sql_engine_snapshot
def test_2_ratio_metrics_from_1_semantic_model(
    request: FixtureRequest,
    mf_test_configuration: MetricFlowTestConfiguration,
    dataflow_plan_builder: DataflowPlanBuilder,
) -> None:
    """Tests that 2 ratio metrics with measures from a 1 semantic model result in 1 scan."""
    check_optimization(
        request=request,
        mf_test_configuration=mf_test_configuration,
        dataflow_plan_builder=dataflow_plan_builder,
        query_spec=MetricFlowQuerySpec(
            metric_specs=(
                MetricSpec(element_name="bookings_per_booker"),
                MetricSpec(element_name="bookings_per_dollar"),
            ),
            dimension_specs=(DataSet.metric_time_dimension_spec(TimeGranularity.DAY),),
        ),
        expected_num_sources_in_unoptimized=4,
        expected_num_sources_in_optimized=1,
    )


@pytest.mark.sql_engine_snapshot
def test_duplicate_measures(
    request: FixtureRequest,
    mf_test_configuration: MetricFlowTestConfiguration,
    dataflow_plan_builder: DataflowPlanBuilder,
) -> None:
    """Tests a case where derived metrics in a query use the same measure (in the same form e.g. filters)."""
    check_optimization(
        request=request,
        mf_test_configuration=mf_test_configuration,
        dataflow_plan_builder=dataflow_plan_builder,
        query_spec=MetricFlowQuerySpec(
            metric_specs=(
                MetricSpec(element_name="derived_bookings_0"),
                MetricSpec(element_name="derived_bookings_1"),
            ),
            dimension_specs=(DataSet.metric_time_dimension_spec(TimeGranularity.DAY),),
        ),
        expected_num_sources_in_unoptimized=2,
        expected_num_sources_in_optimized=1,
    )
