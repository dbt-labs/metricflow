from __future__ import annotations

import logging

from _pytest.fixtures import FixtureRequest
from dbt_semantic_interfaces.naming.keywords import METRIC_TIME_ELEMENT_NAME
from dbt_semantic_interfaces.references import EntityReference
from metricflow_semantics.query.query_parser import MetricFlowQueryParser
from metricflow_semantics.specs.column_assoc import ColumnAssociationResolver
from metricflow_semantics.specs.dimension_spec import DimensionSpec
from metricflow_semantics.specs.metric_spec import MetricSpec
from metricflow_semantics.specs.query_spec import MetricFlowQuerySpec
from metricflow_semantics.test_helpers.config_helpers import MetricFlowTestConfiguration
from metricflow_semantics.test_helpers.metric_time_dimension import MTD_SPEC_DAY

from metricflow.dataflow.builder.dataflow_plan_builder import DataflowPlanBuilder
from tests_metricflow.dataflow.optimizer.source_scan.source_scan_optimizer_helpers import check_source_scan_optimization

logger = logging.getLogger(__name__)


def test_2_metrics_from_1_semantic_model(
    request: FixtureRequest,
    mf_test_configuration: MetricFlowTestConfiguration,
    dataflow_plan_builder: DataflowPlanBuilder,
) -> None:
    """Tests optimizing the plan for 2 simple-metrics that are defined from the same model.

    Since they are defined from the same model, the number of scans should be halved.
    """
    check_source_scan_optimization(
        request=request,
        mf_test_configuration=mf_test_configuration,
        dataflow_plan_builder=dataflow_plan_builder,
        query_spec=MetricFlowQuerySpec(
            metric_specs=(MetricSpec(element_name="bookings"), MetricSpec(element_name="booking_value")),
            dimension_specs=(
                MTD_SPEC_DAY,
                DimensionSpec(element_name="country_latest", entity_links=(EntityReference("listing"),)),
            ),
        ),
        expected_num_sources_in_unoptimized=4,
        expected_num_sources_in_optimized=2,
    )


def test_2_metrics_from_2_semantic_models(
    request: FixtureRequest,
    mf_test_configuration: MetricFlowTestConfiguration,
    dataflow_plan_builder: DataflowPlanBuilder,
) -> None:
    """Tests that 2 metrics from 2 different semantic models results in 2 scans."""
    check_source_scan_optimization(
        request=request,
        mf_test_configuration=mf_test_configuration,
        dataflow_plan_builder=dataflow_plan_builder,
        query_spec=MetricFlowQuerySpec(
            metric_specs=(MetricSpec(element_name="bookings"), MetricSpec(element_name="listings")),
            dimension_specs=(MTD_SPEC_DAY,),
        ),
        expected_num_sources_in_unoptimized=2,
        expected_num_sources_in_optimized=2,
    )


def test_3_metrics_from_2_semantic_models(
    request: FixtureRequest,
    mf_test_configuration: MetricFlowTestConfiguration,
    dataflow_plan_builder: DataflowPlanBuilder,
) -> None:
    """Tests that 3 metrics from the 2 different semantic models results in 2 scans."""
    check_source_scan_optimization(
        request=request,
        mf_test_configuration=mf_test_configuration,
        dataflow_plan_builder=dataflow_plan_builder,
        query_spec=MetricFlowQuerySpec(
            metric_specs=(
                MetricSpec(element_name="bookings"),
                MetricSpec(element_name="booking_value"),
                MetricSpec(element_name="listings"),
            ),
            dimension_specs=(MTD_SPEC_DAY,),
        ),
        expected_num_sources_in_unoptimized=3,
        expected_num_sources_in_optimized=2,
    )


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
    check_source_scan_optimization(
        request=request,
        mf_test_configuration=mf_test_configuration,
        dataflow_plan_builder=dataflow_plan_builder,
        query_spec=query_spec,
        expected_num_sources_in_unoptimized=2,
        expected_num_sources_in_optimized=2,
    )


def test_derived_metric(
    request: FixtureRequest,
    mf_test_configuration: MetricFlowTestConfiguration,
    dataflow_plan_builder: DataflowPlanBuilder,
) -> None:
    """Tests optimization of a query that use a derived metrics with simple-metric inputs coming from a single semantic model.

    non_referred_bookings_pct is a derived metric that uses simple metrics `[bookings, referred_bookings]`
    """
    check_source_scan_optimization(
        request=request,
        mf_test_configuration=mf_test_configuration,
        dataflow_plan_builder=dataflow_plan_builder,
        query_spec=MetricFlowQuerySpec(
            metric_specs=(MetricSpec(element_name="non_referred_bookings_pct"),),
            dimension_specs=(MTD_SPEC_DAY,),
        ),
        expected_num_sources_in_unoptimized=2,
        expected_num_sources_in_optimized=1,
    )


def test_nested_derived_metric(
    request: FixtureRequest,
    mf_test_configuration: MetricFlowTestConfiguration,
    dataflow_plan_builder: DataflowPlanBuilder,
) -> None:
    """Tests optimization of a query that use a nested derived metric from a single semantic model.

    The optimal solution would reduce this to 1 source scan, but there are challenges with derived metrics e.g. aliases,
    so that is left as a future improvement.
    """
    check_source_scan_optimization(
        request=request,
        mf_test_configuration=mf_test_configuration,
        dataflow_plan_builder=dataflow_plan_builder,
        query_spec=MetricFlowQuerySpec(
            metric_specs=(MetricSpec(element_name="instant_plus_non_referred_bookings_pct"),),
            dimension_specs=(MTD_SPEC_DAY,),
        ),
        expected_num_sources_in_unoptimized=4,
        expected_num_sources_in_optimized=2,
    )


def test_derived_metric_with_non_derived_metric(
    request: FixtureRequest,
    mf_test_configuration: MetricFlowTestConfiguration,
    dataflow_plan_builder: DataflowPlanBuilder,
) -> None:
    """Tests optimization of queries that use derived metrics and non-derived metrics.

    `non_referred_bookings_pct` is a derived metric that uses metrics [bookings, referred_bookings]
    `booking_value` is a simple metric.

    All of these simple metrics are defined from a common semantic model.

    Computation of `non_referred_bookings_pct` can be optimized to a single source, but isn't combined with the
    computation for `booking_value` as it's not yet supported e.g. alias needed to be handled.
    """
    check_source_scan_optimization(
        request=request,
        mf_test_configuration=mf_test_configuration,
        dataflow_plan_builder=dataflow_plan_builder,
        query_spec=MetricFlowQuerySpec(
            metric_specs=(
                MetricSpec(element_name="booking_value"),
                MetricSpec(element_name="non_referred_bookings_pct"),
            ),
            dimension_specs=(MTD_SPEC_DAY,),
        ),
        expected_num_sources_in_unoptimized=3,
        expected_num_sources_in_optimized=2,
    )


def test_derived_metric_same_alias_components_combined(
    request: FixtureRequest,
    mf_test_configuration: MetricFlowTestConfiguration,
    dataflow_plan_builder: DataflowPlanBuilder,
) -> None:
    """Tests optimization of querying 2 metrics which give the same alias to the same thing in their components.

    In this case we DO combine source nodes, since the components are the same exact thing so we don't need to
    scan over it twice
    """
    check_source_scan_optimization(
        request=request,
        mf_test_configuration=mf_test_configuration,
        dataflow_plan_builder=dataflow_plan_builder,
        query_spec=MetricFlowQuerySpec(
            metric_specs=(
                MetricSpec(element_name="derived_shared_alias_1a"),
                MetricSpec(element_name="derived_shared_alias_1b"),
            ),
            dimension_specs=(DimensionSpec(element_name="is_instant", entity_links=(EntityReference("booking"),)),),
        ),
        expected_num_sources_in_unoptimized=2,
        expected_num_sources_in_optimized=1,
    )


def test_derived_metric_same_alias_components_not_combined(
    request: FixtureRequest,
    mf_test_configuration: MetricFlowTestConfiguration,
    dataflow_plan_builder: DataflowPlanBuilder,
) -> None:
    """Tests optimization of querying 2 metrics which give the same alias different things in their components.

    In this case we should NOT combine source nodes, since this would generate two columns with
    the same alias.
    """
    check_source_scan_optimization(
        request=request,
        mf_test_configuration=mf_test_configuration,
        dataflow_plan_builder=dataflow_plan_builder,
        query_spec=MetricFlowQuerySpec(
            metric_specs=(
                MetricSpec(element_name="derived_shared_alias_1a"),
                MetricSpec(element_name="derived_shared_alias_2"),
            ),
            dimension_specs=(DimensionSpec(element_name="is_instant", entity_links=(EntityReference("booking"),)),),
        ),
        expected_num_sources_in_unoptimized=2,
        expected_num_sources_in_optimized=2,
    )


def test_2_ratio_metrics_from_1_semantic_model(
    request: FixtureRequest,
    mf_test_configuration: MetricFlowTestConfiguration,
    dataflow_plan_builder: DataflowPlanBuilder,
) -> None:
    """Tests that 2 ratio metrics with simple-metric inputs from a 1 semantic model result in 1 scan."""
    check_source_scan_optimization(
        request=request,
        mf_test_configuration=mf_test_configuration,
        dataflow_plan_builder=dataflow_plan_builder,
        query_spec=MetricFlowQuerySpec(
            metric_specs=(
                MetricSpec(element_name="bookings_per_booker"),
                MetricSpec(element_name="bookings_per_dollar"),
            ),
            dimension_specs=(MTD_SPEC_DAY,),
        ),
        expected_num_sources_in_unoptimized=4,
        expected_num_sources_in_optimized=1,
    )


def test_duplicate_simple_metrics(
    request: FixtureRequest,
    mf_test_configuration: MetricFlowTestConfiguration,
    dataflow_plan_builder: DataflowPlanBuilder,
) -> None:
    """Tests a case where derived metrics in a query use the same simple-metric input (in the same form e.g. filters)."""
    check_source_scan_optimization(
        request=request,
        mf_test_configuration=mf_test_configuration,
        dataflow_plan_builder=dataflow_plan_builder,
        query_spec=MetricFlowQuerySpec(
            metric_specs=(
                MetricSpec(element_name="derived_bookings_0"),
                MetricSpec(element_name="derived_bookings_1"),
            ),
            dimension_specs=(MTD_SPEC_DAY,),
        ),
        expected_num_sources_in_unoptimized=2,
        expected_num_sources_in_optimized=1,
    )
