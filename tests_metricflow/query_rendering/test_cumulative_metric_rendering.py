"""Tests cumulative metric query rendering by comparing rendered output against snapshot files."""

from __future__ import annotations

from typing import Mapping

import pytest
from _pytest.fixtures import FixtureRequest
from dbt_semantic_interfaces.implementations.filters.where_filter import PydanticWhereFilter
from dbt_semantic_interfaces.naming.keywords import METRIC_TIME_ELEMENT_NAME
from dbt_semantic_interfaces.references import EntityReference
from dbt_semantic_interfaces.test_utils import as_datetime
from dbt_semantic_interfaces.type_enums.time_granularity import TimeGranularity
from metricflow_semantics.filters.time_constraint import TimeRangeConstraint
from metricflow_semantics.query.query_parser import MetricFlowQueryParser
from metricflow_semantics.specs.column_assoc import ColumnAssociationResolver
from metricflow_semantics.specs.metric_spec import MetricSpec
from metricflow_semantics.specs.query_spec import MetricFlowQuerySpec
from metricflow_semantics.specs.time_dimension_spec import TimeDimensionSpec
from metricflow_semantics.test_helpers.config_helpers import MetricFlowTestConfiguration
from metricflow_semantics.test_helpers.metric_time_dimension import (
    MTD_SPEC_DAY,
    MTD_SPEC_MONTH,
    MTD_SPEC_QUARTER,
    MTD_SPEC_WEEK,
    MTD_SPEC_YEAR,
)
from metricflow_semantics.time.granularity import ExpandedTimeGranularity

from metricflow.dataflow.builder.dataflow_plan_builder import DataflowPlanBuilder
from metricflow.plan_conversion.to_sql_plan.dataflow_to_sql import DataflowToSqlPlanConverter
from metricflow.protocols.sql_client import SqlClient
from tests_metricflow.fixtures.manifest_fixtures import MetricFlowEngineTestFixture, SemanticManifestSetup
from tests_metricflow.query_rendering.compare_rendered_query import render_and_check


@pytest.mark.sql_engine_snapshot
def test_cumulative_metric(
    request: FixtureRequest,
    mf_test_configuration: MetricFlowTestConfiguration,
    dataflow_plan_builder: DataflowPlanBuilder,
    dataflow_to_sql_converter: DataflowToSqlPlanConverter,
    mf_engine_test_fixture_mapping: Mapping[SemanticManifestSetup, MetricFlowEngineTestFixture],
    sql_client: SqlClient,
) -> None:
    """Tests rendering a basic cumulative metric query."""
    query_spec = MetricFlowQuerySpec(
        metric_specs=(MetricSpec(element_name="trailing_2_months_revenue"),),
        time_dimension_specs=(
            TimeDimensionSpec(
                element_name="ds",
                entity_links=(),
                time_granularity=ExpandedTimeGranularity.from_time_granularity(TimeGranularity.DAY),
            ),
        ),
    )

    render_and_check(
        request=request,
        mf_test_configuration=mf_test_configuration,
        dataflow_to_sql_converter=dataflow_to_sql_converter,
        sql_client=sql_client,
        dataflow_plan_builder=dataflow_plan_builder,
        query_spec=query_spec,
    )


@pytest.mark.sql_engine_snapshot
def test_cumulative_metric_with_time_constraint(
    request: FixtureRequest,
    mf_test_configuration: MetricFlowTestConfiguration,
    dataflow_plan_builder: DataflowPlanBuilder,
    dataflow_to_sql_converter: DataflowToSqlPlanConverter,
    mf_engine_test_fixture_mapping: Mapping[SemanticManifestSetup, MetricFlowEngineTestFixture],
    sql_client: SqlClient,
) -> None:
    """Tests rendering a cumulative metric query with an adjustable time constraint.

    Not all query inputs with time constraint filters allow us to adjust the time constraint to include the full
    span of input data for a cumulative metric, but when we receive a time constraint filter expression we can
    automatically adjust it should render a query similar to this one.
    """
    query_spec = MetricFlowQuerySpec(
        metric_specs=(MetricSpec(element_name="trailing_2_months_revenue"),),
        dimension_specs=(),
        time_dimension_specs=(
            TimeDimensionSpec(
                element_name="metric_time",
                entity_links=(),
                time_granularity=ExpandedTimeGranularity.from_time_granularity(TimeGranularity.DAY),
            ),
        ),
        time_range_constraint=TimeRangeConstraint(
            start_time=as_datetime("2020-01-01"), end_time=as_datetime("2020-01-01")
        ),
    )

    render_and_check(
        request=request,
        mf_test_configuration=mf_test_configuration,
        dataflow_to_sql_converter=dataflow_to_sql_converter,
        sql_client=sql_client,
        dataflow_plan_builder=dataflow_plan_builder,
        query_spec=query_spec,
    )


@pytest.mark.sql_engine_snapshot
def test_cumulative_metric_with_non_adjustable_time_filter(
    request: FixtureRequest,
    mf_test_configuration: MetricFlowTestConfiguration,
    column_association_resolver: ColumnAssociationResolver,
    query_parser: MetricFlowQueryParser,
    dataflow_plan_builder: DataflowPlanBuilder,
    dataflow_to_sql_converter: DataflowToSqlPlanConverter,
    mf_engine_test_fixture_mapping: Mapping[SemanticManifestSetup, MetricFlowEngineTestFixture],
    sql_client: SqlClient,
) -> None:
    """Tests rendering a cumulative metric query with a time filter that cannot be automatically adjusted.

    Not all query inputs with time constraint filters allow us to adjust the time constraint to include the full
    span of input data for a cumulative metric. When we do not have an adjustable time filter we must include all
    input data in order to ensure the cumulative metric is correct.
    """
    query_spec = query_parser.parse_and_validate_query(
        metric_names=("every_two_days_bookers",),
        group_by_names=(METRIC_TIME_ELEMENT_NAME,),
        where_constraints=[
            PydanticWhereFilter(
                where_sql_template=(
                    "{{ TimeDimension('metric_time', 'day') }} = '2020-01-03' "
                    "or {{ TimeDimension('metric_time', 'day') }} = '2020-01-07'"
                )
            )
        ],
    ).query_spec

    render_and_check(
        request=request,
        mf_test_configuration=mf_test_configuration,
        dataflow_to_sql_converter=dataflow_to_sql_converter,
        sql_client=sql_client,
        dataflow_plan_builder=dataflow_plan_builder,
        query_spec=query_spec,
    )


@pytest.mark.sql_engine_snapshot
def test_cumulative_metric_no_ds(
    request: FixtureRequest,
    mf_test_configuration: MetricFlowTestConfiguration,
    dataflow_plan_builder: DataflowPlanBuilder,
    dataflow_to_sql_converter: DataflowToSqlPlanConverter,
    mf_engine_test_fixture_mapping: Mapping[SemanticManifestSetup, MetricFlowEngineTestFixture],
    sql_client: SqlClient,
) -> None:
    """Tests rendering a cumulative metric with no time dimension specified."""
    query_spec = MetricFlowQuerySpec(
        metric_specs=(MetricSpec(element_name="trailing_2_months_revenue"),),
        time_dimension_specs=(),
    )

    render_and_check(
        request=request,
        mf_test_configuration=mf_test_configuration,
        dataflow_to_sql_converter=dataflow_to_sql_converter,
        sql_client=sql_client,
        dataflow_plan_builder=dataflow_plan_builder,
        query_spec=query_spec,
    )


@pytest.mark.sql_engine_snapshot
def test_cumulative_metric_no_window(
    request: FixtureRequest,
    mf_test_configuration: MetricFlowTestConfiguration,
    dataflow_plan_builder: DataflowPlanBuilder,
    dataflow_to_sql_converter: DataflowToSqlPlanConverter,
    mf_engine_test_fixture_mapping: Mapping[SemanticManifestSetup, MetricFlowEngineTestFixture],
    sql_client: SqlClient,
) -> None:
    """Tests rendering a query where there is a windowless cumulative metric to compute."""
    query_spec = MetricFlowQuerySpec(
        metric_specs=(MetricSpec(element_name="revenue_all_time"),),
        time_dimension_specs=(
            TimeDimensionSpec(
                element_name="ds",
                entity_links=(),
                time_granularity=ExpandedTimeGranularity.from_time_granularity(TimeGranularity.MONTH),
            ),
        ),
    )

    render_and_check(
        request=request,
        mf_test_configuration=mf_test_configuration,
        dataflow_to_sql_converter=dataflow_to_sql_converter,
        sql_client=sql_client,
        dataflow_plan_builder=dataflow_plan_builder,
        query_spec=query_spec,
    )


@pytest.mark.sql_engine_snapshot
def test_cumulative_metric_no_window_with_time_constraint(
    request: FixtureRequest,
    mf_test_configuration: MetricFlowTestConfiguration,
    dataflow_plan_builder: DataflowPlanBuilder,
    dataflow_to_sql_converter: DataflowToSqlPlanConverter,
    mf_engine_test_fixture_mapping: Mapping[SemanticManifestSetup, MetricFlowEngineTestFixture],
    sql_client: SqlClient,
) -> None:
    """Tests rendering a query for a windowless cumulative metric query with an adjustable time constraint."""
    query_spec = MetricFlowQuerySpec(
        metric_specs=(MetricSpec(element_name="revenue_all_time"),),
        time_dimension_specs=(MTD_SPEC_DAY,),
        time_range_constraint=TimeRangeConstraint(
            start_time=as_datetime("2020-01-01"), end_time=as_datetime("2020-01-01")
        ),
    )

    render_and_check(
        request=request,
        mf_test_configuration=mf_test_configuration,
        dataflow_to_sql_converter=dataflow_to_sql_converter,
        sql_client=sql_client,
        dataflow_plan_builder=dataflow_plan_builder,
        query_spec=query_spec,
    )


@pytest.mark.sql_engine_snapshot
def test_cumulative_metric_grain_to_date(
    request: FixtureRequest,
    mf_test_configuration: MetricFlowTestConfiguration,
    dataflow_plan_builder: DataflowPlanBuilder,
    dataflow_to_sql_converter: DataflowToSqlPlanConverter,
    mf_engine_test_fixture_mapping: Mapping[SemanticManifestSetup, MetricFlowEngineTestFixture],
    sql_client: SqlClient,
) -> None:
    """Tests rendering a query against a grain_to_date cumulative metric."""
    query_spec = MetricFlowQuerySpec(
        metric_specs=(MetricSpec(element_name="revenue_mtd"),),
        time_dimension_specs=(
            TimeDimensionSpec(
                element_name="ds",
                entity_links=(),
                time_granularity=ExpandedTimeGranularity.from_time_granularity(TimeGranularity.MONTH),
            ),
        ),
    )

    render_and_check(
        request=request,
        mf_test_configuration=mf_test_configuration,
        dataflow_to_sql_converter=dataflow_to_sql_converter,
        sql_client=sql_client,
        dataflow_plan_builder=dataflow_plan_builder,
        query_spec=query_spec,
    )


@pytest.mark.skip("Test is currently broken")
@pytest.mark.sql_engine_snapshot
def test_cumulative_metric_month(
    request: FixtureRequest,
    mf_test_configuration: MetricFlowTestConfiguration,
    extended_date_dataflow_plan_builder: DataflowPlanBuilder,
    extended_date_dataflow_to_sql_converter: DataflowToSqlPlanConverter,
    mf_engine_test_fixture_mapping: Mapping[SemanticManifestSetup, MetricFlowEngineTestFixture],
    sql_client: SqlClient,
) -> None:
    """Tests rendering a query for a cumulative metric based on a monthly time dimension."""
    query_spec = MetricFlowQuerySpec(
        metric_specs=(MetricSpec(element_name="trailing_3_months_bookings"),),
        time_dimension_specs=(MTD_SPEC_MONTH,),
        time_range_constraint=TimeRangeConstraint(
            start_time=as_datetime("2020-03-05"), end_time=as_datetime("2021-01-04")
        ),
    )

    render_and_check(
        request=request,
        mf_test_configuration=mf_test_configuration,
        dataflow_to_sql_converter=extended_date_dataflow_to_sql_converter,
        sql_client=sql_client,
        dataflow_plan_builder=extended_date_dataflow_plan_builder,
        query_spec=query_spec,
    )


@pytest.mark.sql_engine_snapshot
def test_cumulative_metric_with_agg_time_dimension(
    request: FixtureRequest,
    mf_test_configuration: MetricFlowTestConfiguration,
    dataflow_plan_builder: DataflowPlanBuilder,
    dataflow_to_sql_converter: DataflowToSqlPlanConverter,
    mf_engine_test_fixture_mapping: Mapping[SemanticManifestSetup, MetricFlowEngineTestFixture],
    sql_client: SqlClient,
) -> None:
    """Tests rendering a query for a cumulative metric queried with agg time dimension."""
    query_spec = MetricFlowQuerySpec(
        metric_specs=(MetricSpec(element_name="trailing_2_months_revenue"),),
        time_dimension_specs=(
            TimeDimensionSpec(
                element_name="ds",
                entity_links=(EntityReference("revenue_instance"),),
                time_granularity=ExpandedTimeGranularity.from_time_granularity(TimeGranularity.DAY),
            ),
        ),
    )

    render_and_check(
        request=request,
        mf_test_configuration=mf_test_configuration,
        dataflow_to_sql_converter=dataflow_to_sql_converter,
        sql_client=sql_client,
        dataflow_plan_builder=dataflow_plan_builder,
        query_spec=query_spec,
    )


@pytest.mark.sql_engine_snapshot
def test_cumulative_metric_with_multiple_agg_time_dimensions(
    request: FixtureRequest,
    mf_test_configuration: MetricFlowTestConfiguration,
    dataflow_plan_builder: DataflowPlanBuilder,
    dataflow_to_sql_converter: DataflowToSqlPlanConverter,
    mf_engine_test_fixture_mapping: Mapping[SemanticManifestSetup, MetricFlowEngineTestFixture],
    sql_client: SqlClient,
) -> None:
    """Tests rendering a query for a cumulative metric queried with multiple agg time dimensions."""
    query_spec = MetricFlowQuerySpec(
        metric_specs=(MetricSpec(element_name="trailing_2_months_revenue"),),
        time_dimension_specs=(
            TimeDimensionSpec(
                element_name="ds",
                entity_links=(EntityReference("revenue_instance"),),
                time_granularity=ExpandedTimeGranularity.from_time_granularity(TimeGranularity.DAY),
            ),
            TimeDimensionSpec(
                element_name="ds",
                entity_links=(EntityReference("revenue_instance"),),
                time_granularity=ExpandedTimeGranularity.from_time_granularity(TimeGranularity.MONTH),
            ),
        ),
    )

    render_and_check(
        request=request,
        mf_test_configuration=mf_test_configuration,
        dataflow_to_sql_converter=dataflow_to_sql_converter,
        sql_client=sql_client,
        dataflow_plan_builder=dataflow_plan_builder,
        query_spec=query_spec,
    )


@pytest.mark.sql_engine_snapshot
def test_cumulative_metric_with_multiple_metric_time_dimensions(
    request: FixtureRequest,
    mf_test_configuration: MetricFlowTestConfiguration,
    dataflow_plan_builder: DataflowPlanBuilder,
    dataflow_to_sql_converter: DataflowToSqlPlanConverter,
    mf_engine_test_fixture_mapping: Mapping[SemanticManifestSetup, MetricFlowEngineTestFixture],
    sql_client: SqlClient,
) -> None:
    """Tests rendering a query for a cumulative metric queried with multiple metric time dimensions."""
    query_spec = MetricFlowQuerySpec(
        metric_specs=(MetricSpec(element_name="trailing_2_months_revenue"),),
        time_dimension_specs=(MTD_SPEC_DAY, MTD_SPEC_MONTH),
    )

    render_and_check(
        request=request,
        mf_test_configuration=mf_test_configuration,
        dataflow_to_sql_converter=dataflow_to_sql_converter,
        sql_client=sql_client,
        dataflow_plan_builder=dataflow_plan_builder,
        query_spec=query_spec,
    )


@pytest.mark.sql_engine_snapshot
def test_cumulative_metric_with_agg_time_and_metric_time(
    request: FixtureRequest,
    mf_test_configuration: MetricFlowTestConfiguration,
    dataflow_plan_builder: DataflowPlanBuilder,
    dataflow_to_sql_converter: DataflowToSqlPlanConverter,
    mf_engine_test_fixture_mapping: Mapping[SemanticManifestSetup, MetricFlowEngineTestFixture],
    sql_client: SqlClient,
) -> None:
    """Tests rendering a query for a cumulative metric queried with one agg time dimension and one metric time dimension."""
    query_spec = MetricFlowQuerySpec(
        metric_specs=(MetricSpec(element_name="trailing_2_months_revenue"),),
        time_dimension_specs=(
            MTD_SPEC_DAY,
            TimeDimensionSpec(
                element_name="ds",
                entity_links=(EntityReference("revenue_instance"),),
                time_granularity=ExpandedTimeGranularity.from_time_granularity(TimeGranularity.MONTH),
            ),
        ),
    )

    render_and_check(
        request=request,
        mf_test_configuration=mf_test_configuration,
        dataflow_to_sql_converter=dataflow_to_sql_converter,
        sql_client=sql_client,
        dataflow_plan_builder=dataflow_plan_builder,
        query_spec=query_spec,
    )


@pytest.mark.sql_engine_snapshot
def test_cumulative_metric_with_non_default_grain(
    request: FixtureRequest,
    mf_test_configuration: MetricFlowTestConfiguration,
    dataflow_plan_builder: DataflowPlanBuilder,
    dataflow_to_sql_converter: DataflowToSqlPlanConverter,
    mf_engine_test_fixture_mapping: Mapping[SemanticManifestSetup, MetricFlowEngineTestFixture],
    sql_client: SqlClient,
) -> None:
    """Tests rendering a query for a cumulative all-time metric queried with non-default grain."""
    query_spec = MetricFlowQuerySpec(
        metric_specs=(MetricSpec(element_name="revenue_all_time"),),
        time_dimension_specs=(MTD_SPEC_WEEK,),
    )

    render_and_check(
        request=request,
        mf_test_configuration=mf_test_configuration,
        dataflow_to_sql_converter=dataflow_to_sql_converter,
        sql_client=sql_client,
        dataflow_plan_builder=dataflow_plan_builder,
        query_spec=query_spec,
    )


@pytest.mark.sql_engine_snapshot
def test_window_metric_with_non_default_grain(
    request: FixtureRequest,
    mf_test_configuration: MetricFlowTestConfiguration,
    dataflow_plan_builder: DataflowPlanBuilder,
    dataflow_to_sql_converter: DataflowToSqlPlanConverter,
    mf_engine_test_fixture_mapping: Mapping[SemanticManifestSetup, MetricFlowEngineTestFixture],
    sql_client: SqlClient,
) -> None:
    """Tests rendering a query for a cumulative window metric queried with non-default grain."""
    query_spec = MetricFlowQuerySpec(
        metric_specs=(MetricSpec(element_name="trailing_2_months_revenue"),),
        time_dimension_specs=(MTD_SPEC_YEAR,),
    )

    render_and_check(
        request=request,
        mf_test_configuration=mf_test_configuration,
        dataflow_to_sql_converter=dataflow_to_sql_converter,
        sql_client=sql_client,
        dataflow_plan_builder=dataflow_plan_builder,
        query_spec=query_spec,
    )


@pytest.mark.sql_engine_snapshot
def test_grain_to_date_metric_with_non_default_grain(
    request: FixtureRequest,
    mf_test_configuration: MetricFlowTestConfiguration,
    dataflow_plan_builder: DataflowPlanBuilder,
    dataflow_to_sql_converter: DataflowToSqlPlanConverter,
    mf_engine_test_fixture_mapping: Mapping[SemanticManifestSetup, MetricFlowEngineTestFixture],
    sql_client: SqlClient,
) -> None:
    """Tests rendering a query for a cumulative grain to date metric queried with non-default grain."""
    query_spec = MetricFlowQuerySpec(
        metric_specs=(MetricSpec(element_name="revenue_mtd"),),
        time_dimension_specs=(MTD_SPEC_MONTH,),
    )

    render_and_check(
        request=request,
        mf_test_configuration=mf_test_configuration,
        dataflow_to_sql_converter=dataflow_to_sql_converter,
        sql_client=sql_client,
        dataflow_plan_builder=dataflow_plan_builder,
        query_spec=query_spec,
    )


@pytest.mark.sql_engine_snapshot
def test_window_metric_with_non_default_grains(
    request: FixtureRequest,
    mf_test_configuration: MetricFlowTestConfiguration,
    dataflow_plan_builder: DataflowPlanBuilder,
    dataflow_to_sql_converter: DataflowToSqlPlanConverter,
    mf_engine_test_fixture_mapping: Mapping[SemanticManifestSetup, MetricFlowEngineTestFixture],
    sql_client: SqlClient,
) -> None:
    """Tests rendering a query for a cumulative window metric queried with non-default grains.

    Uses both metric_time and agg_time_dimension. Excludes default grain.
    """
    query_spec = MetricFlowQuerySpec(
        metric_specs=(MetricSpec(element_name="every_two_days_bookers_fill_nulls_with_0"),),
        time_dimension_specs=(
            MTD_SPEC_WEEK,
            TimeDimensionSpec(
                element_name="ds",
                entity_links=(EntityReference("booking"),),
                time_granularity=ExpandedTimeGranularity.from_time_granularity(TimeGranularity.MONTH),
            ),
        ),
    )

    render_and_check(
        request=request,
        mf_test_configuration=mf_test_configuration,
        dataflow_to_sql_converter=dataflow_to_sql_converter,
        sql_client=sql_client,
        dataflow_plan_builder=dataflow_plan_builder,
        query_spec=query_spec,
    )


@pytest.mark.sql_engine_snapshot
def test_grain_to_date_metric_with_non_default_grains(
    request: FixtureRequest,
    mf_test_configuration: MetricFlowTestConfiguration,
    dataflow_plan_builder: DataflowPlanBuilder,
    dataflow_to_sql_converter: DataflowToSqlPlanConverter,
    mf_engine_test_fixture_mapping: Mapping[SemanticManifestSetup, MetricFlowEngineTestFixture],
    sql_client: SqlClient,
) -> None:
    """Tests rendering a query for a cumulative grain to date metric queried with non-default grains.

    Uses agg time dimension instead of metric_time. Excludes default grain.
    """
    query_spec = MetricFlowQuerySpec(
        metric_specs=(MetricSpec(element_name="revenue_mtd"),),
        time_dimension_specs=(
            TimeDimensionSpec(
                element_name="ds",
                entity_links=(EntityReference("revenue_instance"),),
                time_granularity=ExpandedTimeGranularity.from_time_granularity(TimeGranularity.QUARTER),
            ),
            TimeDimensionSpec(
                element_name="ds",
                entity_links=(EntityReference("revenue_instance"),),
                time_granularity=ExpandedTimeGranularity.from_time_granularity(TimeGranularity.YEAR),
            ),
        ),
    )

    render_and_check(
        request=request,
        mf_test_configuration=mf_test_configuration,
        dataflow_to_sql_converter=dataflow_to_sql_converter,
        sql_client=sql_client,
        dataflow_plan_builder=dataflow_plan_builder,
        query_spec=query_spec,
    )


@pytest.mark.sql_engine_snapshot
def test_all_time_metric_with_non_default_grains(
    request: FixtureRequest,
    mf_test_configuration: MetricFlowTestConfiguration,
    dataflow_plan_builder: DataflowPlanBuilder,
    dataflow_to_sql_converter: DataflowToSqlPlanConverter,
    mf_engine_test_fixture_mapping: Mapping[SemanticManifestSetup, MetricFlowEngineTestFixture],
    sql_client: SqlClient,
) -> None:
    """Tests rendering a query for a cumulative all-time metric queried with non-default grains.

    Uses only metric_time. Excludes default grain.
    """
    query_spec = MetricFlowQuerySpec(
        metric_specs=(MetricSpec(element_name="revenue_all_time"),),
        time_dimension_specs=(MTD_SPEC_WEEK, MTD_SPEC_QUARTER),
    )

    render_and_check(
        request=request,
        mf_test_configuration=mf_test_configuration,
        dataflow_to_sql_converter=dataflow_to_sql_converter,
        sql_client=sql_client,
        dataflow_plan_builder=dataflow_plan_builder,
        query_spec=query_spec,
    )


@pytest.mark.sql_engine_snapshot
def test_derived_cumulative_metric_with_non_default_grains(
    request: FixtureRequest,
    mf_test_configuration: MetricFlowTestConfiguration,
    dataflow_plan_builder: DataflowPlanBuilder,
    dataflow_to_sql_converter: DataflowToSqlPlanConverter,
    mf_engine_test_fixture_mapping: Mapping[SemanticManifestSetup, MetricFlowEngineTestFixture],
    sql_client: SqlClient,
) -> None:
    """Test querying a derived metric with a cumulative input metric using non-default grains."""
    query_spec = MetricFlowQuerySpec(
        metric_specs=(MetricSpec(element_name="trailing_2_months_revenue_sub_10"),),
        time_dimension_specs=(MTD_SPEC_WEEK,),
    )

    render_and_check(
        request=request,
        mf_test_configuration=mf_test_configuration,
        dataflow_to_sql_converter=dataflow_to_sql_converter,
        sql_client=sql_client,
        dataflow_plan_builder=dataflow_plan_builder,
        query_spec=query_spec,
    )


@pytest.mark.sql_engine_snapshot
def test_cumulative_metric_with_metric_definition_filter(
    request: FixtureRequest,
    mf_test_configuration: MetricFlowTestConfiguration,
    dataflow_plan_builder: DataflowPlanBuilder,
    dataflow_to_sql_converter: DataflowToSqlPlanConverter,
    mf_engine_test_fixture_mapping: Mapping[SemanticManifestSetup, MetricFlowEngineTestFixture],
    sql_client: SqlClient,
    query_parser: MetricFlowQueryParser,
) -> None:
    """Tests rendering a cumulative metric that has a filter defined in the YAML metric definition."""
    parsed_query = query_parser.parse_and_validate_query(
        metric_names=("trailing_2_months_revenue_with_filter",),
        group_by_names=("metric_time__day",),
    )

    render_and_check(
        request=request,
        mf_test_configuration=mf_test_configuration,
        dataflow_to_sql_converter=dataflow_to_sql_converter,
        sql_client=sql_client,
        dataflow_plan_builder=dataflow_plan_builder,
        query_spec=parsed_query.query_spec,
    )


# TODO: write the following tests when unblocked
# - Query cumulative metric with non-day default_grain (using default grain and non-default grain)
# - Query 2 metrics with different default_grains using metric_time (no grain specified)
# - If default grain is WEEK, query with a higher grain (check that we still get correct values)
# - Query cumulative metric with sub-daily grain
