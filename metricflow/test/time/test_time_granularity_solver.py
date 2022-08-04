import datetime

import pytest

from metricflow.constraints.time_constraint import TimeRangeConstraint
from metricflow.dataflow.builder.node_data_set import DataflowPlanNodeOutputDataSetResolver
from metricflow.dataflow.builder.source_node import SourceNodeBuilder
from metricflow.dataset.convert_data_source import DataSourceToDataSetConverter
from metricflow.dataset.data_source_adapter import DataSourceDataSet
from metricflow.dataset.dataset import DataSet
from metricflow.model.semantic_model import SemanticModel
from metricflow.plan_conversion.column_resolver import DefaultColumnAssociationResolver
from metricflow.plan_conversion.time_spine import TimeSpineSource
from metricflow.specs import MetricSpec
from metricflow.test.time.metric_time_dimension import MTD_SPEC_DAY, MTD_SPEC_MONTH, MTD_SPEC_YEAR, MTD_REFERENCE
from metricflow.time.time_granularity import TimeGranularity
from metricflow.time.time_granularity_solver import (
    TimeGranularitySolver,
    PartialTimeDimensionSpec,
    RequestTimeGranularityException,
)


@pytest.fixture(scope="session")
def time_granularity_solver(  # noqa: D
    extended_date_semantic_model: SemanticModel,
    time_spine_source: TimeSpineSource,
) -> TimeGranularitySolver:
    column_association_resolver = DefaultColumnAssociationResolver(extended_date_semantic_model)
    node_output_resolver = DataflowPlanNodeOutputDataSetResolver[DataSourceDataSet](
        column_association_resolver=column_association_resolver,
        semantic_model=extended_date_semantic_model,
        time_spine_source=time_spine_source,
    )
    to_data_set_converter = DataSourceToDataSetConverter(column_association_resolver)
    source_data_sets = [
        to_data_set_converter.create_sql_source_data_set(x)
        for x in extended_date_semantic_model.user_configured_model.data_sources
    ]

    source_node_builder = SourceNodeBuilder(extended_date_semantic_model)
    source_nodes = source_node_builder.create_from_data_sets(source_data_sets)
    return TimeGranularitySolver(
        semantic_model=extended_date_semantic_model,
        source_nodes=source_nodes,
        node_output_resolver=node_output_resolver,
    )


def test_validate_day_granuarity_for_day_metric(time_granularity_solver: TimeGranularitySolver) -> None:  # noqa: D
    time_granularity_solver.validate_time_granularity(
        metric_specs=[MetricSpec(element_name="bookings")],
        time_dimension_specs=[DataSet.metric_time_dimension_spec(TimeGranularity.DAY)],
    )


def test_validate_month_granuarity_for_day_metric(time_granularity_solver: TimeGranularitySolver) -> None:  # noqa: D
    time_granularity_solver.validate_time_granularity(
        metric_specs=[MetricSpec(element_name="bookings")],
        time_dimension_specs=[DataSet.metric_time_dimension_spec(TimeGranularity.MONTH)],
    )


def test_validate_month_granuarity_for_month_metric(time_granularity_solver: TimeGranularitySolver) -> None:  # noqa: D
    time_granularity_solver.validate_time_granularity(
        metric_specs=[MetricSpec(element_name="bookings_monthly")],
        time_dimension_specs=[DataSet.metric_time_dimension_spec(TimeGranularity.MONTH)],
    )


def test_validate_month_granuarity_for_day_and_month_metrics(  # noqa: D
    time_granularity_solver: TimeGranularitySolver,
) -> None:
    time_granularity_solver.validate_time_granularity(
        metric_specs=[MetricSpec(element_name="bookings"), MetricSpec(element_name="bookings_monthly")],
        time_dimension_specs=[DataSet.metric_time_dimension_spec(TimeGranularity.MONTH)],
    )


def test_validate_year_granularity_for_day_and_month_metrics(  # noqa: D
    time_granularity_solver: TimeGranularitySolver,
) -> None:
    time_granularity_solver.validate_time_granularity(
        metric_specs=[MetricSpec(element_name="bookings"), MetricSpec(element_name="bookings_monthly")],
        time_dimension_specs=[DataSet.metric_time_dimension_spec(TimeGranularity.YEAR)],
    )


def test_validate_day_granuarity_for_month_metric(time_granularity_solver: TimeGranularitySolver) -> None:  # noqa: D
    with pytest.raises(RequestTimeGranularityException):
        time_granularity_solver.validate_time_granularity(
            metric_specs=[MetricSpec(element_name="bookings_monthly")],
            time_dimension_specs=[DataSet.metric_time_dimension_spec(TimeGranularity.DAY)],
        )


def test_validate_day_granularity_for_day_and_month_metric(  # noqa: D
    time_granularity_solver: TimeGranularitySolver,
) -> None:
    with pytest.raises(RequestTimeGranularityException):
        time_granularity_solver.validate_time_granularity(
            metric_specs=[MetricSpec(element_name="bookings"), MetricSpec(element_name="bookings_monthly")],
            time_dimension_specs=[DataSet.metric_time_dimension_spec(TimeGranularity.DAY)],
        )


PARTIAL_PTD_SPEC = PartialTimeDimensionSpec(element_name=DataSet.metric_time_dimension_name(), identifier_links=())


def test_granularity_solution_for_day_metric(time_granularity_solver: TimeGranularitySolver) -> None:  # noqa: D
    assert time_granularity_solver.resolve_granularity_for_partial_time_dimension_specs(
        metric_specs=[MetricSpec(element_name="bookings")],
        partial_time_dimension_specs=[PARTIAL_PTD_SPEC],
        metric_time_dimension_reference=MTD_REFERENCE,
    ) == {
        PARTIAL_PTD_SPEC: MTD_SPEC_DAY,
    }


def test_granularity_solution_for_month_metric(time_granularity_solver: TimeGranularitySolver) -> None:  # noqa: D
    assert time_granularity_solver.resolve_granularity_for_partial_time_dimension_specs(
        metric_specs=[MetricSpec(element_name="bookings_monthly")],
        partial_time_dimension_specs=[PARTIAL_PTD_SPEC],
        metric_time_dimension_reference=MTD_REFERENCE,
    ) == {
        PARTIAL_PTD_SPEC: MTD_SPEC_MONTH,
    }


def test_granularity_solution_for_day_and_month_metrics(  # noqa: D
    time_granularity_solver: TimeGranularitySolver,
) -> None:
    assert time_granularity_solver.resolve_granularity_for_partial_time_dimension_specs(
        metric_specs=[MetricSpec(element_name="bookings"), MetricSpec(element_name="bookings_monthly")],
        partial_time_dimension_specs=[PARTIAL_PTD_SPEC],
        metric_time_dimension_reference=MTD_REFERENCE,
    ) == {PARTIAL_PTD_SPEC: MTD_SPEC_MONTH}


def test_time_granularity_parameter(  # noqa: D
    time_granularity_solver: TimeGranularitySolver,
) -> None:
    assert time_granularity_solver.resolve_granularity_for_partial_time_dimension_specs(
        metric_specs=[MetricSpec(element_name="bookings"), MetricSpec(element_name="bookings_monthly")],
        partial_time_dimension_specs=[PARTIAL_PTD_SPEC],
        metric_time_dimension_reference=MTD_REFERENCE,
        time_granularity=TimeGranularity.YEAR,
    ) == {PARTIAL_PTD_SPEC: MTD_SPEC_YEAR}


def test_invalid_time_granularity_parameter(  # noqa: D
    time_granularity_solver: TimeGranularitySolver,
) -> None:
    with pytest.raises(RequestTimeGranularityException, match="Can't use time granularity.*"):
        time_granularity_solver.resolve_granularity_for_partial_time_dimension_specs(
            metric_specs=[MetricSpec(element_name="bookings"), MetricSpec(element_name="bookings_monthly")],
            partial_time_dimension_specs=[PARTIAL_PTD_SPEC],
            metric_time_dimension_reference=MTD_REFERENCE,
            time_granularity=TimeGranularity.DAY,
        )


def test_adjusted_time_constraint(time_granularity_solver: TimeGranularitySolver) -> None:  # noqa: D
    assert time_granularity_solver.adjust_time_range_to_granularity(
        time_range_constraint=TimeRangeConstraint(
            start_time=datetime.datetime(2020, 1, 1),
            end_time=datetime.datetime(2020, 1, 15),
        ),
        time_granularity=TimeGranularity.DAY,
    ) == TimeRangeConstraint(
        start_time=datetime.datetime(2020, 1, 1),
        end_time=datetime.datetime(2020, 1, 15),
    )

    assert time_granularity_solver.adjust_time_range_to_granularity(
        time_range_constraint=TimeRangeConstraint(
            start_time=datetime.datetime(2020, 1, 1),
            end_time=datetime.datetime(2020, 1, 15),
        ),
        time_granularity=TimeGranularity.MONTH,
    ) == TimeRangeConstraint(
        start_time=datetime.datetime(2020, 1, 1),
        end_time=datetime.datetime(2020, 1, 31),
    )

    assert time_granularity_solver.adjust_time_range_to_granularity(
        time_range_constraint=TimeRangeConstraint(
            start_time=datetime.datetime(2020, 1, 15),
            end_time=datetime.datetime(2020, 2, 15),
        ),
        time_granularity=TimeGranularity.MONTH,
    ) == TimeRangeConstraint(
        start_time=datetime.datetime(2020, 1, 1),
        end_time=datetime.datetime(2020, 2, 29),
    )


def test_adjusted_time_constraint_for_week_granularity(
    time_granularity_solver: TimeGranularitySolver,
) -> None:
    """Tests the time range adjustment behavior for weekly granularities"""
    assert time_granularity_solver.adjust_time_range_to_granularity(
        time_range_constraint=TimeRangeConstraint(
            start_time=datetime.datetime(2020, 1, 8),
            end_time=datetime.datetime(2020, 1, 15),
        ),
        time_granularity=TimeGranularity.WEEK,
    ) == TimeRangeConstraint(
        start_time=datetime.datetime(2020, 1, 6),
        end_time=datetime.datetime(2020, 1, 19),
    )


def test_adjusted_time_constraint_at_boundaries(
    time_granularity_solver: TimeGranularitySolver,
) -> None:
    """Tests the time range adjustment behavior at the min / max boundaries"""
    assert (
        time_granularity_solver.adjust_time_range_to_granularity(
            time_range_constraint=TimeRangeConstraint(
                start_time=TimeRangeConstraint.all_time().start_time + datetime.timedelta(days=-30),
                end_time=TimeRangeConstraint.all_time().end_time + datetime.timedelta(days=30),
            ),
            time_granularity=TimeGranularity.WEEK,
        )
        == TimeRangeConstraint.all_time()
    )
