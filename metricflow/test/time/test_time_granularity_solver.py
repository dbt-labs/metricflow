import datetime

import pytest

from metricflow.constraints.time_constraint import TimeRangeConstraint
from metricflow.model.semantic_model import SemanticModel
from metricflow.specs import MetricSpec, TimeDimensionSpec, TimeDimensionReference
from metricflow.time.time_granularity import TimeGranularity
from metricflow.time.time_granularity_solver import (
    TimeGranularitySolver,
    PartialTimeDimensionSpec,
    RequestTimeGranularityException,
)


@pytest.fixture(scope="session")
def time_granularity_solver(  # noqa: D
    extended_date_semantic_model: SemanticModel,
) -> TimeGranularitySolver:
    return TimeGranularitySolver(extended_date_semantic_model)


def test_validate_day_granuarity_for_day_metric(time_granularity_solver: TimeGranularitySolver) -> None:  # noqa: D
    time_granularity_solver.validate_time_granularity(
        metric_specs=[MetricSpec(element_name="bookings")],
        time_dimension_specs=[
            TimeDimensionSpec(element_name="ds", identifier_links=(), time_granularity=TimeGranularity.DAY)
        ],
    )


def test_validate_month_granuarity_for_day_metric(time_granularity_solver: TimeGranularitySolver) -> None:  # noqa: D
    time_granularity_solver.validate_time_granularity(
        metric_specs=[MetricSpec(element_name="bookings")],
        time_dimension_specs=[
            TimeDimensionSpec(element_name="ds", identifier_links=(), time_granularity=TimeGranularity.MONTH)
        ],
    )


def test_validate_month_granuarity_for_month_metric(time_granularity_solver: TimeGranularitySolver) -> None:  # noqa: D
    time_granularity_solver.validate_time_granularity(
        metric_specs=[MetricSpec(element_name="bookings_monthly")],
        time_dimension_specs=[
            TimeDimensionSpec(element_name="ds", identifier_links=(), time_granularity=TimeGranularity.MONTH)
        ],
    )


def test_validate_month_granuarity_for_day_and_month_metrics(  # noqa: D
    time_granularity_solver: TimeGranularitySolver,
) -> None:
    time_granularity_solver.validate_time_granularity(
        metric_specs=[MetricSpec(element_name="bookings"), MetricSpec(element_name="bookings_monthly")],
        time_dimension_specs=[
            TimeDimensionSpec(element_name="ds", identifier_links=(), time_granularity=TimeGranularity.MONTH)
        ],
    )


def test_validate_year_granularity_for_day_and_month_metrics(  # noqa: D
    time_granularity_solver: TimeGranularitySolver,
) -> None:
    time_granularity_solver.validate_time_granularity(
        metric_specs=[MetricSpec(element_name="bookings"), MetricSpec(element_name="bookings_monthly")],
        time_dimension_specs=[
            TimeDimensionSpec(element_name="ds", identifier_links=(), time_granularity=TimeGranularity.YEAR)
        ],
    )


def test_validate_day_granuarity_for_month_metric(time_granularity_solver: TimeGranularitySolver) -> None:  # noqa: D
    with pytest.raises(RequestTimeGranularityException):
        time_granularity_solver.validate_time_granularity(
            metric_specs=[MetricSpec(element_name="bookings_monthly")],
            time_dimension_specs=[
                TimeDimensionSpec(element_name="ds", identifier_links=(), time_granularity=TimeGranularity.DAY)
            ],
        )


def test_validate_day_granularity_for_day_and_month_metric(  # noqa: D
    time_granularity_solver: TimeGranularitySolver,
) -> None:
    with pytest.raises(RequestTimeGranularityException):
        time_granularity_solver.validate_time_granularity(
            metric_specs=[MetricSpec(element_name="bookings"), MetricSpec(element_name="bookings_monthly")],
            time_dimension_specs=[
                TimeDimensionSpec(element_name="ds", identifier_links=(), time_granularity=TimeGranularity.DAY)
            ],
        )


def test_granularity_solution_for_day_metric(time_granularity_solver: TimeGranularitySolver) -> None:  # noqa: D
    assert time_granularity_solver.resolve_granularity_for_partial_time_dimension_specs(
        metric_specs=[MetricSpec(element_name="bookings")],
        partial_time_dimension_specs=[PartialTimeDimensionSpec(element_name="ds", identifier_links=())],
        primary_time_dimension_reference=TimeDimensionReference(element_name="ds"),
    ) == {
        PartialTimeDimensionSpec(element_name="ds", identifier_links=()): TimeDimensionSpec(
            element_name="ds", identifier_links=(), time_granularity=TimeGranularity.DAY
        ),
    }


def test_granularity_solution_for_month_metric(time_granularity_solver: TimeGranularitySolver) -> None:  # noqa: D
    assert time_granularity_solver.resolve_granularity_for_partial_time_dimension_specs(
        metric_specs=[MetricSpec(element_name="bookings_monthly")],
        partial_time_dimension_specs=[PartialTimeDimensionSpec(element_name="ds", identifier_links=())],
        primary_time_dimension_reference=TimeDimensionReference(element_name="ds"),
    ) == {
        PartialTimeDimensionSpec(element_name="ds", identifier_links=()): TimeDimensionSpec(
            element_name="ds", identifier_links=(), time_granularity=TimeGranularity.MONTH
        ),
    }


def test_granularity_solution_for_day_and_month_metrics(  # noqa: D
    time_granularity_solver: TimeGranularitySolver,
) -> None:
    assert time_granularity_solver.resolve_granularity_for_partial_time_dimension_specs(
        metric_specs=[MetricSpec(element_name="bookings"), MetricSpec(element_name="bookings_monthly")],
        partial_time_dimension_specs=[PartialTimeDimensionSpec(element_name="ds", identifier_links=())],
        primary_time_dimension_reference=TimeDimensionReference(element_name="ds"),
    ) == {
        PartialTimeDimensionSpec(element_name="ds", identifier_links=()): TimeDimensionSpec(
            element_name="ds", identifier_links=(), time_granularity=TimeGranularity.MONTH
        )
    }


def test_time_granularity_parameter(  # noqa: D
    time_granularity_solver: TimeGranularitySolver,
) -> None:
    assert time_granularity_solver.resolve_granularity_for_partial_time_dimension_specs(
        metric_specs=[MetricSpec(element_name="bookings"), MetricSpec(element_name="bookings_monthly")],
        partial_time_dimension_specs=[PartialTimeDimensionSpec(element_name="ds", identifier_links=())],
        primary_time_dimension_reference=TimeDimensionReference(element_name="ds"),
        time_granularity=TimeGranularity.YEAR,
    ) == {
        PartialTimeDimensionSpec(element_name="ds", identifier_links=()): TimeDimensionSpec(
            element_name="ds", identifier_links=(), time_granularity=TimeGranularity.YEAR
        )
    }


def test_invalid_time_granularity_parameter(  # noqa: D
    time_granularity_solver: TimeGranularitySolver,
) -> None:
    with pytest.raises(RequestTimeGranularityException, match="Can't use time granularity.*"):
        time_granularity_solver.resolve_granularity_for_partial_time_dimension_specs(
            metric_specs=[MetricSpec(element_name="bookings"), MetricSpec(element_name="bookings_monthly")],
            partial_time_dimension_specs=[PartialTimeDimensionSpec(element_name="ds", identifier_links=())],
            primary_time_dimension_reference=TimeDimensionReference(element_name="ds"),
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
