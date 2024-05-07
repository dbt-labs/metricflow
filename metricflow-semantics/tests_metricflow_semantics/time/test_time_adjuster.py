from __future__ import annotations

import datetime
import logging
from typing import Dict, List, Sequence, Tuple

import pytest
import tabulate
from _pytest.fixtures import FixtureRequest
from dbt_semantic_interfaces.test_utils import as_datetime
from dbt_semantic_interfaces.type_enums import TimeGranularity
from metricflow_semantics.filters.time_constraint import TimeRangeConstraint
from metricflow_semantics.test_helpers.config_helpers import MetricFlowTestConfiguration
from metricflow_semantics.test_helpers.pandas_adjuster import PandasTimePeriodAdjuster
from metricflow_semantics.test_helpers.snapshot_helpers import assert_str_snapshot_equal
from metricflow_semantics.time.dateutil_adjuster import DateutilTimePeriodAdjuster

logger = logging.getLogger(__name__)


@pytest.fixture(scope="session")
def date_times_to_check() -> Sequence[datetime.datetime]:  # noqa: D103
    date_times = []
    # Cover regular and leap years.
    start_date_time = datetime.datetime(year=2020, month=1, day=1)
    end_date_time = datetime.datetime(year=2021, month=12, day=31)
    current_date_time = start_date_time
    while True:
        date_times.append(current_date_time)
        current_date_time += datetime.timedelta(days=1)
        if current_date_time == end_date_time:
            break
    return date_times


@pytest.fixture(scope="session")
def grain_to_count_in_year() -> Dict[TimeGranularity, int]:
    """Returns the maximum number of times the given item occurs in a year."""
    return {
        TimeGranularity.DAY: 366,
        TimeGranularity.WEEK: 53,
        TimeGranularity.MONTH: 31,
        TimeGranularity.QUARTER: 4,
        TimeGranularity.YEAR: 1,
    }


def test_start_and_end_periods(  # noqa: D103
    request: FixtureRequest,
    mf_test_configuration: MetricFlowTestConfiguration,
    date_times_to_check: Sequence[datetime.datetime],
) -> None:
    pandas_adjuster = PandasTimePeriodAdjuster()
    dateutil_adjuster = DateutilTimePeriodAdjuster()

    rows: List[Tuple[str, ...]] = []
    for date_time in date_times_to_check:
        for time_granularity in TimeGranularity:
            # Pandas implementation of `adjust_to_start_of_period` doesn't support DAY.
            if time_granularity == TimeGranularity.DAY:
                pandas_start_of_period = None
                pandas_end_of_period = None
            else:
                pandas_start_of_period = pandas_adjuster.adjust_to_start_of_period(time_granularity, date_time)
                pandas_end_of_period = pandas_adjuster.adjust_to_end_of_period(time_granularity, date_time)
            dateutil_start_of_period = dateutil_adjuster.adjust_to_start_of_period(time_granularity, date_time)
            dateutil_end_of_period = dateutil_adjuster.adjust_to_end_of_period(time_granularity, date_time)
            assert (
                pandas_start_of_period or dateutil_start_of_period
            ) == dateutil_start_of_period, f"start-of-period mismatch: {date_time.isoformat()} {time_granularity}"
            assert (
                pandas_end_of_period or dateutil_end_of_period
            ) == dateutil_end_of_period, f"end-of-period mismatch: {date_time.isoformat()} {time_granularity}"
            rows.append(
                (
                    date_time.isoformat(),
                    time_granularity.name,
                    dateutil_start_of_period.isoformat(),
                    dateutil_end_of_period.isoformat(),
                )
            )
    assert_str_snapshot_equal(
        request=request,
        mf_test_configuration=mf_test_configuration,
        snapshot_id="results",
        snapshot_str=tabulate.tabulate(rows, headers=["Date", "Grain", "Period Start", "Period End"]),
    )


def test_expand_time_constraint_to_fill_granularity(  # noqa: D103
    date_times_to_check: Sequence[datetime.datetime], grain_to_count_in_year: Dict[TimeGranularity, int]
) -> None:
    pandas_adjuster = PandasTimePeriodAdjuster()
    dateutil_adjuster = DateutilTimePeriodAdjuster()

    test_cases = tuple(
        (start_time, end_time, time_granularity)
        for start_time in date_times_to_check
        for time_granularity in TimeGranularity
        for end_time in (
            start_time + datetime.timedelta(days=day_offset)
            for day_offset in range(grain_to_count_in_year[time_granularity] + 2)
        )
    )

    test_case_count = len(test_cases)
    logger.info(f"There are {test_case_count} test cases")

    finished_count = 0

    for start_time, end_time, time_granularity in test_cases:
        time_constraint = TimeRangeConstraint(start_time=start_time, end_time=end_time)
        pandas_adjuster_result = pandas_adjuster.expand_time_constraint_to_fill_granularity(
            time_constraint, time_granularity
        )

        dateutil_adjuster_result = dateutil_adjuster.expand_time_constraint_to_fill_granularity(
            time_constraint, time_granularity
        )

        assert (
            pandas_adjuster_result == dateutil_adjuster_result
        ), f"Expansion mismatch: {pandas_adjuster_result=} {dateutil_adjuster_result=} {time_granularity=}"
        finished_count += 1
        if finished_count % 100000 == 0 or finished_count == test_case_count:
            logger.info(f"Progress {finished_count / test_case_count * 100:.0f}%")


def test_expand_time_constraint_for_cumulative_metric(  # noqa: D103
    grain_to_count_in_year: Dict[TimeGranularity, int]
) -> None:
    pandas_adjuster = PandasTimePeriodAdjuster()
    dateutil_adjuster = DateutilTimePeriodAdjuster()

    test_cases = tuple(
        (as_datetime("2020-01-01"), time_granularity, count)
        for time_granularity in TimeGranularity
        for count in (range(grain_to_count_in_year[time_granularity] + 2))
    )

    test_case_count = len(test_cases)
    logger.info(f"There are {test_case_count} test cases")

    for start_time, time_granularity, count in test_cases:
        time_constraint = TimeRangeConstraint(start_time=start_time, end_time=start_time)
        pandas_adjuster_result = pandas_adjuster.expand_time_constraint_for_cumulative_metric(
            time_constraint, time_granularity, count
        )

        dateutil_adjuster_result = dateutil_adjuster.expand_time_constraint_for_cumulative_metric(
            time_constraint, time_granularity, count
        )

        assert (
            pandas_adjuster_result == dateutil_adjuster_result
        ), f"Expansion mismatch: {start_time=}, {time_granularity=}, {count=}"
