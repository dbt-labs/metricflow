from __future__ import annotations

import datetime
import logging
from typing import Dict, List, Sequence, Tuple

import pytest
import tabulate
from _pytest.fixtures import FixtureRequest
from dbt_semantic_interfaces.type_enums import TimeGranularity
from metricflow_semantics.test_helpers.config_helpers import MetricFlowTestConfiguration
from metricflow_semantics.test_helpers.snapshot_helpers import assert_str_snapshot_equal
from metricflow_semantics.time.dateutil_adjuster import DateutilTimePeriodAdjuster

logger = logging.getLogger(__name__)


@pytest.fixture(scope="session")
def date_times_to_check() -> Sequence[datetime.datetime]:  # noqa: D103
    date_times = []
    # Cover leap years, non-leap years
    # 1900 was tested to work, though that requires a change to `TimeRangeConstraint.ALL_TIME_BEGIN`
    for year in (2000, 2000, 2021):
        start_date_time = datetime.datetime(year=year, month=1, day=1)
        end_date_time = datetime.datetime(year=year, month=12, day=31)
        current_date_time = start_date_time
        while True:
            date_times.append(current_date_time)
            current_date_time += datetime.timedelta(days=1)
            if current_date_time == end_date_time:
                break

    # Add specific times throughout the day to test sub-day granularities (hour, minute, second)
    # Test various hours, minutes, and seconds across different dates
    test_date = datetime.datetime(year=2021, month=6, day=15)
    times_to_test = [
        (0, 0, 0),  # Midnight
        (0, 0, 30),  # 30 seconds past midnight
        (0, 30, 0),  # 30 minutes past midnight
        (0, 30, 45),  # 30 minutes 45 seconds past midnight
        (12, 0, 0),  # Noon
        (12, 30, 30),  # 12:30:30 PM
        (23, 59, 59),  # Last second of day
        (1, 1, 1),  # 1:01:01 AM
        (13, 45, 22),  # 1:45:22 PM
    ]
    for hour, minute, second in times_to_test:
        date_times.append(
            datetime.datetime(
                year=test_date.year, month=test_date.month, day=test_date.day, hour=hour, minute=minute, second=second
            )
        )

    return date_times


@pytest.fixture(scope="session")
def grain_to_count_in_year() -> Dict[TimeGranularity, int]:
    """Returns the maximum number of times the given item occurs in a year."""
    return {
        TimeGranularity.SECOND: 31622400,  # 366 days * 24 hours * 60 minutes * 60 seconds
        TimeGranularity.MINUTE: 527040,  # 366 days * 24 hours * 60 minutes
        TimeGranularity.HOUR: 8784,  # 366 days * 24 hours
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
    dateutil_adjuster = DateutilTimePeriodAdjuster()

    rows: List[Tuple[str, ...]] = []
    for date_time in date_times_to_check:
        for time_granularity in TimeGranularity:
            # Skip sub-second granularities as they are not supported
            if time_granularity.to_int() < TimeGranularity.SECOND.to_int():
                continue
            dateutil_start_of_period = dateutil_adjuster.adjust_to_start_of_period(time_granularity, date_time)
            dateutil_end_of_period = dateutil_adjuster.adjust_to_end_of_period(time_granularity, date_time)

            # Validate that end of period is actually at the end, not the beginning
            # For all granularities, the end should be >= start
            assert dateutil_end_of_period >= dateutil_start_of_period, (
                f"End of period {dateutil_end_of_period.isoformat()} should be >= start {dateutil_start_of_period.isoformat()} "
                f"for {time_granularity.name} granularity on {date_time.isoformat()}"
            )

            # For MINUTE and larger granularities, the end time should always be the last second of the period
            if time_granularity.to_int() >= TimeGranularity.MINUTE.to_int():
                assert dateutil_end_of_period.second == 59, (
                    f"End of {time_granularity.name} period should have second=59, "
                    f"got {dateutil_end_of_period.isoformat()} for input {date_time.isoformat()}"
                )
            if time_granularity.to_int() >= TimeGranularity.HOUR.to_int():
                assert dateutil_end_of_period.minute == 59, (
                    f"End of {time_granularity.name} period should have minute=59, "
                    f"got {dateutil_end_of_period.isoformat()} for input {date_time.isoformat()}"
                )
            if time_granularity.to_int() >= TimeGranularity.DAY.to_int():
                assert dateutil_end_of_period.hour == 23, (
                    f"End of {time_granularity.name} period should have hour=23, "
                    f"got {dateutil_end_of_period.isoformat()} for input {date_time.isoformat()}"
                )

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
        snapshot_configuration=mf_test_configuration,
        snapshot_id="results",
        snapshot_str=tabulate.tabulate(rows, headers=["Date", "Grain", "Period Start", "Period End"]),
    )
