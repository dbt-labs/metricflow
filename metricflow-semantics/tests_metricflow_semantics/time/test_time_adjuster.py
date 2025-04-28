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
    dateutil_adjuster = DateutilTimePeriodAdjuster()

    rows: List[Tuple[str, ...]] = []
    for date_time in date_times_to_check:
        for time_granularity in TimeGranularity:
            if time_granularity.to_int() < TimeGranularity.DAY.to_int():
                continue
            dateutil_start_of_period = dateutil_adjuster.adjust_to_start_of_period(time_granularity, date_time)
            dateutil_end_of_period = dateutil_adjuster.adjust_to_end_of_period(time_granularity, date_time)
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
