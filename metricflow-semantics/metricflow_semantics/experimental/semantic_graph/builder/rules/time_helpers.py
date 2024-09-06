from typing import Sequence

from dbt_semantic_interfaces.type_enums import TimeGranularity


class TimeHelper:
    # TODO: Remove this before PR.
    ALLOWED_TIME_GRAINS = (TimeGranularity.DAY, TimeGranularity.YEAR)
    ALL_TIME_GRAINS = tuple(TimeGranularity)

    @staticmethod
    def more_coarse_time_grains(time_grain: TimeGranularity) -> Sequence[TimeGranularity]:
        return sorted(
            other_time_grain
            for other_time_grain in TimeHelper.ALLOWED_TIME_GRAINS
            if other_time_grain.to_int() > time_grain.to_int()
        )

    @staticmethod
    def more_fine_time_grains(time_grain: TimeGranularity) -> Sequence[TimeGranularity]:
        return sorted(
            other_time_grain
            for other_time_grain in TimeHelper.ALLOWED_TIME_GRAINS
            if other_time_grain.to_int() < time_grain.to_int()
        )