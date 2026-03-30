from __future__ import annotations

import logging
from functools import cached_property

from metricflow_semantics.toolkit.dataclass_helpers import fast_frozen_dataclass

from metricflow_semantic_interfaces.dataclass_serialization import SerializableDataclass
from metricflow_semantic_interfaces.protocols import MetricTimeWindow
from metricflow_semantic_interfaces.type_enums import TimeGranularity

logger = logging.getLogger(__name__)


@fast_frozen_dataclass()
class TimeWindow(SerializableDataclass):
    """An immutable version of `PydanticMetricTimeWindow`."""

    count: int
    granularity: str

    @staticmethod
    def create_from_dsi_time_window(time_window: MetricTimeWindow) -> TimeWindow:  # noqa: D102
        return TimeWindow(
            count=time_window.count,
            granularity=time_window.granularity,
        )

    @cached_property
    def is_standard_granularity(self) -> bool:
        """Returns whether the window uses standard TimeGranularity."""
        return self.granularity.casefold() in {item.value.casefold() for item in TimeGranularity}
