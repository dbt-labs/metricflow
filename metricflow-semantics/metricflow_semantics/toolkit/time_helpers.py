from __future__ import annotations

import logging
from functools import cached_property
from typing import Final, Iterable, Optional

from typing_extensions import override

from metricflow_semantics.toolkit.mf_logging.lazy_formattable import LazyFormat
from metricflow_semantics.toolkit.mf_logging.pretty_formattable import MetricFlowPrettyFormattable
from metricflow_semantics.toolkit.mf_logging.pretty_formatter import PrettyFormatContext

logger = logging.getLogger(__name__)

DEFAULT_DECIMAL_COUNT = 2


class PrettyDuration(MetricFlowPrettyFormattable):
    """Wrapper to format durations using the MF pretty-printer."""

    def __init__(self, seconds: float, decimals: int = DEFAULT_DECIMAL_COUNT) -> None:  # noqa: D107
        self.seconds: Final[float] = seconds
        if decimals < 0:
            logger.error(LazyFormat("`decimals` argument must not be negative. Using 0 instead.", decimals=decimals))
            decimals = 0
        self._decimals = decimals

    @cached_property
    def _pretty_seconds(self) -> str:
        return f"{self.seconds:.{self._decimals}f} s"

    @override
    def __str__(self) -> str:
        return self._pretty_seconds

    @override
    def pretty_format(self, format_context: PrettyFormatContext) -> Optional[str]:
        return self._pretty_seconds

    @staticmethod
    def sum(durations: Iterable[PrettyDuration]) -> PrettyDuration:  # noqa: D102
        return PrettyDuration(
            sum((duration.seconds for duration in durations), start=0.0),
            max((duration._decimals for duration in durations), default=DEFAULT_DECIMAL_COUNT),
        )
