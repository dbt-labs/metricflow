from __future__ import annotations

import logging
from functools import cached_property
from typing import Final, Iterable, Optional

from typing_extensions import override

from metricflow_semantics.mf_logging.lazy_formattable import LazyFormat
from metricflow_semantics.mf_logging.pretty_formattable import MetricFlowPrettyFormattable
from metricflow_semantics.mf_logging.pretty_formatter import PrettyFormatContext

logger = logging.getLogger(__name__)


class PrettyTimeDelta(MetricFlowPrettyFormattable):
    """Wrapper to format times using the MF pretty-printer."""

    def __init__(self, seconds: float, decimals: int = 2) -> None:  # noqa: D107
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
    def sum(pretty_times: Iterable[PrettyTimeDelta]) -> PrettyTimeDelta:  # noqa: D102
        return PrettyTimeDelta(
            sum(pretty_time.seconds for pretty_time in pretty_times),
            max(pretty_time._decimals for pretty_time in pretty_times),
        )
