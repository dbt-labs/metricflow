from __future__ import annotations

import itertools
import logging
from abc import ABC, abstractmethod
from typing import Optional

from typing_extensions import override

from metricflow_semantics.mf_logging.pretty_formattable import MetricFlowPrettyFormattable
from metricflow_semantics.mf_logging.pretty_formatter import PrettyFormatContext

logger = logging.getLogger(__name__)


class MetricflowGraphId(ABC):
    """Interface for an object to identify different graphs."""

    @property
    @abstractmethod
    def str_value(self) -> str:
        """Return a string representation of the ID."""
        raise NotImplementedError


class SequentialGraphId(MetricflowGraphId, MetricFlowPrettyFormattable):
    """Graph IDs that are generated sequentially."""

    # `itertools.count()` returns an iterable that is thread-safe.
    _ID_COUNTER = itertools.count()

    def __init__(self) -> None:
        self._str_value = "id_" + str(next(SequentialGraphId._ID_COUNTER))

    @staticmethod
    def create() -> SequentialGraphId:  # noqa: D102
        return SequentialGraphId()

    @override
    @property
    def str_value(self) -> str:
        return self.str_value

    @override
    def pretty_format(self, format_context: PrettyFormatContext) -> Optional[str]:
        return self._str_value
