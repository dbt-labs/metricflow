from __future__ import annotations

import itertools
import logging
from abc import ABC, abstractmethod
from functools import cached_property
from typing import Optional

from typing_extensions import override

from metricflow_semantics.toolkit.comparison_helpers import ComparisonOtherType
from metricflow_semantics.toolkit.mf_logging.pretty_formattable import MetricFlowPrettyFormattable
from metricflow_semantics.toolkit.mf_logging.pretty_formatter import PrettyFormatContext

logger = logging.getLogger(__name__)


class MetricFlowGraphId(ABC):
    """Interface for an object to identify different graphs."""

    @property
    @abstractmethod
    def str_value(self) -> str:
        """Return a string representation of the ID."""
        raise NotImplementedError


class SequentialGraphId(MetricFlowGraphId, MetricFlowPrettyFormattable):
    """Graph IDs that are generated sequentially."""

    # `itertools.count()` returns an iterable that is thread-safe, so this is a way of generating sequential
    # IDs without using a lock.
    _ID_COUNTER = itertools.count()

    def __init__(self) -> None:  # noqa: D107
        self._str_value = "id_" + str(next(SequentialGraphId._ID_COUNTER))

    @staticmethod
    def create() -> SequentialGraphId:  # noqa: D102
        """TODO: Remove and update call sites."""
        return SequentialGraphId()

    @override
    @property
    def str_value(self) -> str:
        return self.str_value

    @override
    def pretty_format(self, format_context: PrettyFormatContext) -> Optional[str]:
        return self._str_value

    @override
    def __eq__(self, other: ComparisonOtherType) -> bool:
        if self.__class__ is not other.__class__:
            return False

        return self._str_value == other._str_value

    @cached_property
    def _cached_hash(self) -> int:
        return hash(self._str_value)

    @override
    def __hash__(self) -> int:
        return self._cached_hash
