from __future__ import annotations

import logging
from abc import ABC, abstractmethod
from typing import Optional

from typing_extensions import override

from metricflow_semantics.dag.id_prefix import DynamicIdPrefix
from metricflow_semantics.dag.sequential_id import SequentialIdGenerator
from metricflow_semantics.experimental.singleton_decorator import singleton_dataclass
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


@singleton_dataclass()
class SequentialGraphId(MetricflowGraphId, MetricFlowPrettyFormattable):
    """Graph IDs that are generated sequentially."""

    _str_value: str

    @staticmethod
    def create() -> SequentialGraphId:  # noqa: D102
        return SequentialGraphId(
            _str_value=SequentialIdGenerator.create_next_id(
                DynamicIdPrefix(SequentialGraphId.__class__.__name__)
            ).str_value
        )

    @override
    @property
    def str_value(self) -> str:
        return self.str_value

    @override
    def pretty_format(self, format_context: PrettyFormatContext) -> Optional[str]:
        return self._str_value
