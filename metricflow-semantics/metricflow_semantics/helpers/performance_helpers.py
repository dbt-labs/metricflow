from __future__ import annotations

import logging
import time
from dataclasses import dataclass
from typing import ContextManager, Iterable, Optional, Type

from typing_extensions import override

from metricflow_semantics.collection_helpers.mf_type_aliases import ExceptionTracebackAnyType
from metricflow_semantics.experimental.dataclass_helpers import fast_frozen_dataclass
from metricflow_semantics.mf_logging.lazy_formattable import LazyFormat
from metricflow_semantics.mf_logging.pretty_formattable import MetricFlowPrettyFormattable
from metricflow_semantics.mf_logging.pretty_formatter import PrettyFormatContext

logger = logging.getLogger(__name__)


class ExecutionTimer(ContextManager["ExecutionTimer"]):
    """A context manager for timing sections of code.

    This and associates classes are a WIP and may be removed.
    """

    def __init__(self, description: Optional[str] = None) -> None:  # noqa: D107
        self._start_time: Optional[float] = None
        self._execution_time = 0.0
        self._description = description

    @property
    def execution_time_float(self) -> float:  # noqa: D102
        return self._execution_time

    def __enter__(self) -> ExecutionTimer:  # noqa: D105
        if self._description is not None:
            logger.info(LazyFormat(lambda: f"[  BEGIN  ] {self._description}"))
        self._start_time = time.perf_counter()
        return self

    def __exit__(  # noqa: D105
        self,
        exc_type: Optional[Type[BaseException]],
        exc_val: Optional[BaseException],
        exc_tb: Optional[ExceptionTracebackAnyType],
    ) -> None:
        if self._start_time is None:
            raise RuntimeError("Context manager shouldn't exit without first entering.")

        self._execution_time += time.perf_counter() - self._start_time
        if self._description is not None:
            logger.info(LazyFormat(lambda: f"[   END   ] {self._description} in {self._execution_time:.2f}s"))

    @property
    def execution_time(self) -> ExecutionTime:  # noqa: D102
        return ExecutionTime(execution_time=self._execution_time)


@fast_frozen_dataclass()
class ExecutionTime(MetricFlowPrettyFormattable):
    """Encapsulates the execution time so that it can be formatted consistently in logs."""

    execution_time: float

    @override
    def pretty_format(self, format_context: PrettyFormatContext) -> Optional[str]:
        return f"{self.execution_time:.2f}s"

    @staticmethod
    def sum(times: Iterable[ExecutionTime]) -> ExecutionTime:  # noqa: D102
        return ExecutionTime(execution_time=sum(execution_time.execution_time for execution_time in times))


@dataclass
class ExecutionTimedResult:
    """A mixin to include the execution time in result objects."""

    execution_time: ExecutionTime
