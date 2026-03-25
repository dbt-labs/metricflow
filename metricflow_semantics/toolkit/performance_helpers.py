from __future__ import annotations

import logging
import threading
import time
from typing import ContextManager, Optional, Type, Union

from metricflow_semantics.toolkit.mf_logging.lazy_formattable import LazyFormat
from metricflow_semantics.toolkit.mf_type_aliases import ExceptionTracebackAnyType
from metricflow_semantics.toolkit.time_helpers import PrettyDuration

logger = logging.getLogger(__name__)


class ExecutionTimer(ContextManager["ExecutionTimer"]):
    """A context manager for timing sections of code.

    This and associates classes are a WIP and may be removed.
    """

    def __init__(self, description: Optional[Union[str, LazyFormat]] = None, log_level: int = logging.INFO) -> None:
        """Initializer.

        Args:
            description: If specified, log start / end messages using this value.
            log_level: Log `description` using this log level.
        """
        self._log_level = log_level
        self._local_state = _ExecutionTimerLocalState(description)

    def __enter__(self) -> ExecutionTimer:  # noqa: D105
        description = self._local_state.description
        if description is not None:
            logger.log(level=self._log_level, msg=LazyFormat(lambda: f"[  BEGIN  ] {description}"))
        self._local_state.start_time = time.perf_counter()
        return self

    def __exit__(  # noqa: D105
        self,
        exc_type: Optional[Type[BaseException]],
        exc_val: Optional[BaseException],
        exc_tb: Optional[ExceptionTracebackAnyType],
    ) -> None:
        if self._local_state.start_time is None:
            logger.error(LazyFormat(lambda: f"{self.__class__.__name__} shouldn't exit context without entering."))
            return
        self._local_state.total_duration_for_completed_contexts += time.perf_counter() - self._local_state.start_time
        description = self._local_state.description
        if description is not None:
            total_duration = self._local_state.total_duration_for_completed_contexts
            logger.log(
                level=self._log_level,
                msg=LazyFormat(lambda: f"[   END   ] {description} in {PrettyDuration(total_duration)}"),
            )

    @property
    def total_duration(self) -> PrettyDuration:  # noqa: D102
        return PrettyDuration(self._local_state.total_duration_for_completed_contexts)


class _ExecutionTimerLocalState(threading.local):
    """Mutable thread-local object for storing state for the timer."""

    def __init__(self, description: Optional[Union[str, LazyFormat]]) -> None:  # noqa: D107
        self.start_time: Optional[float] = None
        self.total_duration_for_completed_contexts = 0.0
        self.description = description
