from __future__ import annotations

import logging
import threading
import time
from typing import ContextManager, Optional, Type, Union

from metricflow_semantics.collection_helpers.mf_type_aliases import ExceptionTracebackAnyType
from metricflow_semantics.helpers.time_helpers import PrettyDuration
from metricflow_semantics.mf_logging.lazy_formattable import LazyFormat

logger = logging.getLogger(__name__)


class ExecutionTimer(ContextManager["ExecutionTimer"]):
    """A context manager for timing sections of code.

    This and associates classes are a WIP and may be removed.
    """

    def __init__(self, description: Optional[Union[str, LazyFormat]] = None) -> None:  # noqa: D107
        self._local_state = _ExecutionTimerLocalState(description)

    def __enter__(self) -> ExecutionTimer:  # noqa: D105
        description = self._local_state.description
        if description is not None:
            logger.info(LazyFormat(lambda: f"[  BEGIN  ] {description}"))
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
            logger.info(LazyFormat(lambda: f"[   END   ] {description} in {PrettyDuration(total_duration)}"))

    @property
    def total_duration(self) -> PrettyDuration:  # noqa: D102
        return PrettyDuration(self._local_state.total_duration_for_completed_contexts)


class _ExecutionTimerLocalState(threading.local):
    def __init__(self, description: Optional[Union[str, LazyFormat]]) -> None:  # noqa: D107
        self.start_time: Optional[float] = None
        self.total_duration_for_completed_contexts = 0.0
        self.description = description
