from __future__ import annotations

import logging
import time
from typing import ContextManager, Optional, Type, Union

from metricflow_semantics.collection_helpers.mf_type_aliases import ExceptionTracebackAnyType
from metricflow_semantics.helpers.time_helpers import PrettyTimeDelta
from metricflow_semantics.mf_logging.lazy_formattable import LazyFormat
from metricflow_semantics.mf_logging.pretty_print import mf_pformat

logger = logging.getLogger(__name__)


class ExecutionTimer(ContextManager["ExecutionTimer"]):
    """A context manager for timing sections of code.

    This and associates classes are a WIP and may be removed.
    """

    def __init__(self, description: Optional[Union[str, LazyFormat]] = None) -> None:  # noqa: D107
        self._start_time = 0.0
        self._execution_time = 0.0
        self._description = description

    def __enter__(self) -> ExecutionTimer:  # noqa: D105
        description = self._description
        if description is not None:
            logger.info(LazyFormat(lambda: f"[  BEGIN  ] {description}"))
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
        description = self._description
        if description is not None:
            logger.info(
                LazyFormat(lambda: f"[   END   ] {description} in {mf_pformat(PrettyTimeDelta(self._execution_time))}")
            )

    @property
    def execution_time(self) -> PrettyTimeDelta:  # noqa: D102
        return PrettyTimeDelta(self._execution_time)
