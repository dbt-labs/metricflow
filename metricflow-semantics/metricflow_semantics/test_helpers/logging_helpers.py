from __future__ import annotations

import logging
from contextlib import contextmanager
from typing import Iterator, List, Optional, Sequence, Tuple

from typing_extensions import override

from metricflow_semantics.mf_logging.logger_configuration import mf_get_logger
from metricflow_semantics.mf_logging.mf_logger import MetricFlowLogger


class RecordingLogHandler(logging.Handler):
    """A log-record handler that stores them so that they can be checked in tests."""

    def __init__(self) -> None:  # noqa: D107
        super().__init__()
        self._log_records: List[logging.LogRecord] = []

    @override
    def emit(self, record: logging.LogRecord) -> None:
        self._log_records.append(record)

    @property
    def log_records(self) -> Sequence[logging.LogRecord]:
        """Return the log records seen by the handler."""
        return self._log_records

    def get_last_message(self) -> Optional[str]:
        """Return the message in the last log record, or None if this hasn't seen any."""
        if len(self._log_records) == 0:
            return None

        return self._log_records[-1].message


@contextmanager
def recorded_logging_context(logging_level_int: int) -> Iterator[Tuple[MetricFlowLogger, RecordingLogHandler]]:
    """Context with a logger (with the given log level) and associated handler to check what was logged.

    The handler records all log records emitted that is appropriate for the given level during this context.
    Log propagation could be disabled in this context to clean test log output, but some issues need to be resolved.
    """
    mf_logger = mf_get_logger()
    standard_logger = mf_logger.standard_library_logger

    previous_logging_level = standard_logger.level
    handler = RecordingLogHandler()
    standard_logger.addHandler(handler)
    try:
        standard_logger.setLevel(logging_level_int)
        yield mf_logger, handler
    finally:
        standard_logger.removeHandler(handler)
        standard_logger.setLevel(previous_logging_level)
