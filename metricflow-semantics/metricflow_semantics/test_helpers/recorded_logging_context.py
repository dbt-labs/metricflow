from __future__ import annotations

import logging
from contextlib import contextmanager
from typing import Iterator, List, Optional, Sequence, Tuple

from typing_extensions import override


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
def recorded_logging_context(
    logger: logging.Logger, logging_level_int: int
) -> Iterator[Tuple[logging.Logger, RecordingLogHandler]]:
    """Context with a logger (with the given log level) and associated handler to check what was logged.

    The handler records all log records emitted that is appropriate for the given level during this context.
    Log propagation could be disabled in this context to clean test log output, but some issues need to be resolved.
    """
    previous_logging_level = logger.level
    handler = RecordingLogHandler()
    logger.addHandler(handler)
    try:
        logger.setLevel(logging_level_int)
        yield logger, handler
    finally:
        logger.removeHandler(handler)
        logger.setLevel(previous_logging_level)
