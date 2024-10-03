from __future__ import annotations

import logging

from metricflow_semantics.mf_logging.lazy_formattable import LazyFormat

from metricflow.telemetry.handlers.handlers import PayloadType, TelemetryHandler

logger = logging.getLogger(__name__)


class PythonLoggerTelemetryHandler(TelemetryHandler):
    """A telemetry client that logs data to the Python logger for debugging during tests."""

    def __init__(self, logger_level: int) -> None:  # noqa: D107
        self._logger_level = logger_level

    def _write_log(self, client_id: str, payload: PayloadType) -> None:
        logger.log(level=self._logger_level, msg=LazyFormat("Logging telemetry payload", payload=payload))
