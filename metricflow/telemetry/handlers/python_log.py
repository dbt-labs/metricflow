import logging
import textwrap

from metricflow.object_utils import pformat_big_objects
from metricflow.telemetry.handlers.handlers import TelemetryHandler, PayloadType

logger = logging.getLogger(__name__)


class PythonLoggerTelemetryHandler(TelemetryHandler):
    """A telemetry client that logs data to the Python logger for debugging during tests."""

    def __init__(self, logger_level: int) -> None:  # noqa: D
        self._logger_level = logger_level

    def _write_log(self, client_id: str, payload: PayloadType) -> None:  # noqa: D
        logger.log(
            level=self._logger_level,
            msg=f"Logging telemetry payload:\n{textwrap.indent(pformat_big_objects(payload), prefix='    ')}",
        )
