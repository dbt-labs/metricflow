from __future__ import annotations

import logging
from abc import ABC, abstractmethod
from typing import Any, Dict, List, Optional, Sequence

from metricflow.telemetry.models import FunctionEndEvent, FunctionStartEvent, TelemetryPayload

PayloadType = Dict[Any, Any]  # type: ignore
logger = logging.getLogger(__name__)


class TelemetryHandler(ABC):
    """Base class to record telemetry to some destination."""

    @abstractmethod
    def _write_log(self, client_id: str, payload: PayloadType) -> None:
        """Subclasses should implement this to log a serialized version of the payload."""
        pass

    def log(
        self,
        client_id: str,
        function_start_event: Optional[FunctionStartEvent] = None,
        function_end_event: Optional[FunctionEndEvent] = None,
    ) -> bool:
        """Log an event to telemetry."""
        payload = TelemetryPayload(
            client_id=client_id,
            function_start_events=(function_start_event,) if function_start_event else (),
            function_end_events=(function_end_event,) if function_end_event else (),
        )
        self._write_log(client_id, payload.dict())
        return True


class ToMemoryTelemetryHandler(TelemetryHandler):
    """Records telemetry events to memory for testing purposes."""

    def __init__(self) -> None:  # noqa: D
        self._payloads: List[TelemetryPayload] = []

    def _write_log(self, client_id: str, payload: PayloadType) -> None:  # noqa: D
        pass

    @property
    def payloads(self) -> Sequence[TelemetryPayload]:  # noqa: D
        return self._payloads

    def log(
        self,
        client_id: str,
        function_start_event: Optional[FunctionStartEvent] = None,
        function_end_event: Optional[FunctionEndEvent] = None,
    ) -> bool:
        """Log an event to telemetry."""
        payload = TelemetryPayload(
            client_id=client_id,
            function_start_events=(function_start_event,) if function_start_event else (),
            function_end_events=(function_end_event,) if function_end_event else (),
        )
        if len(self._payloads) > 10:
            self._payloads.pop()

        self._payloads.append(payload)
        return True
