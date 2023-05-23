from __future__ import annotations

import logging
from abc import ABC, abstractmethod
from datetime import datetime
from typing import Any, Dict, List, Optional, Sequence

from rudder_analytics.client import Client as RudderstackClient

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


class RudderstackTelemetryHandler(TelemetryHandler):
    """A telemetry client that logs data to Rudderstack."""

    RUDDERSTACK_EVENT_NAME = "MF_OSS"

    # Key for logging to Transform
    TFD_WRITE_KEY: str = "2777X6I2uHwXmCM9hB6GPKcegdn"
    TFD_DATA_PLANE: str = "https://transformdprul.dataplane.rudderstack.com"

    def __init__(  # noqa: D
        self,
        data_plane_url: str = TFD_DATA_PLANE,
        write_key: str = TFD_WRITE_KEY,
    ) -> None:
        self._rudderstack_client = RudderstackClient(
            write_key=write_key, host=data_plane_url, debug=False, on_error=None, send=True, sync_mode=False
        )

    def _write_log(self, client_id: str, payload: PayloadType) -> None:  # noqa: D
        """Write log to rudderstack.

        Added context to handle the error, but seems odd.

        >       msg['context']['traits']['anonymousId'] = msg['anonymousId']
        E       KeyError: 'traits'

        /usr/local/lib/python3.8/site-packages/rudder_analytics/client.py:243: KeyError
        """
        context: PayloadType = {"traits": {}}
        self._rudderstack_client.track(
            anonymous_id=client_id,
            event=RudderstackTelemetryHandler.RUDDERSTACK_EVENT_NAME,
            timestamp=datetime.now(),
            properties=payload,
            context=context,
        )


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
