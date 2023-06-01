from __future__ import annotations

import datetime
from abc import ABC
from enum import Enum, IntEnum
from typing import Optional, Tuple

from dbt_semantic_interfaces.implementations.base import FrozenBaseModel

EVENT_SCHEMA = "v1.0"


class TelemetryLevel(IntEnum):
    """Determines the level of a telemetry event."""

    USAGE = 10
    ERROR = 20
    EXCEPTION = 30
    OFF = 40


class TelemetryEventName(Enum):
    """Names of all possible telemetry events."""

    FUNCTION_START = "FUNCTION_START"
    FUNCTION_END = "FUNCTION_END"


class TelemetryEvent(ABC, FrozenBaseModel):
    """Base class for events that are sent via telemetry for analysis."""

    event_name: str
    event_schema: str
    event_time: datetime.datetime
    level_name: str


class FunctionStartEvent(TelemetryEvent, FrozenBaseModel):
    """Start of a function call."""

    # An ID to uniquely identify a function call so that it can be matched with FunctionEndEvent. Seems unlikely that
    # there would be mismatched events.
    invocation_id: str
    module_name: str
    function_name: str

    @staticmethod
    def create(  # noqa: D
        event_time: datetime.datetime,
        level_name: str,
        invocation_id: str,
        module_name: str,
        function_name: str,
    ) -> FunctionStartEvent:
        return FunctionStartEvent(
            event_name=TelemetryEventName.FUNCTION_START.name,
            event_schema=EVENT_SCHEMA,
            event_time=event_time,
            level_name=level_name,
            invocation_id=invocation_id,
            module_name=module_name,
            function_name=function_name,
        )


class FunctionEndEvent(TelemetryEvent, FrozenBaseModel):
    """Similar to FunctionStartEvent but with runtime and exception trace if the function fails."""

    invocation_id: str
    module_name: str
    function_name: str
    runtime: float
    exception_trace: Optional[str] = None

    @staticmethod
    def create(  # noqa: D
        event_time: datetime.datetime,
        level_name: str,
        invocation_id: str,
        module_name: str,
        function_name: str,
        runtime: float,
        exception_trace: Optional[str] = None,
    ) -> FunctionEndEvent:
        return FunctionEndEvent(
            event_name=TelemetryEventName.FUNCTION_END.name,
            event_schema=EVENT_SCHEMA,
            event_time=event_time,
            level_name=level_name,
            invocation_id=invocation_id,
            module_name=module_name,
            function_name=function_name,
            runtime=runtime,
            exception_trace=exception_trace,
        )


class TelemetryPayload(FrozenBaseModel):
    """Payload that can be easily serialized to JSON."""

    client_id: str
    function_start_events: Tuple[FunctionStartEvent, ...] = ()
    function_end_events: Tuple[FunctionEndEvent, ...] = ()
    payload_schema: str = EVENT_SCHEMA
