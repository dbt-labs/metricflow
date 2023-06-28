from __future__ import annotations

import datetime
import functools
import logging
import os
import platform
import sys
import time
import traceback
import uuid
from hashlib import sha256
from typing import Callable, List, Optional, TypeVar

from typing_extensions import ParamSpec

from metricflow.random_id import random_id
from metricflow.telemetry.handlers.handlers import (
    RudderstackTelemetryHandler,
    TelemetryHandler,
    ToMemoryTelemetryHandler,
)
from metricflow.telemetry.handlers.python_log import PythonLoggerTelemetryHandler
from metricflow.telemetry.models import FunctionEndEvent, FunctionStartEvent, TelemetryLevel

logger = logging.getLogger(__name__)


class TelemetryReporter:
    """Reports telemetry for improving product experience."""

    # Session ID to use when requesting a non-uniquely identifiable ID.
    FULLY_ANONYMOUS_CLIENT_ID = "anonymous"
    ENV_EMAIL_OVERRIDE = "METRICFLOW_CLIENT_EMAIL"

    def __init__(self, report_levels_higher_or_equal_to: TelemetryLevel, fully_anonymous: bool = False) -> None:
        """If fully_anonymous is set, use a client_id that is not unique."""
        self._report_levels_higher_or_equal_to = report_levels_higher_or_equal_to
        self._fully_anonymous = fully_anonymous
        self._email = os.getenv(TelemetryReporter.ENV_EMAIL_OVERRIDE)

        if fully_anonymous:
            self._client_id = TelemetryReporter.FULLY_ANONYMOUS_CLIENT_ID
        elif self._email:
            self._client_id = self._email
        else:
            self._client_id = TelemetryReporter._create_client_id()

        # For testing
        self._test_handler = ToMemoryTelemetryHandler()
        self._handlers: List[TelemetryHandler] = []

    @staticmethod
    def _create_client_id() -> str:
        """Creates an identifier for the current user based on their current environment.

        More specifically, this function creates a SHA-256 hash based on the system platform, release, and MAC address.

        The created client ID is not guaranteed to be unique by user.
        """
        # getnode() returns the MAC.
        id_str = "_".join([sys.platform, platform.release(), str(uuid.getnode())])
        return sha256(id_str.encode("utf-8")).hexdigest()

    def add_python_log_handler(self) -> None:  # noqa: D
        self._handlers.append(PythonLoggerTelemetryHandler(logger_level=logging.INFO))

    def add_rudderstack_handler(self) -> None:  # noqa: D
        self._handlers.append(RudderstackTelemetryHandler())

    def add_test_handler(self) -> None:
        """See test_handler."""
        self._handlers.append(self._test_handler)

    @property
    def test_handler(self) -> ToMemoryTelemetryHandler:
        """Used for testing only to verify that the handlers are getting the right events."""
        return self._test_handler

    def log_function_start(  # noqa: D
        self,
        invocation_id: str,
        module_name: str,
        function_name: str,
    ) -> None:
        """Logs the start of a function call when the logging level >= USAGE.

        invocation_id is to uniquely identify different function calls.
        """
        if TelemetryLevel.USAGE >= self._report_levels_higher_or_equal_to:
            for handler in self._handlers:
                handler.log(
                    client_id=self._client_id,
                    function_start_event=FunctionStartEvent.create(
                        event_time=datetime.datetime.now(),
                        level_name=TelemetryLevel.USAGE.name,
                        invocation_id=invocation_id,
                        module_name=module_name,
                        function_name=function_name,
                    ),
                )

    def log_function_end(  # noqa: D
        self, invocation_id: str, module_name: str, function_name: str, runtime: float, exception_trace: Optional[str]
    ) -> None:
        """Similar to log_function_end, except adding the duration of the call and exception trace on error."""
        if TelemetryLevel.USAGE >= self._report_levels_higher_or_equal_to or (
            exception_trace and TelemetryLevel.EXCEPTION >= self._report_levels_higher_or_equal_to
        ):
            for handler in self._handlers:
                handler.log(
                    client_id=self._client_id,
                    function_end_event=FunctionEndEvent.create(
                        event_time=datetime.datetime.now(),
                        level_name=TelemetryLevel.USAGE.name if not exception_trace else TelemetryLevel.EXCEPTION.name,
                        invocation_id=invocation_id,
                        module_name=module_name,
                        function_name=function_name,
                        runtime=runtime,
                        exception_trace=exception_trace,
                    ),
                )


P = ParamSpec("P")
R = TypeVar("R")


def log_call(telemetry_reporter: TelemetryReporter, module_name: str) -> Callable[[Callable[P, R]], Callable[P, R]]:
    """Decorator to make it easier to log telemetry for function calls.

    Using module_name instead of introspection since it seems more robust.

    Example call:

    @log_call(telemetry_reporter=telemetry_reporter, module_name=__name__)
    def test_function() -> str:
        return "foo"

    """

    def decorator(func: Callable[P, R]) -> Callable[P, R]:
        @functools.wraps(func)
        def wrapped(*args: P.args, **kwargs: P.kwargs) -> R:
            # Not every Callable has a __name__
            function_name = getattr(func, "__name__", repr(func))
            invocation_id = f"call_{random_id()}"
            start_time = time.time()
            telemetry_reporter.log_function_start(
                invocation_id=invocation_id, module_name=module_name, function_name=function_name
            )
            exception_trace: Optional[str] = None
            try:
                return func(*args, **kwargs)
            except Exception:
                exception_trace = traceback.format_exc()
                raise
            finally:
                telemetry_reporter.log_function_end(
                    invocation_id=invocation_id,
                    module_name=module_name,
                    function_name=function_name,
                    runtime=time.time() - start_time,
                    exception_trace=exception_trace,
                )

        return wrapped

    return decorator
