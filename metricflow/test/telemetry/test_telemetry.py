import logging

import pytest

from metricflow.telemetry.models import TelemetryLevel
from metricflow.telemetry.reporter import TelemetryReporter, log_call

logger = logging.getLogger(__name__)


@pytest.fixture
def telemetry_reporter() -> TelemetryReporter:  # noqa: D
    reporter = TelemetryReporter(report_levels_higher_or_equal_to=TelemetryLevel.USAGE)
    reporter.add_python_log_handler()
    reporter.add_test_handler()
    return reporter


def test_function_call(telemetry_reporter: TelemetryReporter) -> None:  # noqa: D
    @log_call(telemetry_reporter=telemetry_reporter, module_name=__name__)
    def test_function() -> str:
        return "foo"

    test_function()

    start_event = telemetry_reporter.test_handler.payloads[0].function_start_events[0]
    assert start_event.module_name == "metricflow.test.telemetry.test_telemetry"
    assert start_event.function_name == "test_function"

    end_event = telemetry_reporter.test_handler.payloads[1].function_end_events[0]
    assert end_event.module_name == "metricflow.test.telemetry.test_telemetry"
    assert end_event.function_name == "test_function"
    assert not end_event.exception_trace
    assert end_event.runtime > 0


def test_function_exception(telemetry_reporter: TelemetryReporter) -> None:  # noqa: D
    with pytest.raises(ValueError):

        @log_call(telemetry_reporter=telemetry_reporter, module_name=__name__)
        def test_function() -> str:
            raise ValueError("foo")

        test_function()

    start_event = telemetry_reporter.test_handler.payloads[0].function_start_events[0]
    assert start_event.module_name == "metricflow.test.telemetry.test_telemetry"
    assert start_event.function_name == "test_function"

    end_event = telemetry_reporter.test_handler.payloads[1].function_end_events[0]
    assert end_event.module_name == "metricflow.test.telemetry.test_telemetry"
    assert end_event.function_name == "test_function"
    assert end_event.exception_trace
    assert end_event.exception_trace.find("Traceback (most recent call last):") != -1
    assert end_event.exception_trace.find("ValueError: foo") != -1
    assert end_event.runtime > 0


def test_telemetry_off() -> None:  # noqa: D
    reporter = TelemetryReporter(report_levels_higher_or_equal_to=TelemetryLevel.OFF)
    reporter.add_python_log_handler()
    reporter.add_test_handler()

    @log_call(telemetry_reporter=reporter, module_name=__name__)
    def test_function() -> str:
        return "foo"

    test_function()

    with pytest.raises(ValueError):

        @log_call(telemetry_reporter=reporter, module_name=__name__)
        def test_exception_function() -> str:
            raise ValueError("foo")

        test_exception_function()

    assert len(reporter.test_handler.payloads) == 0


@pytest.mark.skip("Sends events to Rudderstack")
def test_rudderstack_logging() -> None:  # noqa: D
    reporter = TelemetryReporter(report_levels_higher_or_equal_to=TelemetryLevel.USAGE)
    reporter.add_python_log_handler()
    reporter.add_rudderstack_handler()

    @log_call(telemetry_reporter=reporter, module_name=__name__)
    def test_function() -> str:
        return "foo"

    test_function()

    with pytest.raises(ValueError):

        @log_call(telemetry_reporter=reporter, module_name=__name__)
        def test_exception_function() -> str:
            raise ValueError("foo")

        test_exception_function()
