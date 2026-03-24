from __future__ import annotations

import logging

from metricflow_semantics.test_helpers.recorded_logging_context import RecordingLogHandler, recorded_logging_context
from metricflow_semantics.toolkit.mf_logging.lazy_formattable import LazyFormat
from typing_extensions import override

logger = logging.getLogger(__name__)

_NOT_FORMATTED_ASSERTION_MESSAGE = "This should not have been formatted as a string."


def test_log_kwargs() -> None:
    """Test that objects included via keyword args are formatted."""
    recorded_logger: logging.Logger
    handler: RecordingLogHandler
    with recorded_logging_context(logger, logging.DEBUG) as (recorded_logger, handler):
        recorded_logger.debug(
            LazyFormat("Found candidates.", matches=[1, 2, 3], parameters={"field_0": "value_0", "field_1": "value_1"})
        )
        assert handler.get_last_message() == (
            "Found candidates. (matches=[1, 2, 3], parameters={'field_0': 'value_0', 'field_1': 'value_1'})"
        )


def test_lambda() -> None:
    """Test that a lambda that is passed in is called."""
    example_message = "Example message."

    recorded_logger: logging.Logger
    handler: RecordingLogHandler
    with recorded_logging_context(logger, logging.DEBUG) as (recorded_logger, handler):
        recorded_logger.debug(LazyFormat(lambda: example_message))
        assert handler.get_last_message() == example_message


def test_lazy_object() -> None:
    """Test that formatting of objects are done lazily and not when the logging level is not appropriate."""

    class _NotFormattedObject:
        @override
        def __repr__(self) -> str:
            raise AssertionError(_NOT_FORMATTED_ASSERTION_MESSAGE)

    # Logging level is INFO, so DEBUG messages shouldn't be logged / formatted.
    recorded_logger: logging.Logger
    handler: RecordingLogHandler
    with recorded_logging_context(logger, logging.INFO) as (recorded_logger, handler):
        recorded_logger.debug(LazyFormat("Found candidates", should_not_be_formatted=_NotFormattedObject()))


def test_lazy_lambda() -> None:
    """Test that a lambda message is not evaluated when the logging level is not appropriate."""

    def _should_not_be_called() -> str:
        raise AssertionError(_NOT_FORMATTED_ASSERTION_MESSAGE)

    # Logging level is INFO, so DEBUG messages shouldn't be logged / formatted.
    recorded_logger: logging.Logger
    handler: RecordingLogHandler
    with recorded_logging_context(logger, logging.INFO) as (recorded_logger, handler):
        recorded_logger.debug(LazyFormat(lambda: f"{_should_not_be_called()}"))


def test_lambda_argument() -> None:
    """Tests that a callable that's supplied as an argument value is evaluated."""
    recorded_logger: logging.Logger
    handler: RecordingLogHandler
    with recorded_logging_context(logger, logging.INFO) as (recorded_logger, handler):
        recorded_logger.info(LazyFormat("Example message", arg_0=lambda: 1))
        assert handler.get_last_message() == "Example message (arg_0=1)"


def test_lazy_lambda_argument() -> None:
    """Test that a lambda input is not evaluated when the logging level is not appropriate."""

    def _should_not_be_called() -> str:
        raise AssertionError(_NOT_FORMATTED_ASSERTION_MESSAGE)

    # Logging level is INFO, so DEBUG messages shouldn't be logged / formatted.
    recorded_logger: logging.Logger
    handler: RecordingLogHandler
    with recorded_logging_context(logger, logging.INFO) as (recorded_logger, handler):
        recorded_logger.debug(LazyFormat("Example message", arg_0=lambda: f"{_should_not_be_called()}"))


def test_evaluated_properties() -> None:
    """Test `LazyFormat.evaluated_*` properties."""
    generate_message_call_count = 0

    def _generate_message_title() -> str:
        nonlocal generate_message_call_count
        generate_message_call_count += 1
        return "title"

    generate_argument_value_call_count = 0

    def _generate_argument_value() -> str:
        nonlocal generate_argument_value_call_count
        generate_argument_value_call_count += 1
        return "value"

    lazy_format = LazyFormat(_generate_message_title, argument_key=_generate_argument_value)

    # Get properties twice and check that it's only evaluated once.
    for _ in range(2):
        assert lazy_format.evaluated_message_title == "title"
        assert dict(lazy_format.evaluated_kwargs) == {"argument_key": "value"}

    assert generate_message_call_count == 1
    assert generate_argument_value_call_count == 1
