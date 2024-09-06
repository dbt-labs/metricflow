from __future__ import annotations

import logging
from typing import override

import pytest

from metricflow_semantics.formatting.formatting_helpers import mf_dedent
from metricflow_semantics.mf_logging.lazy_formattable import LazyFormat
from tests_metricflow_semantics.mf_logging.recorded_logging_context import RecordingLogHandler, recorded_logging_context

logger = logging.getLogger(__name__)


def test_log_kwargs() -> None:
    """Test that objects included via keyword args are formatted."""
    recorded_logger: logging.Logger
    handler: RecordingLogHandler
    with recorded_logging_context(logger, logging.DEBUG) as (recorded_logger, handler):
        recorded_logger.debug(
            LazyFormat("Found candidates.", matches=[1, 2, 3], parameters={"field_0": "value_0", "field_1": "value_1"})
        )
        assert handler.get_last_message() == mf_dedent(
            """
            Found candidates.
              matches: [1, 2, 3]
              parameters: {'field_0': 'value_0', 'field_1': 'value_1'}
            """
        )


def test_lazy_evaluation() -> None:
    """Test that formatting of objects are done lazily and not when the logging level is not appropriate."""
    recorded_logger: logging.Logger
    handler: RecordingLogHandler

    assertion_message = "This should not have been formatted as a string."

    class _NotFormattedObject:
        @override
        def __repr__(self) -> str:
            raise AssertionError(assertion_message)

    # Logging level is INFO, so DEBUG messages shouldn't be logged / formatted.
    with recorded_logging_context(logger, logging.INFO) as (recorded_logger, handler):
        recorded_logger.debug(LazyFormat("Found candidates", matches=[_NotFormattedObject()]))
