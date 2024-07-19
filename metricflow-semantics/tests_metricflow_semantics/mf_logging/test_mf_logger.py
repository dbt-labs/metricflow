from __future__ import annotations

import logging
from pathlib import Path

import pytest
from metricflow_semantics.formatting.formatting_helpers import mf_dedent
from metricflow_semantics.mf_logging.mf_logger import MetricFlowLogger
from metricflow_semantics.test_helpers.logging_helpers import RecordingLogHandler, recorded_logging_context


def test_log_file_name_and_line_number() -> None:
    """Tests that the file and the line number associated with the log call points to the source.

    The source is the file / line where the `.info(...)` and similar call was made, not within the MF logging library.
    """
    with recorded_logging_context(logging.INFO) as (logger, handler):
        log_line_number = 19
        logger.info(f"Example message from line {log_line_number} in {__file__}")
        assert len(handler.log_records) == 1, f"There should have been 1 record but got: {handler.log_records}"

        record = handler.log_records[0]
        assert record.filename == Path(__file__).name
        assert (
            record.lineno == log_line_number
        ), f"The log line should have been {log_line_number} but got {record.lineno}"


@pytest.mark.skip(reason="Running this prints error messages in the test results.")
def test_log_levels() -> None:
    """Test that the logger works for different logging levels."""
    for level_int in (logging.DEBUG, logging.INFO, logging.WARNING, logging.ERROR, logging.CRITICAL, logging.FATAL):
        with recorded_logging_context(level_int) as (logger, handler):
            # Log a message at the appropriate level
            message = f"Example message at level {level_int} in {__file__}"
            if level_int == logging.DEBUG:
                logger.debug(message)
            elif level_int == logging.INFO:
                logger.info(message)
            elif level_int == logging.WARNING:
                logger.warning(message)
            elif level_int == logging.ERROR:
                logger.error(message)
            elif level_int == logging.CRITICAL:
                logger.critical(message)
            elif level_int == logging.FATAL:
                logger.fatal(message)
            else:
                raise ValueError(f"Logging level not handled {level_int=}")

            # Check that the log record is correct.
            assert (
                len(handler.log_records) == 1
            ), f"There should have been 1 record for {level_int=} but got: {handler.log_records}"

            record = handler.log_records[0]
            assert record.levelno == level_int, f"Expected a record with {level_int=} but got {record.levelno}"
            assert record.message == message
            assert record.levelno == level_int


def test_log_kwargs() -> None:
    """Test that objects included via keyword args are formatted."""
    logger: MetricFlowLogger
    handler: RecordingLogHandler
    with recorded_logging_context(logging.DEBUG) as (logger, handler):
        logger.debug("Found candidates.", matches=[1, 2, 3], parameters={"field_0": "value_0", "field_1": "value_1"})
        assert handler.get_last_message() == mf_dedent(
            """
            Found candidates.
              matches:
                [1, 2, 3]
              parameters:
                {'field_0': 'value_0', 'field_1': 'value_1'}
            """
        )
