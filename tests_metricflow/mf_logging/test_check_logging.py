from __future__ import annotations

from metricflow_semantics.mf_logging.lazy_formattable import LazyFormat
from metricflow_semantics.test_helpers.logging_helpers import find_log_calls_that_dont_use_lazy_format

from tests_metricflow import TESTS_METRICFLOW_DIRECTORY_ANCHOR


def test_all_calls_use_lazy_format() -> None:
    """Check that all Python files in the repo make log calls using `LazyFormat`."""
    log_calls = find_log_calls_that_dont_use_lazy_format(TESTS_METRICFLOW_DIRECTORY_ANCHOR.directory.parent)

    if len(log_calls) > 0:
        raise ValueError(
            LazyFormat(
                "Found log call(s) that doesn't use `LazyFormat`. Log calls should be of the form"
                " `logger.info(LazyFormat(...))`.",
                log_calls=log_calls,
            )
        )
