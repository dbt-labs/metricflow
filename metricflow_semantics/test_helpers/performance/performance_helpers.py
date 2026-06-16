from __future__ import annotations

import datetime
import gc
import logging
import time
import timeit

from metricflow_semantics.toolkit.mf_logging.lazy_formattable import LazyFormat

logger = logging.getLogger(__name__)


def assert_performance_factor(
    left_setup: str,
    left_statement: str,
    right_setup: str,
    right_statement: str,
    min_performance_factor: float,
) -> None:
    """Using `timeit`, check that the right statement is faster than the left statement by the given factor.

    See `timeit` for definitions of `setup` and `statement`.
    """
    attempt_count = 3
    sleep_time = 1.0
    for i in range(attempt_count):
        try:
            # `autorange()` runs the given code multiple times until 0.2s is spent.
            gc.collect()
            iteration_count, total_time = timeit.Timer(
                timer=time.thread_time, setup=left_setup, stmt=left_statement
            ).autorange()
            average_left_code_runtime = total_time / iteration_count

            gc.collect()
            iteration_count, total_time = timeit.Timer(
                timer=time.thread_time, setup=right_setup, stmt=right_statement
            ).autorange()
            average_right_code_runtime = total_time / iteration_count

        except Exception as e:
            raise RuntimeError(
                LazyFormat(
                    "Got an exception with the executed statements.",
                    left_setup=left_setup,
                    left_statement=left_statement,
                    right_setup=right_setup,
                    right_statement=right_statement,
                )
            ) from e

        performance_factor = average_left_code_runtime / average_right_code_runtime

        logger.debug(
            LazyFormat(
                "Compared performance of left and right statements.",
                left_statement_runtime=lambda: datetime.timedelta(seconds=average_left_code_runtime),
                right_statement_runtime=lambda: datetime.timedelta(seconds=average_right_code_runtime),
                performance_factor=lambda: f"{performance_factor:.2f}",
                expected_min_performance_factor=lambda: f"{min_performance_factor:.2f}",
            )
        )

        if performance_factor > min_performance_factor:
            return

        if i == attempt_count - 1:
            raise AssertionError(
                LazyFormat(
                    "The performance factor is lower than the minimum expected.",
                    expected_min_performance_factor=min_performance_factor,
                    actual_performance_factor=lambda: f"{performance_factor:.2f}",
                    attempt_count=attempt_count,
                )
            )
        else:
            logger.debug(
                LazyFormat(
                    "Since performance is lower than expected, sleeping and retrying in case it's a fluke",
                    sleep_time=lambda: f"{sleep_time:.2f}s",
                    attempt_num=i + 1,
                    attempt_count=attempt_count,
                )
            )
            time.sleep(sleep_time)
