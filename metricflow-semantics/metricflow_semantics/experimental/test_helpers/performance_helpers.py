from __future__ import annotations

import datetime
import gc
import logging
import time
import timeit
from abc import ABC, abstractmethod
from io import StringIO

from metricflow_semantics.mf_logging.lazy_formattable import LazyFormat

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


class BenchmarkFunction(ABC):
    """Closure to encapsulate the setup code and the benchmarked code.

    This closure is helpful for authoring tests as the arguments to the `timeit` calls are strings of code
    snippets. Those strings are annoying to author as the IDE inspections don't work / mypy does not do type checks in
    strings. In addition, all imports must be specifically included in the code snippet.

    Using the closure, there's no need to add additional imports as this can make references to objects in the
    caller's frame.

    This is a WIP - there may be other approaches and there may be implications to measuring code performance. One
    issue that is known is that this is not suitable for checking fast snippets as due to the overhead in the
    method call.

    The `__init__` method of implementing classes should have no parameters.
    """

    @abstractmethod
    def run(self) -> None:
        """This should contain the code to run in the benchmark."""
        raise NotImplementedError


class PerformanceBenchmark:
    """Helper to benchmark execution performance using `timeit`."""

    @staticmethod
    def assert_function_performance(
        left_function_class: type[BenchmarkFunction],
        right_function_class: type[BenchmarkFunction],
        min_performance_factor: float,
    ) -> None:
        """Assert that the right function is `min_performance_factor` times faster than the left function.

        As there is an overhead to calling a function in a class, this is not suitable to fast functions.
        """
        attempt_count = 3
        sleep_time = 1.0
        # 50 microseconds.
        min_function_runtime = 50e-6
        left_timer = timeit.Timer(
            timer=time.thread_time,
            setup="left_function = left_function_class()",
            stmt="left_function.run()",
            globals={"left_function_class": left_function_class},
        )
        right_timer = timeit.Timer(
            timer=time.thread_time,
            setup="right_function = right_function_class()",
            stmt="right_function.run()",
            globals={"right_function_class": right_function_class},
        )

        for i in range(attempt_count):
            left_function_runtime = PerformanceBenchmark._call_with_exception_handling(left_timer)
            right_function_runtime = PerformanceBenchmark._call_with_exception_handling(right_timer)

            if any(runtime < min_function_runtime for runtime in (left_function_runtime, right_function_runtime)):
                raise RuntimeError(
                    LazyFormat(
                        "The runtime of at least one of the provided functions is too short."
                        " This means that the runtime will include the function-call overhead as a significant"
                        " component",
                        left_function_runtime=lambda: datetime.timedelta(seconds=left_function_runtime),
                        right_function_runtime=lambda: datetime.timedelta(seconds=right_function_runtime),
                        min_function_runtime=min_function_runtime,
                    )
                )

            performance_factor = left_function_runtime / right_function_runtime

            logger.debug(
                LazyFormat(
                    "Compared performance of left and right functions.",
                    left_function_runtime=lambda: datetime.timedelta(seconds=left_function_runtime),
                    right_function_runtime=lambda: datetime.timedelta(seconds=right_function_runtime),
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

    @staticmethod
    def _call_with_exception_handling(timer: timeit.Timer) -> float:
        """Wraps the call to `timeit` to include the stack trace as the one raised by `timeit` does not."""
        gc.collect()
        try:
            return timer.timeit(number=1)
        except Exception as e:
            with StringIO() as fp:
                timer.print_exc(fp)
                # noinspection PyUnresolvedReferences
                raise RuntimeError(
                    LazyFormat("Got an exception while executing timed code.", exception=fp.getvalue())
                ) from e
