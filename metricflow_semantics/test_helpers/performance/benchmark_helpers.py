from __future__ import annotations

import datetime
import gc
import logging
import math
import time
import timeit
from abc import ABC, abstractmethod
from dataclasses import dataclass
from io import StringIO

from typing_extensions import override

from metricflow_semantics.toolkit.mf_logging.lazy_formattable import LazyFormat

logger = logging.getLogger(__name__)


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


@dataclass(frozen=True)
class _Point:
    x: int
    y: int

    @staticmethod
    def distance(left: _Point, right: _Point) -> float:
        x_delta = left.x - right.x
        y_delta = left.y - right.y
        return math.sqrt(x_delta * x_delta + y_delta * y_delta)


class _ReferenceFunction(BenchmarkFunction, ABC):
    """Function that runs a bunch of dummy operations to provide a reference for relative performance comparisons."""

    def __init__(self, create_object_count: int, function_call_count: int) -> None:
        self._create_object_count = create_object_count
        self._function_call_count = function_call_count

    @override
    def run(self) -> None:
        points = tuple(_Point(x=i, y=i) for i in range(self._create_object_count))
        for i in range(self._function_call_count):
            _Point.distance(points[i], points[self._create_object_count - i - 1])


class OneSecondFunction(_ReferenceFunction):
    """Function that takes about 1s to run."""

    def __init__(self) -> None:  # noqa: D107
        super().__init__(create_object_count=1_000_000, function_call_count=1_000_000)
