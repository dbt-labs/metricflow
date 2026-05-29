from __future__ import annotations

import logging
from abc import ABC

from metricflow_semantics.test_helpers.performance.benchmark_helpers import BenchmarkFunction, PerformanceBenchmark

logger = logging.getLogger(__name__)


def test_assert_performance_factor() -> None:
    """Using `assert_function_performance`, check that a function that does 1/2 the work shows 2x performance.

    The left function adds 20,000 strings to a set while the right function adds 10,000 strings to a set.
    """

    class _AddStringsToSetFunction(BenchmarkFunction, ABC):
        def __init__(self, string_count: int) -> None:
            self._string_count = string_count

        def run(self) -> None:
            items: set[str] = set()
            for i in range(self._string_count):
                items.add(str(i))

    class _LeftFunction(_AddStringsToSetFunction):
        def __init__(self) -> None:
            super().__init__(20_000)

    class _RightFunction(_AddStringsToSetFunction):
        def __init__(self) -> None:
            super().__init__(10_000)

    PerformanceBenchmark.assert_function_performance(
        left_function_class=_LeftFunction,
        right_function_class=_RightFunction,
        min_performance_factor=1.9,
    )
