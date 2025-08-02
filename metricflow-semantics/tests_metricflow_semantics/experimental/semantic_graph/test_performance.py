from __future__ import annotations

import logging

from metricflow_semantics.experimental.ordered_set import FrozenOrderedSet
from metricflow_semantics.experimental.test_helpers.performance_helpers import BenchmarkFunction, PerformanceBenchmark

logger = logging.getLogger(__name__)


def test_set_in() -> None:
    """Test the performance of set inclusion check"""
    set_size = 1000
    call_count = 10_000

    items = [i % set_size for i in range(call_count)]

    python_set = set(range(set_size))
    ordered_set = FrozenOrderedSet(range(set_size))

    class _LeftFunction(BenchmarkFunction):
        """Using the existing lookup takes ~0.8s."""

        def run(self) -> None:
            for item in items:
                result = item in python_set

    class _RightFunction(BenchmarkFunction):
        def run(self) -> None:
            for item in items:
                result = item in ordered_set

    PerformanceBenchmark.assert_function_performance(
        left_function_class=_LeftFunction,
        right_function_class=_RightFunction,
        min_performance_factor=0,
    )


def test_set_union() -> None:
    """Test the performance of set inclusion check"""
    set_size = 100
    call_count = 10_000

    items = [i % set_size for i in range(call_count)]

    python_set = set(range(set_size))
    other_python_set = set(range(set_size + 1))
    ordered_set = FrozenOrderedSet(range(set_size))
    other_ordered_set = FrozenOrderedSet(range(set_size + 1))

    class _LeftFunction(BenchmarkFunction):
        """Using the existing lookup takes ~0.8s."""

        def run(self) -> None:
            for _ in range(call_count):
                result = python_set | other_python_set

    class _RightFunction(BenchmarkFunction):
        def run(self) -> None:
            for _ in range(call_count):
                result = other_ordered_set | other_ordered_set

    PerformanceBenchmark.assert_function_performance(
        left_function_class=_LeftFunction,
        right_function_class=_RightFunction,
        min_performance_factor=0,
    )
