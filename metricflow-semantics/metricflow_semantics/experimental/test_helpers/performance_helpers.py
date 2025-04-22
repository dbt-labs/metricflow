from __future__ import annotations

import datetime
import logging
import timeit

from metricflow_semantics.mf_logging.lazy_formattable import LazyFormat

logger = logging.getLogger(__name__)


def assert_performance_factor(
    slow_code_setup: str,
    slow_code_statement: str,
    fast_code_setup: str,
    fast_code_statement: str,
    min_performance_factor: float,
    statement_executions_per_run: int = 100,
    run_repeat_count: int = 100,
) -> None:
    """Using `timeit`, check that the fast code is faster than the slow code by the given factor.

    See `timeit` for definitions of `setup` and `statement`.

    For each run, the given statement is executed `statement_executions_per_run` times and the total time to execute
    those statements is stored.

    The run is repeated `run_repeat_count` times, and the performance factor is computed from the fastest run to
    reduce variations caused by other processes.
    """
    try:
        slow_code_runtime = min(
            timeit.repeat(
                setup=slow_code_setup,
                stmt=slow_code_statement,
                number=statement_executions_per_run,
                repeat=run_repeat_count,
            )
        )

        fast_code_runtime = min(
            timeit.repeat(
                setup=fast_code_setup,
                stmt=fast_code_statement,
                number=statement_executions_per_run,
                repeat=run_repeat_count,
            )
        )
        performance_factor = slow_code_runtime / fast_code_runtime

        logger.debug(
            LazyFormat(
                "Compared performance of slow code and fast code",
                slow_code_setup=slow_code_setup,
                slow_code_statement=slow_code_statement,
                fast_code_setup=fast_code_setup,
                fast_code_statement=fast_code_statement,
                slow_code_runtime=lambda: datetime.timedelta(seconds=slow_code_runtime),
                fast_code_runtime=lambda: datetime.timedelta(seconds=fast_code_runtime),
                performance_factor=lambda: f"{performance_factor:.2f}",
            )
        )
    except Exception as e:
        raise RuntimeError(
            LazyFormat(
                "Got an exception with the given code",
                slow_code_setup=slow_code_setup,
                slow_code_statement=slow_code_statement,
                fast_code_setup=fast_code_setup,
                fast_code_statement=fast_code_statement,
            )
        ) from e
    assert performance_factor >= min_performance_factor
