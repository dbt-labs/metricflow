from __future__ import annotations

import functools
import logging
import time
from contextlib import contextmanager
from typing import Callable, Iterator, TypeVar

from typing_extensions import ParamSpec

from metricflow_semantics.toolkit.mf_logging.lazy_formattable import LazyFormat

logger = logging.getLogger(__name__)

ReturnType = TypeVar("ReturnType")
ParametersType = ParamSpec("ParametersType")


def log_runtime(
    runtime_warning_threshold: float = 5.0,
) -> Callable[[Callable[ParametersType, ReturnType]], Callable[ParametersType, ReturnType]]:
    """Logs how long a function took to run.

    If the runtime exceeds runtime_warning_threshold, then a warning is logged.
    """

    def decorator(wrapped_function: Callable[ParametersType, ReturnType]) -> Callable[ParametersType, ReturnType]:
        # wraps() preserves attributes like the __qualname__ and the docstring in the returned function.
        @functools.wraps(wrapped_function)
        def _inner(*args: ParametersType.args, **kwargs: ParametersType.kwargs) -> ReturnType:
            # __qualname__ includes the path like MyClass.my_function
            function_name = f"{wrapped_function.__qualname__}()"
            start_time = time.perf_counter()
            logger.info(LazyFormat(lambda: f"Starting {function_name}"))

            try:
                result = wrapped_function(*args, **kwargs)
            finally:
                runtime = time.perf_counter() - start_time
                logger.info(LazyFormat(lambda: f"Finished {function_name} in {runtime:.1f}s"))
                if runtime > runtime_warning_threshold:
                    logger.warning(LazyFormat(lambda: f"{function_name} is slow with a runtime of {runtime:.1f}s"))

            return result

        return _inner

    return decorator


@contextmanager
def log_block_runtime(code_block_name: str, runtime_warning_threshold: float = 5.0) -> Iterator[None]:
    """Logs the runtime of the enclosed code block."""
    start_time = time.perf_counter()
    logger.info(LazyFormat(lambda: f"Starting {code_block_name!r}"))

    yield

    runtime = time.perf_counter() - start_time
    logger.info(LazyFormat(lambda: f"Finished {code_block_name!r} in {runtime:.1f}s"))
    if runtime > runtime_warning_threshold:
        logger.warning(LazyFormat(lambda: f"{code_block_name!r} is slow with a runtime of {runtime:.1f}s"))
