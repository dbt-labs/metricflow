from __future__ import annotations

import functools
import logging
import time
from typing import Callable, TypeVar

from typing_extensions import ParamSpec

logger = logging.getLogger(__name__)

ReturnType = TypeVar("ReturnType")
ParametersType = ParamSpec("ParametersType")


def log_runtime(
    runtime_warning_threshold: float = 3.0,
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
            start_time = time.time()
            logger.info(f"Starting {function_name}")

            try:
                result = wrapped_function(*args, **kwargs)
            finally:
                runtime = time.time() - start_time
                logger.info(f"Finished {function_name} in {runtime:.1f}s")
                if runtime > runtime_warning_threshold:
                    logger.warning(f"{function_name} is slow with a runtime of {runtime:.1f}s")

            return result

        return _inner

    return decorator
