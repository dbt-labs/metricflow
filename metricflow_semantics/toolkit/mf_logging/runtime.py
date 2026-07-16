from __future__ import annotations

import functools
import logging
from typing import Callable, Optional, TypeVar

from metricflow_semantics.toolkit.performance_helpers import ExecutionTimer
from typing_extensions import ParamSpec

logger = logging.getLogger(__name__)

ReturnType = TypeVar("ReturnType")
ParametersType = ParamSpec("ParametersType")


def log_runtime(
    log_level: int = logging.INFO,
    duration_warning_threshold: Optional[float] = 30.0,
) -> Callable[[Callable[ParametersType, ReturnType]], Callable[ParametersType, ReturnType]]:
    """Logs how long a function took to run.

    If the runtime exceeds duration_warning_threshold, then a warning is logged.
    """

    def decorator(wrapped_function: Callable[ParametersType, ReturnType]) -> Callable[ParametersType, ReturnType]:
        # wraps() preserves attributes like the __qualname__ and the docstring in the returned function.
        @functools.wraps(wrapped_function)
        def _inner(*args: ParametersType.args, **kwargs: ParametersType.kwargs) -> ReturnType:
            # __qualname__ includes the path like MyClass.my_function
            function_name = f"{wrapped_function.__qualname__}()"
            with ExecutionTimer(
                function_name,
                log_level=log_level,
                duration_warning_threshold=duration_warning_threshold,
            ):
                return wrapped_function(*args, **kwargs)

        return _inner

    return decorator
