from __future__ import annotations

import functools
import logging
import time
from contextvars import ContextVar, Token
from dataclasses import dataclass
from typing import Callable, ClassVar, ContextManager, Mapping, Optional, Type, TypeVar, Union

from metricflow_semantics.toolkit.mf_logging.lazy_formattable import LazyFormat
from metricflow_semantics.toolkit.mf_type_aliases import ExceptionTracebackAnyType
from metricflow_semantics.toolkit.time_helpers import PrettyDuration
from typing_extensions import ParamSpec

logger = logging.getLogger(__name__)

ExecutionTimerDescription = Union[str, LazyFormat, Callable]


class ExecutionTimer(ContextManager["ExecutionTimer"]):
    """A context manager for logging the runtime of code sections.

    Example:
        with ExecutionTimer("Build semantic graph"):
            build_semantic_graph()

    Resulting log lines:
        [START D0] Build semantic graph
        [END D0] Build semantic graph [duration=0.42 s]

    In the log prefixes, `D0` is the outermost timer depth, `D1` is a timer nested inside it, and so on.
    """

    _NESTING_DEPTH: ClassVar[ContextVar[int]] = ContextVar("execution_timer_nesting_depth", default=0)

    def __init__(
        self,
        description: Optional[ExecutionTimerDescription] = None,
        log_level: int = logging.INFO,
        extra: Optional[Mapping[str, object]] = None,
        duration_warning_threshold: Optional[float] = 30.0,
    ) -> None:
        """Initializer.

        Args:
            description: If specified, log start / end messages using this value.
            log_level: Log `description` using this log level.
            extra: Additional fields to add to start / end log records.
            duration_warning_threshold: If specified, log a warning if the elapsed duration exceeds this threshold.
        """
        self._log_level = log_level
        self._duration_warning_threshold = duration_warning_threshold
        self._context_state = _ExecutionTimerState(description, extra)

    def __enter__(self) -> ExecutionTimer:  # noqa: D105
        depth = self._NESTING_DEPTH.get()
        nesting_token = self._NESTING_DEPTH.set(depth + 1)
        description = self._context_state.description
        if description is not None:
            logger.log(
                level=self._log_level,
                msg=LazyFormat(lambda: f"[START D{depth}] {self._render_execution_timer_description(description)}"),
                extra=self._context_state.extra,
            )
        self._context_state.push_active_context(
            _ExecutionTimerFrame(
                depth=depth,
                nesting_token=nesting_token,
                start_time=time.perf_counter(),
            )
        )
        return self

    def __exit__(  # noqa: D105
        self,
        exc_type: Optional[Type[BaseException]],
        exc_val: Optional[BaseException],
        exc_tb: Optional[ExceptionTracebackAnyType],
    ) -> None:
        active_context = self._context_state.pop_active_context()
        if active_context is None:
            logger.error(LazyFormat(lambda: f"{self.__class__.__name__} shouldn't exit context without entering."))
            return

        try:
            total_duration = self._context_state.add_completed_context_duration(
                time.perf_counter() - active_context.start_time
            )
            description = self._context_state.description
            if description is not None:
                pretty_total_duration = PrettyDuration(total_duration)
                logger.log(
                    level=self._log_level,
                    msg=LazyFormat(
                        lambda: f"[END D{active_context.depth}] "
                        f"{self._render_execution_timer_description(description)} [duration={pretty_total_duration}]"
                    ),
                    extra=self._context_state.extra,
                )
                if self._duration_warning_threshold is not None and total_duration > self._duration_warning_threshold:
                    logger.warning(
                        LazyFormat(
                            lambda: f"{str(self._render_execution_timer_description(description))!r}"
                            f" is slow with a duration of {pretty_total_duration}"
                        ),
                        extra=self._context_state.extra,
                    )
        finally:
            self._NESTING_DEPTH.reset(active_context.nesting_token)

    @property
    def total_duration(self) -> PrettyDuration:  # noqa: D102
        return PrettyDuration(self._context_state.total_duration_for_completed_contexts)

    @staticmethod
    def _render_execution_timer_description(description: ExecutionTimerDescription) -> Union[str, LazyFormat]:
        """Return the timer description, evaluating it first if it is callable."""
        if callable(description):
            return description()
        return description


@dataclass(frozen=True)
class _ExecutionTimerFrame:
    """State for an active timer context."""

    # Nesting depth when this timer was entered.
    depth: int
    # Token used to reset nesting depth when this timer exits.
    nesting_token: Token[int]
    # Perf-counter value recorded after the start log message.
    start_time: float


class _ExecutionTimerState:
    """Context-local object for storing state for the timer."""

    def __init__(  # noqa: D107
        self,
        description: Optional[ExecutionTimerDescription],
        extra: Optional[Mapping[str, object]],
    ) -> None:
        self.description = description
        self.extra = extra
        self._active_context_stack: ContextVar[tuple[_ExecutionTimerFrame, ...]] = ContextVar(
            f"{self.__class__.__name__}.active_context_stack",
            default=(),
        )
        self._total_duration_for_completed_contexts: ContextVar[float] = ContextVar(
            f"{self.__class__.__name__}.total_duration_for_completed_contexts",
            default=0.0,
        )

    def push_active_context(self, active_context: _ExecutionTimerFrame) -> None:
        """Push an active timer context onto this context's stack."""
        self._active_context_stack.set(self._active_context_stack.get() + (active_context,))

    def pop_active_context(self) -> Optional[_ExecutionTimerFrame]:
        """Pop an active timer context if one exists."""
        active_context_stack = self._active_context_stack.get()
        if len(active_context_stack) == 0:
            return None
        self._active_context_stack.set(active_context_stack[:-1])
        return active_context_stack[-1]

    def add_completed_context_duration(self, duration: float) -> float:
        """Add a completed context duration and return the new total."""
        total_duration_for_completed_contexts = self.total_duration_for_completed_contexts + duration
        self._total_duration_for_completed_contexts.set(total_duration_for_completed_contexts)
        return total_duration_for_completed_contexts

    @property
    def total_duration_for_completed_contexts(self) -> float:  # noqa: D102
        return self._total_duration_for_completed_contexts.get()


ReturnType = TypeVar("ReturnType")
ParametersType = ParamSpec("ParametersType")


def mf_log_duration(
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
