from __future__ import annotations

import functools
import logging
import statistics
import sys
import time
from collections import defaultdict
from contextlib import ExitStack, contextmanager
from typing import Any, Callable, Dict, Iterator, List, Optional, TypeVar
from unittest.mock import patch as mock_patch

from dbt_semantic_interfaces.implementations.base import FrozenBaseModel
from typing_extensions import ParamSpec

from metricflow.dataflow.builder.dataflow_plan_builder import DataflowPlanBuilder

logger = logging.getLogger(__name__)


_tracking_class_methods: List[Callable[..., Any]] = [  # type: ignore
    DataflowPlanBuilder.build_plan,
    DataflowPlanBuilder.build_plan_for_distinct_values,
]


# TODO: maybe do object pooling and prealloc a bunch of these
# so we dont pay the allocation tax when tracking perf
class Call(FrozenBaseModel):
    """A call to some method."""

    # TODO: use psutil to track memory and CPU
    # https://github.com/giampaolo/psutil
    total_cpu_ns: int
    total_wall_ns: int


class ContextReport(FrozenBaseModel):
    """Contains aggregated runtime statistics about a single performance context."""

    context_id: str

    cpu_ns_average: int
    cpu_ns_median: int
    cpu_ns_max: int

    wall_ns_average: int
    wall_ns_median: int
    wall_ns_max: int

    @classmethod
    def from_calls(cls, context_id: str, calls: List[Call]) -> ContextReport:
        """Init from a list of calls."""
        return cls(
            context_id=context_id,
            cpu_ns_average=int(statistics.mean(c.total_cpu_ns for c in calls)),
            cpu_ns_median=int(statistics.median(c.total_cpu_ns for c in calls)),
            cpu_ns_max=int(max(c.total_cpu_ns for c in calls)),
            wall_ns_average=int(statistics.mean(c.total_wall_ns for c in calls)),
            wall_ns_median=int(statistics.median(c.total_wall_ns for c in calls)),
            wall_ns_max=int(max(c.total_wall_ns for c in calls)),
        )


class SessionReport(FrozenBaseModel):
    """A performance report containing aggregated runtime statistics from a session."""

    session_id: str
    contexts: Dict[str, ContextReport]


class SessionReportSet(FrozenBaseModel):
    """A set of session reports."""

    sessions: Dict[str, SessionReport] = {}

    def add_report(self, report: SessionReport) -> None:
        """Add a report and associate it with the session ID."""
        self.sessions[report.session_id] = report


class PerformanceTracker:
    """Track performance metrics across different contexts.

    Don't use this directly. Instead, use the global methods in this method which interact
    with the _perf singleton.
    """

    def __init__(self) -> None:
        """Initialize the tracker."""
        self._session_id: Optional[str] = None
        self._call_map: Dict[str, List[Call]] = defaultdict(list)

        self._session_set = SessionReportSet()

    @contextmanager
    def session(self, session_id: str) -> Iterator[None]:
        """Create a new measurement session.

        At session start, all state is fresh and it gets cleaned when the session ends.
        """
        if self._session_id:
            raise ValueError("Cannot create nested sessions.")

        self._session_id = session_id

        yield

        report = self.get_session_report()
        self._session_set.add_report(report)

        self._call_map = defaultdict(list)
        self._session_id = None

    @contextmanager
    def measure(self, context_id: str) -> Iterator[PerformanceTracker]:
        """Measure performance while executing this block."""
        if not self._session_id:
            raise ValueError("Cannot measure outside of a session.")

        start_wall = time.time_ns()
        start_cpu = time.process_time_ns()

        yield self

        end_wall = time.time_ns()
        end_cpu = time.process_time_ns()

        self._call_map[context_id].append(
            Call(
                total_wall_ns=(end_wall - start_wall),
                total_cpu_ns=(end_cpu - start_cpu),
            )
        )

    def get_session_report(self) -> SessionReport:
        """Generate a report based on all the tracked calls in the current session."""
        if not self._session_id:
            raise ValueError("Cannot create report outside of a session.")

        return SessionReport(
            session_id=self._session_id,
            contexts={
                context_id: ContextReport.from_calls(context_id, calls) for context_id, calls in self._call_map.items()
            },
        )

    def get_report_set(self) -> SessionReportSet:
        """Get the performance report set for all opened sessions so far."""
        return self._session_set


TRet = TypeVar("TRet")
TParam = ParamSpec("TParam")


@contextmanager
def _track_performance_single(target: Callable[TParam, TRet], perf: PerformanceTracker) -> Iterator[None]:
    """Enable tracking for a single target.

    This method patches all instances where it is imported.
    """

    @functools.wraps(target)
    def wrap_tracking(*args: TParam.args, **kwargs: TParam.kwargs) -> TRet:
        with perf.measure(target.__qualname__):
            return target(*args, **kwargs)

    mod_name = target.__module__
    class_name, method_name = target.__qualname__.split(".")
    mod = sys.modules[mod_name]
    klass = getattr(mod, class_name)
    full_name = f"{mod_name}.{target.__qualname__}"

    patchers = []

    # patch the module itself for future imports
    patchers.append(mock_patch(full_name, new=wrap_tracking))

    # patch all current references to the method
    for module in sys.modules.values():
        # no need to patch sys modules, third party libraries etc
        if not module.__name__.startswith("metricflow"):
            continue

        for module_target_name, module_target in module.__dict__.items():
            if module_target is klass:
                module_target_full_name = f"{module.__name__}.{module_target_name}.{method_name}"
                patchers.append(mock_patch(module_target_full_name, new=wrap_tracking))

    for patcher in patchers:
        patcher.start()

    yield

    for patcher in patchers:
        patcher.stop()


@contextmanager
def track_performance() -> Iterator[PerformanceTracker]:
    """Enable performance tracking while in this context manager."""
    global _tracking_class_methods

    logger.info("Enabling performance tracking")
    perf = PerformanceTracker()

    with ExitStack() as stack:
        for target in _tracking_class_methods:
            stack.enter_context(_track_performance_single(target, perf))

        yield perf
