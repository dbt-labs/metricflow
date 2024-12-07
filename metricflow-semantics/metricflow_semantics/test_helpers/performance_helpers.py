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

    def compare(self, other: ContextReport) -> ContextReportComparison:
        """Compare this report with other."""
        assert self.context_id == other.context_id, "Cannot compare unrelated contexts."

        calculated_keys = (
            "cpu_ns_average",
            "cpu_ns_median",
            "cpu_ns_max",
            "wall_ns_average",
            "wall_ns_median",
            "wall_ns_max",
        )

        kwargs = {}
        max_pct_change = float("-inf")
        for key in calculated_keys:
            self_val = getattr(self, key)
            other_val = getattr(other, key)

            diff = self_val - other_val
            kwargs[f"{key}_abs"] = diff

            pct = diff / self_val
            kwargs[f"{key}_pct"] = pct
            if pct > max_pct_change:
                max_pct_change = pct

        return ContextReportComparison(
            context_id=self.context_id,
            a=self,
            b=other,
            max_pct_change=max_pct_change,
            **kwargs,
        )


class ContextReportComparison(FrozenBaseModel):
    """A comparison between two context reports."""

    context_id: str

    a: ContextReport
    b: ContextReport

    max_pct_change: float

    cpu_ns_average_abs: int
    cpu_ns_average_pct: float
    cpu_ns_median_abs: int
    cpu_ns_median_pct: float
    cpu_ns_max_abs: int
    cpu_ns_max_pct: float

    wall_ns_average_abs: int
    wall_ns_average_pct: float
    wall_ns_median_abs: int
    wall_ns_median_pct: float
    wall_ns_max_abs: int
    wall_ns_max_pct: float


class SessionReport(FrozenBaseModel):
    """A performance report containing aggregated runtime statistics from a session."""

    session_id: str
    contexts: Dict[str, ContextReport]

    def compare(self, other: SessionReport) -> SessionReportComparison:
        """Compare this report with other."""
        assert self.session_id == other.session_id, "Cannot compare unrelated sessions."

        self_contexts = set(self.contexts.keys())
        other_contexts = set(other.contexts.keys())
        all_contexts = self_contexts.union(other_contexts)

        comparisons: Dict[str, Optional[ContextReportComparison]] = {}
        max_pct_change = float("-inf")
        for context in all_contexts:
            if context not in self.contexts or context not in other.contexts:
                comparisons[context] = None
            else:
                comp = self.contexts[context].compare(other.contexts[context])
                comparisons[context] = comp
                if comp.max_pct_change > max_pct_change:
                    max_pct_change = comp.max_pct_change

        return SessionReportComparison(
            session_id=self.session_id,
            a=self.contexts,
            b=other.contexts,
            contexts=comparisons,
            max_pct_change=max_pct_change,
        )


class SessionReportComparison(FrozenBaseModel):
    """A comparison between two session reports.

    If a context is not present in A or B, the absolute and pct values will be None for
    that entry.
    """

    session_id: str
    a: Dict[str, ContextReport]
    b: Dict[str, ContextReport]
    contexts: Dict[str, Optional[ContextReportComparison]]
    max_pct_change: float


class SessionReportSet(FrozenBaseModel):
    """A set of session reports."""

    sessions: Dict[str, SessionReport] = {}

    def add_report(self, report: SessionReport) -> None:
        """Add a report and associate it with the session ID."""
        self.sessions[report.session_id] = report

    def compare(self, other: SessionReportSet) -> SessionReportSetComparison:
        """Compare this report set with other."""
        self_sessions = set(self.sessions.keys())
        other_sessions = set(other.sessions.keys())
        all_sessions = self_sessions.union(other_sessions)

        comparison: Dict[str, Optional[SessionReportComparison]] = {}
        max_pct_change = float("-inf")
        max_pct_change_session = ""
        for session in all_sessions:
            if session not in self.sessions or session not in other.sessions:
                comparison[session] = None
            else:
                comp = self.sessions[session].compare(other.sessions[session])
                comparison[session] = comp
                if comp.max_pct_change > max_pct_change:
                    max_pct_change = comp.max_pct_change
                    max_pct_change_session = session

        return SessionReportSetComparison(
            sessions=comparison,
            max_pct_change=max_pct_change,
            max_pct_change_session=max_pct_change_session,
        )


class SessionReportSetComparison(FrozenBaseModel):
    """A comparison between two session report sets.

    If a session ID is not present in A or B, the comparison is None
    """

    sessions: Dict[str, Optional[SessionReportComparison]]

    max_pct_change: float
    max_pct_change_session: str


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
