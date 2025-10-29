from __future__ import annotations

import cProfile
import logging
from contextlib import contextmanager
from pstats import FunctionProfile, Stats, StatsProfile
from typing import Dict, Generic, Iterator, Optional, TypeVar

from dbt_semantic_interfaces.implementations.base import FrozenBaseModel

from metricflow_semantics.test_helpers.performance.report_formatter import (
    SessionReportTextFormatter,
    TableTextFormatter,
)

logger = logging.getLogger(__name__)


TNumber = TypeVar("TNumber", int, float)


class PerformanceMetricComparison(FrozenBaseModel, Generic[TNumber]):
    """Comparison of a performance metric."""

    a: TNumber
    b: TNumber
    abs: TNumber
    pct: float

    @classmethod
    def from_metrics(cls, a: TNumber, b: TNumber) -> PerformanceMetricComparison[TNumber]:
        """Construct a PerformanceMetricComparison from two values."""
        abs = a - b
        pct = (abs / a) if a != 0 else float("inf")
        return PerformanceMetricComparison(
            a=a,
            b=b,
            abs=abs,
            pct=pct,
        )


class FunctionReport(FrozenBaseModel):
    """Performance report for a given function.

    We're using this class since the original FunctionProfile has terrible naming, no
    documentation and weird typing (ncalls is a string that can optionally contain numbers).

    See: https://github.com/python/cpython/blob/732670d93b9b0c0ff8adde07418fd6f8397893ef/Lib/pstats.py#L58
    """

    function_name: str

    # number of non-recursive calls
    base_calls: int
    # total number of times the function was called, including recursion
    total_calls: int

    # time spent on the function body, not counting function calls
    body_time: float
    per_call_body_time: float

    # total time spent on function, including function calls and recursion
    total_time: float
    per_call_total_time: float

    @classmethod
    def from_function_profile(cls, function_name: str, prof: FunctionProfile) -> FunctionReport:
        """Construct a FunctionReport from a FunctionProfile."""
        # Who thought this would ever be a good idea?
        if "/" in prof.ncalls:
            total_calls_str, base_calls_str = prof.ncalls.split("/")
            total_calls = int(total_calls_str)
            base_calls = int(base_calls_str)
        else:
            base_calls = total_calls = int(prof.ncalls)

        return cls(
            function_name=function_name,
            base_calls=base_calls,
            total_calls=total_calls,
            body_time=prof.tottime,
            per_call_body_time=prof.percall_tottime,
            total_time=prof.cumtime,
            per_call_total_time=prof.percall_cumtime,
        )

    def compare(self, other: FunctionReport) -> FunctionReportComparison:
        """Compare self with other."""
        assert self.function_name == other.function_name, "Cannot compare unrelated functions"

        return FunctionReportComparison(
            function_name=self.function_name,
            a=self,
            b=other,
            base_calls=PerformanceMetricComparison[int].from_metrics(self.base_calls, other.base_calls),
            total_calls=PerformanceMetricComparison[int].from_metrics(self.total_calls, other.total_calls),
            body_time=PerformanceMetricComparison[float].from_metrics(self.body_time, other.body_time),
            per_call_body_time=PerformanceMetricComparison[float].from_metrics(
                self.per_call_body_time, other.per_call_body_time
            ),
            total_time=PerformanceMetricComparison[float].from_metrics(self.total_time, other.total_time),
            per_call_total_time=PerformanceMetricComparison[float].from_metrics(
                self.per_call_body_time, other.per_call_body_time
            ),
        )


class FunctionReportComparison(FrozenBaseModel):
    """Comparison of two function reports."""

    function_name: str

    a: FunctionReport
    b: FunctionReport

    base_calls: PerformanceMetricComparison[int]
    total_calls: PerformanceMetricComparison[int]
    body_time: PerformanceMetricComparison[float]
    per_call_body_time: PerformanceMetricComparison[float]
    total_time: PerformanceMetricComparison[float]
    per_call_total_time: PerformanceMetricComparison[float]


class SessionReport(FrozenBaseModel):
    """Contains aggregated runtime statistics about a single performance session."""

    session_id: str

    total_time: float
    functions: Dict[str, FunctionReport]

    @classmethod
    def from_stats(cls, session_id: str, stats: StatsProfile) -> SessionReport:
        """Construct a SessionReport from a StatsProfile instance."""
        functions: Dict[str, FunctionReport] = {}
        for (
            func_name,
            func_profile,
        ) in stats.func_profiles.items():
            full_name = f"{func_profile.file_name}:{func_profile.line_number} :: {func_name}"
            report = FunctionReport.from_function_profile(full_name, func_profile)
            functions[full_name] = report

        return cls(
            session_id=session_id,
            total_time=stats.total_tt,
            functions=functions,
        )

    def compare(self, other: SessionReport) -> SessionReportComparison:
        """Compare this report with other.

        If some function is present in one report but not the other, the entry for its comparison
        will be None.
        """
        assert self.session_id == other.session_id, "Cannot compare unrelated sessions."

        self_functions = set(self.functions.keys())
        other_functions = set(other.functions.keys())
        all_functions = self_functions.union(other_functions)

        return SessionReportComparison(
            session_id=self.session_id,
            a=self,
            b=other,
            total_time=PerformanceMetricComparison[float].from_metrics(self.total_time, other.total_time),
            functions={
                func_name: (
                    self.functions[func_name].compare(other.functions[func_name])
                    if func_name in self.functions and func_name in other.functions
                    else None
                )
                for func_name in all_functions
            },
        )

    def text_format(self, formatter: Optional[SessionReportTextFormatter] = None) -> str:
        """Format this report using the provided formatter."""
        if formatter is None:
            return TableTextFormatter().format_report(self)
        return formatter.format_report(self)


class SessionReportComparison(FrozenBaseModel):
    """A comparison between two session reports."""

    session_id: str

    a: SessionReport
    b: SessionReport

    total_time: PerformanceMetricComparison[float]
    functions: Dict[str, Optional[FunctionReportComparison]]


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

        return SessionReportSetComparison(
            sessions={
                session: (
                    self.sessions[session].compare(other.sessions[session])
                    if session in self.sessions and session in other.sessions
                    else None
                )
                for session in all_sessions
            }
        )


class SessionReportSetComparison(FrozenBaseModel):
    """A comparison between two session report sets.

    If a session ID is not present in A or B, the comparison is None
    """

    sessions: Dict[str, Optional[SessionReportComparison]]


class PerformanceTracker:
    """Track performance metrics."""

    def __init__(self) -> None:
        """Initialize the tracker."""
        self._session_id: Optional[str] = None
        self._last_session_report: Optional[SessionReport] = None
        self.report_set = SessionReportSet()

    @contextmanager
    def session(self, session_id: str) -> Iterator[PerformanceTracker]:
        """Create a new measurement session.

        At session start, all state is fresh and it gets cleaned when the session ends.
        """
        if self._session_id:
            raise ValueError("Cannot create nested sessions.")

        self._session_id = session_id

        with cProfile.Profile() as profiler:
            yield self

        raw_stats = Stats(profiler)
        stats = raw_stats.strip_dirs().get_stats_profile()

        report = SessionReport.from_stats(session_id, stats)
        self._last_session_report = report
        self.report_set.add_report(report)

        self._session_id = None

    @property
    def last_session_report(self) -> SessionReport:
        """Get the report for the last session."""
        if not self._last_session_report:
            raise ValueError("Run the profiler before getting a report.")

        return self._last_session_report
