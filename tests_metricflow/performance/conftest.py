from __future__ import annotations

import inspect
from pathlib import Path
from typing import Iterator, Protocol

import pytest
from metricflow_semantics.dag.mf_dag import DagId
from metricflow_semantics.specs.query_spec import MetricFlowQuerySpec
from metricflow_semantics.test_helpers.config_helpers import DirectoryPathAnchor
from metricflow_semantics.test_helpers.performance_helpers import (
    PerformanceTracker,
    SessionReport,
    track_performance,
)

from metricflow.dataflow.builder.dataflow_plan_builder import DataflowPlanBuilder
from metricflow.dataflow.optimizer.dataflow_optimizer_factory import DataflowPlanOptimization
from metricflow.plan_conversion.dataflow_to_sql import DataflowToSqlQueryPlanConverter
from metricflow.protocols.sql_client import SqlClient
from metricflow.sql.optimizer.optimization_levels import SqlQueryOptimizationLevel

ANCHOR = DirectoryPathAnchor()

GLOBAL_TRACKING_CONTEXT = "global"


@pytest.fixture(scope="session")
def perf_tracker() -> PerformanceTracker:
    """Instrument MetricFlow with performance tracking utilities."""
    with track_performance() as perf:
        return perf


class MeasureFixture(Protocol):  # noqa: D101
    def __call__(  # noqa: D102
        self,
        dataflow_plan_builder: DataflowPlanBuilder,
        dataflow_to_sql_converter: DataflowToSqlQueryPlanConverter,
        sql_client: SqlClient,
        query_spec: MetricFlowQuerySpec,
    ) -> SessionReport:
        ...


@pytest.fixture(scope="session")
def measure_compilation_performance(perf_tracker: PerformanceTracker) -> Iterator[MeasureFixture]:
    """Fixture that returns a function which measures compilation performance for a given query."""

    def _measure(
        dataflow_plan_builder: DataflowPlanBuilder,
        dataflow_to_sql_converter: DataflowToSqlQueryPlanConverter,
        sql_client: SqlClient,
        query_spec: MetricFlowQuerySpec,
    ) -> SessionReport:
        caller = inspect.stack()[1]
        caller_filename = Path(caller.filename).relative_to(ANCHOR.directory)
        session_id = f"{caller_filename}::{caller.function}"

        with perf_tracker.session(session_id):
            with perf_tracker.measure(GLOBAL_TRACKING_CONTEXT):
                is_distinct_values_plan = not query_spec.metric_specs
                if is_distinct_values_plan:
                    optimized_plan = dataflow_plan_builder.build_plan_for_distinct_values(
                        query_spec, optimizations=DataflowPlanOptimization.enabled_optimizations()
                    )
                else:
                    optimized_plan = dataflow_plan_builder.build_plan(
                        query_spec, optimizations=DataflowPlanOptimization.enabled_optimizations()
                    )
                _ = dataflow_to_sql_converter.convert_to_sql_query_plan(
                    sql_engine_type=sql_client.sql_engine_type,
                    dataflow_plan_node=optimized_plan.sink_node,
                    optimization_level=SqlQueryOptimizationLevel.O4,
                    sql_query_plan_id=DagId.from_str("plan0_optimized"),
                )

            report = perf_tracker.get_session_report()

        return report

    yield _measure

    report_set = perf_tracker.get_report_set()

    print(report_set.to_pretty_json())
