from __future__ import annotations

import inspect
from pathlib import Path
from typing import Iterator, Protocol

import pytest
from metricflow_semantics.dag.mf_dag import DagId
from metricflow_semantics.specs.query_spec import MetricFlowQuerySpec
from metricflow_semantics.test_helpers.config_helpers import DirectoryPathAnchor
from metricflow_semantics.test_helpers.performance.profiling import (
    PerformanceTracker,
    SessionReport,
)

from metricflow.dataflow.builder.dataflow_plan_builder import DataflowPlanBuilder
from metricflow.dataflow.optimizer.dataflow_optimizer_factory import DataflowPlanOptimization
from metricflow.plan_conversion.to_sql_plan.dataflow_to_sql import DataflowToSqlPlanConverter
from metricflow.protocols.sql_client import SqlClient
from metricflow.sql.optimizer.optimization_levels import SqlOptimizationLevel

ANCHOR = DirectoryPathAnchor()

GLOBAL_TRACKING_CONTEXT = "global"


def pytest_addoption(parser: pytest.Parser) -> None:
    """Add option for performance report file."""
    parser.addoption(
        "--output-json",
        action="store",
        default="git_ignored/performance-report.json",
        help="where to store performance results as JSON",
    )


@pytest.fixture(scope="session")
def perf_output_json(request: pytest.FixtureRequest) -> str:
    """The output json for the performance test."""
    return request.config.getoption("--output-json")  # type: ignore


class MeasureFixture(Protocol):  # noqa: D101
    def __call__(  # noqa: D102
        self,
        dataflow_plan_builder: DataflowPlanBuilder,
        dataflow_to_sql_converter: DataflowToSqlPlanConverter,
        sql_client: SqlClient,
        query_spec: MetricFlowQuerySpec,
    ) -> SessionReport:
        ...


@pytest.fixture(scope="session")
def measure_compilation_performance(
    perf_output_json: str,
) -> Iterator[MeasureFixture]:
    """Fixture that returns a function which measures compilation performance for a given query."""
    perf_tracker = PerformanceTracker()

    def _measure(
        dataflow_plan_builder: DataflowPlanBuilder,
        dataflow_to_sql_converter: DataflowToSqlPlanConverter,
        sql_client: SqlClient,
        query_spec: MetricFlowQuerySpec,
    ) -> SessionReport:
        caller = inspect.stack()[1]
        caller_filename = Path(caller.filename).relative_to(ANCHOR.directory)
        session_id = f"{caller_filename}::{caller.function}"

        with perf_tracker.session(session_id):
            is_distinct_values_plan = not query_spec.metric_specs
            if is_distinct_values_plan:
                optimized_plan = dataflow_plan_builder.build_plan_for_distinct_values(
                    query_spec, optimizations=DataflowPlanOptimization.enabled_optimizations()
                )
            else:
                optimized_plan = dataflow_plan_builder.build_plan(
                    query_spec, optimizations=DataflowPlanOptimization.enabled_optimizations()
                )
            _ = dataflow_to_sql_converter.convert_to_sql_plan(
                sql_engine_type=sql_client.sql_engine_type,
                dataflow_plan_node=optimized_plan.sink_node,
                optimization_level=SqlOptimizationLevel.O4,
                sql_query_plan_id=DagId.from_str("plan0_optimized"),
            )

        return perf_tracker.last_session_report

    yield _measure

    output_file_parent_dir = Path(perf_output_json).parent
    output_file_parent_dir.mkdir(exist_ok=True)
    with open(perf_output_json, "w") as f:
        f.write(perf_tracker.report_set.to_pretty_json())
