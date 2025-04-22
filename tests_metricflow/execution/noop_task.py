from __future__ import annotations

import logging
import time
from dataclasses import dataclass
from typing import ClassVar, Sequence

from metricflow_semantics.dag.id_prefix import IdPrefix, StaticIdPrefix

from metricflow.execution.execution_plan import (
    ExecutionPlanTask,
    TaskExecutionError,
    TaskExecutionResult,
)

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class NoOpExecutionPlanTask(ExecutionPlanTask):
    """A no-op task for testing executors.

    Attributes:
        should_error: If true, test the error flow by intentionally returning an error in the results.
    """

    EXAMPLE_ERROR: ClassVar[TaskExecutionError] = TaskExecutionError("Expected Error")

    should_error: bool = False

    @staticmethod
    def create(  # noqa: D102
        parent_tasks: Sequence[ExecutionPlanTask] = (),
        should_error: bool = False,
    ) -> NoOpExecutionPlanTask:
        return NoOpExecutionPlanTask(
            parent_nodes=tuple(parent_tasks),
            sql_statement=None,
            should_error=should_error,
        )

    @property
    def description(self) -> str:  # noqa: D102
        return "Dummy No-Op"

    @classmethod
    def id_prefix(cls) -> IdPrefix:  # noqa: D102
        return StaticIdPrefix.EXEC_NODE_NOOP

    def execute(self) -> TaskExecutionResult:  # noqa: D102
        start_time = time.perf_counter()
        time.sleep(0.01)
        end_time = time.perf_counter()
        return TaskExecutionResult(
            start_time=start_time, end_time=end_time, errors=(self.EXAMPLE_ERROR,) if self.should_error else ()
        )
