import logging
import time
from typing import Sequence, Optional

from metricflow.dag.id_generation import EXEC_NODE_NOOP
from metricflow.execution.execution_plan import (
    ExecutionPlanTask,
    TaskExecutionResult,
    TaskExecutionError,
    SqlQuery,
)

logger = logging.getLogger(__name__)


class NoOpExecutionPlanTask(ExecutionPlanTask):
    """A no-op task for testing executors."""

    # Error to return if should_error is set.
    EXAMPLE_ERROR = TaskExecutionError("Expected Error")

    def __init__(self, parent_tasks: Sequence[ExecutionPlanTask] = (), should_error: bool = False) -> None:  # noqa: D
        """Constructor.

        Args:
            parent_tasks: Self-explanatory.
            should_error: if true, return an error in the results.
        """
        self._should_error = should_error
        super().__init__(task_id=self.create_unique_id(), parent_nodes=list(parent_tasks))

    @property
    def description(self) -> str:  # noqa: D
        return "Dummy No-Op"

    @classmethod
    def id_prefix(cls) -> str:  # noqa: D
        return EXEC_NODE_NOOP

    def execute(self) -> TaskExecutionResult:  # noqa: D
        start_time = time.time()
        time.sleep(0.01)
        end_time = time.time()
        return TaskExecutionResult(
            start_time=start_time, end_time=end_time, errors=(self.EXAMPLE_ERROR,) if self._should_error else ()
        )

    @property
    def sql_query(self) -> Optional[SqlQuery]:  # noqa: D
        return None
