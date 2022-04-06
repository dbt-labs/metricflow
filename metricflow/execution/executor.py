import logging
from abc import ABC, abstractmethod
from collections import OrderedDict
from typing import Dict

from metricflow.dag.mf_dag import NodeId
from metricflow.execution.execution_plan import ExecutionPlan, ExecutionPlanTask, TaskExecutionResult

logger = logging.getLogger(__name__)


class ExecutionResults:
    """Stores the results from executing the tasks in an execution plan."""

    def __init__(self) -> None:  # noqa: D
        # Dict from the task to the result.
        self._results: OrderedDict[NodeId, TaskExecutionResult] = OrderedDict()

    def add_result(self, task_id: NodeId, result: TaskExecutionResult) -> None:
        """Adds the results of executing a task to this."""
        assert task_id not in self._results, f"Task ID: {task_id} already in results as {self._results[task_id]}"
        self._results[task_id] = result

    @property
    def contains_task_errors(self) -> bool:
        """Returns true if any of the tasks had an error."""
        return any([len(result.errors) > 0 for node_id, result in self._results.items()])

    def get_result(self, task_id: NodeId) -> TaskExecutionResult:  # noqa: D
        assert task_id in self._results
        return self._results[task_id]

    def all_results(self) -> Dict[NodeId, TaskExecutionResult]:  # noqa: D
        return self._results


class ExecutionPlanExecutor(ABC):
    """Runs the tasks in an execution plan."""

    @abstractmethod
    def execute_plan(self, plan: ExecutionPlan) -> ExecutionResults:  # noqa: D
        pass


class SequentialPlanExecutor(ExecutionPlanExecutor):
    """Execute tasks in the order of the dependencies in the plan, one by one."""

    def _execute_dfs(self, current_task: ExecutionPlanTask, results: ExecutionResults) -> None:
        """Traverse the execution plan via depth first search.

        Executing the parents tasks first, then the current task.
        """
        for parent_node in current_task.parent_nodes:
            self._execute_dfs(parent_node, results)
            if results.contains_task_errors:
                return

        result = None
        logger.info(f"Started task ID: {current_task.node_id}")
        try:
            result = current_task.execute()
            results.add_result(current_task.task_id, result)
        finally:

            if result:
                runtime = f"{result.end_time - result.start_time:.2f}s"
                if result.errors:
                    logger.info(f"Finished task ID: {current_task.node_id} with errors: {result.errors} in {runtime}")
                else:
                    logger.info(f"Finished task ID: {current_task.node_id} successfully in {runtime}")
            else:
                logger.info(f"Task ID: {current_task.node_id} exited unexpectedly")

    def execute_plan(self, plan: ExecutionPlan) -> ExecutionResults:  # noqa: D
        results = ExecutionResults()

        for leaf_node in plan.sink_nodes:
            self._execute_dfs(leaf_node, results)

        return results
