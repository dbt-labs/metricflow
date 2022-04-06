from metricflow.execution.execution_plan import ExecutionPlan
from metricflow.execution.executor import SequentialPlanExecutor
from metricflow.test.execution.noop_task import NoOpExecutionPlanTask


def test_single_task() -> None:
    """Tests running an execution plan with a single task."""
    task = NoOpExecutionPlanTask()
    execution_plan = ExecutionPlan("plan0", leaf_tasks=[task])
    results = SequentialPlanExecutor().execute_plan(execution_plan)
    assert results.get_result(task.task_id)


def test_single_task_error() -> None:
    """Check that an error is properly returned in the results if a task errors out."""
    task = NoOpExecutionPlanTask(should_error=True)
    execution_plan = ExecutionPlan("plan0", leaf_tasks=[task])
    executor = SequentialPlanExecutor()
    results = executor.execute_plan(execution_plan)
    assert len(results.get_result(task.task_id).errors) == 1
    assert results.get_result(task.task_id).errors[0] == NoOpExecutionPlanTask.EXAMPLE_ERROR


def test_task_with_parents() -> None:
    """Tests a plan with a task that has 2 direct parents."""
    parent_task1 = NoOpExecutionPlanTask()
    parent_task2 = NoOpExecutionPlanTask()
    leaf_task = NoOpExecutionPlanTask(parent_tasks=[parent_task1, parent_task2])
    execution_plan = ExecutionPlan("plan0", leaf_tasks=[leaf_task])
    results = SequentialPlanExecutor().execute_plan(execution_plan)

    # Check that they all finished.
    parent_result1 = results.get_result(parent_task1.task_id)
    parent_result2 = results.get_result(parent_task2.task_id)
    leaf_result = results.get_result(leaf_task.task_id)

    # Check that parents completed before the leaf.
    assert parent_result1.end_time < leaf_result.end_time
    assert parent_result2.end_time < leaf_result.end_time

    assert not results.contains_task_errors


def test_parent_task_error() -> None:
    """Check that a child task is not run if a parent task fails."""
    parent_task1 = NoOpExecutionPlanTask(should_error=True)
    parent_task2 = NoOpExecutionPlanTask()
    leaf_task = NoOpExecutionPlanTask(parent_tasks=[parent_task1, parent_task2])
    execution_plan = ExecutionPlan("plan0", leaf_tasks=[leaf_task])

    executor = SequentialPlanExecutor()
    results = executor.execute_plan(execution_plan)
    assert len(results.all_results()) == 1
    assert results.get_result(parent_task1.task_id).errors[0] == NoOpExecutionPlanTask.EXAMPLE_ERROR
