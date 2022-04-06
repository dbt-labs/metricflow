from __future__ import annotations

import logging
import textwrap
import time
from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import List, Sequence, Optional, Tuple

import jinja2
import pandas as pd

from metricflow.protocols.sql_client import SqlClient
from metricflow.dag.mf_dag import DagNode, MetricFlowDag, NodeId, DisplayedProperty
from metricflow.dag.id_generation import EXEC_NODE_READ_SQL_QUERY, EXEC_NODE_WRITE_TO_TABLE
from metricflow.dataflow.sql_table import SqlTable
from metricflow.sql.sql_bind_parameters import SqlBindParameters
from metricflow.visitor import Visitable

logger = logging.getLogger(__name__)


class ExecutionPlanTask(DagNode, Visitable, ABC):
    """A node (aka task) in the DAG representation of the execution plan.

    In the DAG, a node's parents represent the tasks that need to be run before the node can run. Using the term task
    for these nodes as it seems more intuitive.
    """

    def __init__(self, task_id: NodeId, parent_nodes: List[ExecutionPlanTask]) -> None:
        """Constructor.

        Args:
            task_id: the ID for the node
            parent_nodes: the nodes that should be executed before this one.
        """
        self._parent_nodes = parent_nodes
        super().__init__(node_id=task_id)

    @property
    def parent_nodes(self) -> Sequence[ExecutionPlanTask]:
        """Return the nodes that should execute before this one."""
        return self._parent_nodes

    @abstractmethod
    def execute(self) -> TaskExecutionResult:
        """Execute the actions of this node."""

    @property
    def task_id(self) -> NodeId:
        """Alias for node ID since the nodes represent a task"""
        return self.node_id

    @property
    @abstractmethod
    def sql_query(self) -> Optional[SqlQuery]:
        """If this runs a SQL query, return the associated SQL."""
        pass


@dataclass(frozen=True)
class SqlQuery:
    """A SQL query that can be run along with bind parameters."""

    sql_query: str
    bind_parameters: SqlBindParameters


@dataclass(frozen=True)
class TaskExecutionError(Exception):
    """Error if a task fails."""

    error_str: str


@dataclass(frozen=True)
class TaskExecutionResult:
    """The results of running a task."""

    start_time: float
    end_time: float
    errors: Tuple[TaskExecutionError, ...] = ()

    # If the task was an SQL query, it's stored here
    sql: Optional[str] = None
    bind_params: Optional[SqlBindParameters] = None
    # If the task produces a dataframe as a result, it's stored here.
    df: Optional[pd.DataFrame] = None


class SelectSqlQueryToDataFrameTask(ExecutionPlanTask):
    """A task that runs a SELECT and puts that result into a dataframe."""

    def __init__(  # noqa: D
        self,
        sql_client: SqlClient,
        sql_query: str,
        execution_parameters: SqlBindParameters,
        parent_nodes: Optional[List[ExecutionPlanTask]] = None,
    ) -> None:

        self._sql_client = sql_client
        self._sql_query = sql_query
        self._execution_parameters = execution_parameters
        super().__init__(task_id=self.create_unique_id(), parent_nodes=parent_nodes or [])

    @classmethod
    def id_prefix(cls) -> str:  # noqa: D
        return EXEC_NODE_READ_SQL_QUERY

    @property
    def description(self) -> str:  # noqa: D
        return "Run a query and write the results to a data frame"

    @property
    def displayed_properties(self) -> List[DisplayedProperty]:  # noqa: D
        return super().displayed_properties + [DisplayedProperty(key="sql_query", value=self._sql_query)]

    @property
    def execution_parameters(self) -> SqlBindParameters:  # noqa: D
        return self._execution_parameters

    def execute(self) -> TaskExecutionResult:  # noqa: D
        start_time = time.time()
        df = self._sql_client.query(stmt=self._sql_query, sql_bind_parameters=self.execution_parameters)
        end_time = time.time()
        return TaskExecutionResult(
            start_time=start_time, end_time=end_time, sql=self._sql_query, bind_params=self.execution_parameters, df=df
        )

    @property
    def sql_query(self) -> Optional[SqlQuery]:  # noqa: D
        return SqlQuery(
            sql_query=self._sql_query,
            bind_parameters=self._execution_parameters,
        )

    def __repr__(self) -> str:  # noqa: D
        return f"{self.__class__.__name__}(sql_query='{self._sql_query}')"


class SelectSqlQueryToTableTask(ExecutionPlanTask):
    """A task that runs a SELECT and puts that result into a table."""

    def __init__(  # noqa: D
        self,
        sql_client: SqlClient,
        sql_query: str,
        execution_parameters: SqlBindParameters,
        output_table: SqlTable,
        parent_nodes: Optional[List[ExecutionPlanTask]] = None,
    ) -> None:
        self._sql_client = sql_client
        self._sql_query = sql_query
        self._output_table = output_table
        self._execution_parameters = execution_parameters
        super().__init__(task_id=self.create_unique_id(), parent_nodes=parent_nodes or [])

    @classmethod
    def id_prefix(cls) -> str:  # noqa: D
        return EXEC_NODE_WRITE_TO_TABLE

    @property
    def description(self) -> str:  # noqa: D
        return "Run a query and write the results to a table"

    @property
    def displayed_properties(self) -> List[DisplayedProperty]:  # noqa: D
        return super().displayed_properties + [
            DisplayedProperty(key="sql_query", value=self._sql_query),
            DisplayedProperty(key="output_table", value=self._output_table),
            DisplayedProperty(key="execution_parameters", value=self._execution_parameters),
        ]

    def execute(self) -> TaskExecutionResult:  # noqa: D
        start_time = time.time()
        logger.info(f"Dropping table {self._output_table} in case it already exists")
        self._sql_client.drop_table(self._output_table)
        logger.info(f"Creating table {self._output_table} using a SELECT query")
        self._sql_client.create_table_as_select(
            self._output_table,
            select_query=self._sql_query,
            sql_bind_parameters=self._execution_parameters,
        )
        end_time = time.time()
        return TaskExecutionResult(start_time=start_time, end_time=end_time, sql=self._sql_query)

    @property
    def sql_query(self) -> Optional[SqlQuery]:  # noqa: D
        query_text = jinja2.Template(
            textwrap.dedent(
                """\
                CREATE TABLE {{ output_table }} AS (
                  {{ select_query | indent(2) }}
                )
                """
            ),
            undefined=jinja2.StrictUndefined,
        ).render(output_table=self._output_table.sql, select_query=self._sql_query)

        return SqlQuery(
            sql_query=query_text,
            bind_parameters=self._execution_parameters,
        )

    def __repr__(self) -> str:  # noqa: D
        return f"{self.__class__.__name__}(sql_query='{self._sql_query}', output_table={self._output_table})"


class ExecutionPlan(MetricFlowDag[ExecutionPlanTask]):
    """A DAG where the nodes are tasks, and parents represent prerequisite tasks."""

    def __init__(self, plan_id: str, leaf_tasks: List[ExecutionPlanTask]) -> None:
        """Constructor.

        Args:
            plan_id: A string to uniquely identify this plan.
            leaf_tasks: The final set of tasks that will run, after task dependencies are finished.
        """
        super().__init__(dag_id=plan_id, sink_nodes=leaf_tasks)

    @property
    def tasks(self) -> Sequence[ExecutionPlanTask]:
        """Return all tasks in this plan."""
        if len(self.sink_nodes) == 0:
            return []

        def recursively_get_tasks(task: ExecutionPlanTask) -> List[ExecutionPlanTask]:
            tasks_to_return = []
            for parent_node in task.parent_nodes:
                tasks_to_return.extend(recursively_get_tasks(parent_node))
            tasks_to_return.append(task)

            return tasks_to_return

        assert len(self.sink_nodes) == 1
        return recursively_get_tasks(self.sink_nodes[0])
