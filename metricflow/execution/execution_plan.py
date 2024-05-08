from __future__ import annotations

import logging
import time
from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import List, Optional, Sequence, Tuple

from metricflow_semantics.dag.id_prefix import IdPrefix, StaticIdPrefix
from metricflow_semantics.dag.mf_dag import DagId, DagNode, DisplayedProperty, MetricFlowDag, NodeId
from metricflow_semantics.sql.sql_bind_parameters import SqlBindParameters
from metricflow_semantics.visitor import Visitable

from metricflow.data_table.mf_table import MetricFlowDataTable
from metricflow.protocols.sql_client import SqlClient
from metricflow.sql.sql_table import SqlTable

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
        """Alias for node ID since the nodes represent a task."""
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
    # If the task produces a data_table as a result, it's stored here.
    df: Optional[MetricFlowDataTable] = None


class SelectSqlQueryToDataTableTask(ExecutionPlanTask):
    """A task that runs a SELECT and puts that result into a data_table."""

    def __init__(  # noqa: D107
        self,
        sql_client: SqlClient,
        sql_query: str,
        bind_parameters: SqlBindParameters,
        parent_nodes: Optional[List[ExecutionPlanTask]] = None,
    ) -> None:
        self._sql_client = sql_client
        self._sql_query = sql_query
        self._bind_parameters = bind_parameters
        super().__init__(task_id=self.create_unique_id(), parent_nodes=parent_nodes or [])

    @classmethod
    def id_prefix(cls) -> IdPrefix:  # noqa: D102
        return StaticIdPrefix.EXEC_NODE_READ_SQL_QUERY

    @property
    def description(self) -> str:  # noqa: D102
        return "Run a query and write the results to a data frame"

    @property
    def displayed_properties(self) -> Sequence[DisplayedProperty]:  # noqa: D102
        return tuple(super().displayed_properties) + (DisplayedProperty(key="sql_query", value=self._sql_query),)

    @property
    def bind_parameters(self) -> SqlBindParameters:  # noqa: D102
        return self._bind_parameters

    def execute(self) -> TaskExecutionResult:  # noqa: D102
        start_time = time.time()

        df = self._sql_client.query(
            self._sql_query,
            sql_bind_parameters=self.bind_parameters,
        )

        end_time = time.time()
        return TaskExecutionResult(
            start_time=start_time, end_time=end_time, sql=self._sql_query, bind_params=self.bind_parameters, df=df
        )

    @property
    def sql_query(self) -> Optional[SqlQuery]:  # noqa: D102
        return SqlQuery(
            sql_query=self._sql_query,
            bind_parameters=self._bind_parameters,
        )

    def __repr__(self) -> str:  # noqa: D105
        return f"{self.__class__.__name__}(sql_query='{self._sql_query}')"


class SelectSqlQueryToTableTask(ExecutionPlanTask):
    """A task that runs a SELECT and puts that result into a table.

    The provided SQL query is the query that will be run, so it should be a CREATE... or similar.
    """

    def __init__(  # noqa: D107
        self,
        sql_client: SqlClient,
        sql_query: str,
        bind_parameters: SqlBindParameters,
        output_table: SqlTable,
        parent_nodes: Optional[List[ExecutionPlanTask]] = None,
    ) -> None:
        self._sql_client = sql_client
        self._sql_query = sql_query
        self._output_table = output_table
        self._bind_parameters = bind_parameters
        super().__init__(task_id=self.create_unique_id(), parent_nodes=parent_nodes or [])

    @classmethod
    def id_prefix(cls) -> IdPrefix:  # noqa: D102
        return StaticIdPrefix.EXEC_NODE_WRITE_TO_TABLE

    @property
    def description(self) -> str:  # noqa: D102
        return "Run a query and write the results to a table"

    @property
    def displayed_properties(self) -> Sequence[DisplayedProperty]:  # noqa: D102
        return tuple(super().displayed_properties) + (
            DisplayedProperty(key="sql_query", value=self._sql_query),
            DisplayedProperty(key="output_table", value=self._output_table),
            DisplayedProperty(key="bind_parameters", value=self._bind_parameters),
        )

    def execute(self) -> TaskExecutionResult:  # noqa: D102
        start_time = time.time()
        logger.info(f"Dropping table {self._output_table} in case it already exists")
        self._sql_client.execute(f"DROP TABLE IF EXISTS {self._output_table.sql}")
        logger.info(f"Creating table {self._output_table} using a query")
        self._sql_client.execute(
            self._sql_query,
            sql_bind_parameters=self._bind_parameters,
        )

        end_time = time.time()
        return TaskExecutionResult(start_time=start_time, end_time=end_time, sql=self._sql_query)

    @property
    def sql_query(self) -> Optional[SqlQuery]:  # noqa: D102
        return SqlQuery(sql_query=self._sql_query, bind_parameters=self._bind_parameters)

    def __repr__(self) -> str:  # noqa: D105
        return f"{self.__class__.__name__}(sql_query='{self._sql_query}', output_table={self._output_table})"


class ExecutionPlan(MetricFlowDag[ExecutionPlanTask]):
    """A DAG where the nodes are tasks, and parents represent prerequisite tasks."""

    def __init__(self, leaf_tasks: Sequence[ExecutionPlanTask], dag_id: Optional[DagId] = None) -> None:
        """Constructor.

        Args:
            leaf_tasks: The final set of tasks that will run, after task dependencies are finished.
        """
        super().__init__(
            dag_id=dag_id or DagId.from_id_prefix(StaticIdPrefix.EXEC_PLAN_PREFIX), sink_nodes=tuple(leaf_tasks)
        )

    @property
    def tasks(self) -> Sequence[ExecutionPlanTask]:
        """Return all tasks in this plan."""
        if len(self.sink_nodes) == 0:
            return ()

        def recursively_get_tasks(task: ExecutionPlanTask) -> List[ExecutionPlanTask]:
            tasks_to_return = []
            for parent_node in task.parent_nodes:
                tasks_to_return.extend(recursively_get_tasks(parent_node))
            tasks_to_return.append(task)

            return tasks_to_return

        assert len(self.sink_nodes) == 1
        return tuple(recursively_get_tasks(self.sink_nodes[0]))
