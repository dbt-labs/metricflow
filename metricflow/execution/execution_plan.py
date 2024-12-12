from __future__ import annotations

import logging
import time
from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import List, Optional, Sequence, Tuple

from metricflow_semantics.dag.id_prefix import IdPrefix, StaticIdPrefix
from metricflow_semantics.dag.mf_dag import DagId, DagNode, DisplayedProperty, MetricFlowDag, NodeId
from metricflow_semantics.mf_logging.lazy_formattable import LazyFormat
from metricflow_semantics.sql.sql_bind_parameters import SqlBindParameterSet
from metricflow_semantics.sql.sql_table import SqlTable
from metricflow_semantics.visitor import Visitable

from metricflow.data_table.mf_table import MetricFlowDataTable
from metricflow.protocols.sql_client import SqlClient

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class ExecutionPlanTask(DagNode["ExecutionPlanTask"], Visitable, ABC):
    """A node (aka task) in the DAG representation of the execution plan.

    In the DAG, a node's parents represent the tasks that need to be run before the node can run. Using the term task
    for these nodes as it seems more intuitive.

    Attributes:
        sql_statement: If this runs a SQL query, return the associated SQL.
    """

    sql_statement: Optional[SqlStatement]

    @abstractmethod
    def execute(self) -> TaskExecutionResult:
        """Execute the actions of this node."""
        raise NotImplementedError

    @property
    def task_id(self) -> NodeId:
        """Alias for node ID since the nodes represent a task."""
        return self.node_id


@dataclass(frozen=True)
class SqlStatement:
    """Encapsulates a SQL statement along with the bind parameters that should be used."""

    # This field will be renamed as it is confusing given the class name.
    sql: str
    bind_parameter_set: SqlBindParameterSet

    @property
    def without_descriptions(self) -> SqlStatement:
        """Return the SQL query without the inline descriptions."""
        return SqlStatement(
            sql="\n".join(
                filter(
                    lambda line: not line.strip().startswith("--"),
                    self.sql.split("\n"),
                )
            ),
            bind_parameter_set=self.bind_parameter_set,
        )


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
    bind_params: Optional[SqlBindParameterSet] = None
    # If the task produces a data_table as a result, it's stored here.
    df: Optional[MetricFlowDataTable] = None


@dataclass(frozen=True)
class SelectSqlQueryToDataTableTask(ExecutionPlanTask):
    """A task that runs a SELECT and puts that result into a data_table.

    Attributes:
        sql_client: The SQL client used to run the query.
        sql_statement: The SQL query to run.
        parent_nodes: The parent tasks for this execution plan task.
    """

    sql_client: SqlClient
    parent_nodes: Tuple[ExecutionPlanTask, ...]

    @staticmethod
    def create(  # noqa: D102
        sql_client: SqlClient,
        sql_statement: SqlStatement,
        parent_nodes: Sequence[ExecutionPlanTask] = (),
    ) -> SelectSqlQueryToDataTableTask:
        return SelectSqlQueryToDataTableTask(
            sql_client=sql_client,
            sql_statement=sql_statement,
            parent_nodes=tuple(parent_nodes),
        )

    @classmethod
    def id_prefix(cls) -> IdPrefix:  # noqa: D102
        return StaticIdPrefix.EXEC_NODE_READ_SQL_QUERY

    @property
    def description(self) -> str:  # noqa: D102
        return "Run a query and write the results to a data frame"

    @property
    def displayed_properties(self) -> Sequence[DisplayedProperty]:  # noqa: D102
        assert self.sql_statement is not None, f"{self.sql_statement=} should have been set during creation."
        return tuple(super().displayed_properties) + (DisplayedProperty(key="sql", value=self.sql_statement.sql),)

    def execute(self) -> TaskExecutionResult:  # noqa: D102
        start_time = time.time()
        sql_statement = self.sql_statement
        assert sql_statement is not None, f"{self.sql_statement=} should have been set during creation."

        df = self.sql_client.query(
            sql_statement.sql,
            sql_bind_parameter_set=sql_statement.bind_parameter_set,
        )

        end_time = time.time()
        return TaskExecutionResult(
            start_time=start_time,
            end_time=end_time,
            sql=sql_statement.sql,
            bind_params=sql_statement.bind_parameter_set,
            df=df,
        )

    def __repr__(self) -> str:  # noqa: D105
        return f"{self.__class__.__name__}(sql_statement={self.sql_statement!r})"


@dataclass(frozen=True)
class SelectSqlQueryToTableTask(ExecutionPlanTask):
    """A task that runs a SELECT and puts that result into a table.

    The provided SQL query is the query that will be run, so it should be a CREATE... or similar.

    Attributes:
        sql_client: The SQL client used to run the query.
        sql_statement: The SQL query to run.
        output_table: The table where the results will be written.
    """

    sql_client: SqlClient
    output_table: SqlTable

    @staticmethod
    def create(  # noqa: D102
        sql_client: SqlClient,
        sql_statement: SqlStatement,
        output_table: SqlTable,
        parent_nodes: Sequence[ExecutionPlanTask] = (),
    ) -> SelectSqlQueryToTableTask:
        return SelectSqlQueryToTableTask(
            sql_client=sql_client,
            sql_statement=sql_statement,
            output_table=output_table,
            parent_nodes=tuple(parent_nodes),
        )

    @classmethod
    def id_prefix(cls) -> IdPrefix:  # noqa: D102
        return StaticIdPrefix.EXEC_NODE_WRITE_TO_TABLE

    @property
    def description(self) -> str:  # noqa: D102
        return "Run a query and write the results to a table"

    @property
    def displayed_properties(self) -> Sequence[DisplayedProperty]:  # noqa: D102
        sql_statement = self.sql_statement
        assert sql_statement is not None, f"{self.sql_statement=} should have been set during creation."
        return tuple(super().displayed_properties) + (
            DisplayedProperty(key="sql_statement", value=sql_statement.sql),
            DisplayedProperty(key="output_table", value=self.output_table),
            DisplayedProperty(key="bind_parameter_set", value=sql_statement.bind_parameter_set),
        )

    def execute(self) -> TaskExecutionResult:  # noqa: D102
        sql_statement = self.sql_statement
        assert sql_statement is not None, f"{self.sql_statement=} should have been set during creation."
        start_time = time.time()
        logger.debug(LazyFormat(lambda: f"Dropping table {self.output_table} in case it already exists"))
        self.sql_client.execute(f"DROP TABLE IF EXISTS {self.output_table.sql}")
        logger.debug(LazyFormat(lambda: f"Creating table {self.output_table} using a query"))
        self.sql_client.execute(
            sql_statement.sql,
            sql_bind_parameter_set=sql_statement.bind_parameter_set,
        )

        end_time = time.time()
        return TaskExecutionResult(start_time=start_time, end_time=end_time, sql=sql_statement.sql)

    def __repr__(self) -> str:  # noqa: D105
        return f"{self.__class__.__name__}(sql_statement={self.sql_statement!r}', output_table={self.output_table})"


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
