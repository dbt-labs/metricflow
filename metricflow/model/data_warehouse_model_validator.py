from __future__ import annotations

from collections import OrderedDict
from dataclasses import dataclass, field
from math import floor
from time import perf_counter
from typing import List, Optional
from typing import OrderedDict as ODType

from metricflow.engine.metricflow_engine import MetricFlowEngine, MetricFlowExplainResult, MetricFlowQueryRequest
from metricflow.model.objects.data_source import DataSource
from metricflow.model.objects.user_configured_model import UserConfiguredModel
from metricflow.model.semantic_model import SemanticModel
from metricflow.model.validations.validator_helpers import ValidationError, ValidationIssue, ValidationWarning
from metricflow.protocols.sql_client import SqlClient
from metricflow.sql.sql_bind_parameters import SqlBindParameters


@dataclass
class DataWarehouseValidationTask:
    """Dataclass for defining a task to be used in the DataWarehouseModelValidator"""

    query_string: str
    error_message: str
    object_ref: ODType = field(default_factory=lambda: OrderedDict())
    query_params: SqlBindParameters = field(default_factory=lambda: SqlBindParameters())
    on_fail_subtasks: List[DataWarehouseValidationTask] = field(default_factory=lambda: [])


class DataWarehouseTaskBuilder:
    """Task builder for standard data warehouse validation tasks"""

    QUERY_TEMPLATE = "SELECT {columns_select} FROM {data_source} AS source{unique_id}"
    WHERE_TEMPLATE = " WHERE {where_filters}"
    PARTITION_COL_TEMPLATE = "{partition} IS NOT NULL"
    WRAPPED_COL_TEMPLATE = "({column}) AS col{unique_id}"

    @staticmethod
    def _wrap_col(col: str, id: int) -> str:  # noqa: D
        return DataWarehouseTaskBuilder.WRAPPED_COL_TEMPLATE.format(column=col, unique_id=id)

    @staticmethod
    def _where_clause_from_partitions(data_source: DataSource) -> str:
        """Takes the partitions for a data source and genrates a WHERE clause based on their definintions"""
        if not data_source.partitions:
            return ""

        where_stmts: List[str] = []
        for partition in data_source.partitions:
            element = f"({partition.expr})" if partition.expr else partition.name.element_name
            where_stmts.append(DataWarehouseTaskBuilder.PARTITION_COL_TEMPLATE.format(partition=element))
        return DataWarehouseTaskBuilder.WHERE_TEMPLATE.format(where_filters=" AND ".join(where_stmts))

    @staticmethod
    def _gen_query(data_source: DataSource, id: int, columns: List[str] = ["true"]) -> str:
        """Generates a basic sql query to select parts of them model definition

        Generates a basic sql query for verifying the model's definition with
        the data warehouse. For example if a data source with an sql_table
        value of 'table1' and no partition was passed in no columns list the
        query generated would be "SELECT (true) as col1 FROM table1 AS source0". If a the
        data source had a partition, say on the dimension 'dim1' then the query
        would be "SELECT (true) as col1 FROM table1 AS source0 WHERE dim1 IS NOT NULL".
        And if in addition columns ["dim2", "dim3"] were passed in then the
        query would be "SELECT (dim2) as col1, (dim3) as col2 FROM table1 AS source0 WHERE
        dim1 IS NOT NULL".

        Args:
            data_source: The data source to generate the query for
            id: A unique id used to alias the table reference
            columns: Column strings to select
        Returns:
            A string representing the query for the passed in arguments.
        """
        data_source_str = data_source.sql_table if data_source.sql_table else f"({data_source.sql_query})"

        wrapped_cols = [DataWarehouseTaskBuilder._wrap_col(col, index) for index, col in enumerate(columns)]
        columns_select = ", ".join(wrapped_cols)

        query = DataWarehouseTaskBuilder.QUERY_TEMPLATE.format(
            data_source=data_source_str, columns_select=columns_select, unique_id=id
        )
        query += DataWarehouseTaskBuilder._where_clause_from_partitions(data_source=data_source)
        return query

    @staticmethod
    def gen_data_source_tasks(model: UserConfiguredModel) -> List[DataWarehouseValidationTask]:
        """Generates a list of tasks for validating the data sources of the model"""
        tasks: List[DataWarehouseValidationTask] = []
        for index, data_source in enumerate(model.data_sources):
            tasks.append(
                DataWarehouseValidationTask(
                    query_string=DataWarehouseTaskBuilder._gen_query(data_source=data_source, id=index),
                    object_ref=ValidationIssue.make_object_reference(data_source_name=data_source.name),
                    error_message=f"Unable to access data source `{data_source.name}` in data warehouse",
                )
            )
        return tasks

    @staticmethod
    def gen_dimension_tasks(model: UserConfiguredModel) -> List[DataWarehouseValidationTask]:
        """Generates a list of tasks for validating the dimensions of the model

        The high level tasks returned are "short cut" queries which try to
        query all the dimensions for a given data source. If that query fails,
        one or more of the dimensions is incorrectly specified. Thus if the
        query fails, there are subtasks which query the individual dimensions
        on the data source to identify which have issues.
        """

        tasks: List[DataWarehouseValidationTask] = []
        for index, data_source in enumerate(model.data_sources):
            # generate the subtasks
            data_source_tasks: List[DataWarehouseValidationTask] = []
            data_source_columns: List[str] = []
            for dimension in data_source.dimensions:
                dim_to_query = dimension.expr if dimension.expr is not None else dimension.name.element_name
                data_source_columns.append(dim_to_query)

                data_source_tasks.append(
                    DataWarehouseValidationTask(
                        query_string=DataWarehouseTaskBuilder._gen_query(
                            data_source=data_source, id=index, columns=[dim_to_query]
                        ),
                        object_ref=ValidationIssue.make_object_reference(
                            data_source_name=data_source.name, dimension_name=dimension.name.element_name
                        ),
                        error_message=f"Unable to query `{dim_to_query}` in data warehouse for dimension `{dimension.name.element_name}` on data source `{data_source.name}`.",
                    )
                )

            # generate the shortcut tasks with sub tasks
            tasks.append(
                DataWarehouseValidationTask(
                    query_string=DataWarehouseTaskBuilder._gen_query(
                        data_source=data_source,
                        id=index,
                        columns=data_source_columns,
                    ),
                    object_ref=ValidationIssue.make_object_reference(data_source_name=data_source.name),
                    error_message=f"Failed to query dimensions in data warehouse for data source `{data_source.name}`",
                    on_fail_subtasks=data_source_tasks,
                )
            )
        return tasks

    @staticmethod
    def gen_metric_tasks(model: UserConfiguredModel, mf_engine: MetricFlowEngine) -> List[DataWarehouseValidationTask]:
        """Generates a list of tasks for validating the metrics of the model"""
        primary_time_dim = SemanticModel(
            user_configured_model=model
        ).data_source_semantics.primary_time_dimension_reference
        tasks: List[DataWarehouseValidationTask] = []
        for metric in model.metrics:
            mf_query = MetricFlowQueryRequest.create_with_random_request_id(
                metric_names=[metric.name], group_by_names=[primary_time_dim.element_name]
            )
            explain_result: MetricFlowExplainResult = mf_engine.explain(mf_request=mf_query)
            tasks.append(
                DataWarehouseValidationTask(
                    query_string=explain_result.rendered_sql.sql_query,
                    query_params=explain_result.rendered_sql.bind_parameters,
                    object_ref=ValidationIssue.make_object_reference(metric_name=metric.name),
                    error_message=f"Unable to query metric `{metric.name}`.",
                )
            )
        return tasks


class DataWarehouseModelValidator:
    """A Validator for checking specific tasks for the model against the Data Warehouse

    Data Warehouse Validations are validations that are done against the data
    warehouse based on the model configured by the user. Their purpose is to
    ensure that queries generated by MetricFlow won't fail when you go to use
    them (assuming the model has passed these validations before use).
    """

    def __init__(self, sql_client: SqlClient, mf_engine: MetricFlowEngine) -> None:  # noqa: D
        self._sql_client = sql_client
        self._mf_engine = mf_engine

    def run_tasks(
        self, tasks: List[DataWarehouseValidationTask], timeout: Optional[int] = None
    ) -> List[ValidationIssue]:
        """Runs the list of tasks as queries agains the data warehouse, returning any found issues

        Args:
            tasks: A list of tasks to run against the data warehouse
            timeout: An optional timeout. Default is None. When the timeout is hit, function will return early.

        Returns:
            A list of validation issues discovered when running the passed in tasks against the data warehosue
        """

        # Used for keeping track if we go past the max time
        start_time = perf_counter()

        issues: List[ValidationIssue] = []
        # TODO: Asyncio implementation
        for index, task in enumerate(tasks):
            if timeout is not None and perf_counter() - start_time > timeout:
                issues.append(
                    ValidationWarning(
                        model_object_reference=OrderedDict(),
                        message=f"Hit timeout before completing all tasks. Completed {index}/{len(tasks)} tasks.",
                    )
                )
                break
            try:
                self._sql_client.dry_run(stmt=task.query_string, sql_bind_parameters=task.query_params)
            except Exception as e:
                issues.append(
                    ValidationError(
                        model_object_reference=task.object_ref,
                        message=task.error_message + f"\nRecieved following error from data warehouse:\n{e}",
                    )
                )
                if task.on_fail_subtasks:
                    sub_task_timeout = floor(timeout - (perf_counter() - start_time)) if timeout else None
                    issues += self.run_tasks(tasks=task.on_fail_subtasks, timeout=sub_task_timeout)

        return issues

    def validate_data_sources(self, model: UserConfiguredModel, timeout: Optional[int] = None) -> List[ValidationIssue]:
        """Generates a list of tasks for validating the data sources of the model and then runs them

        Args:
            model: Model which to run data warehouse validations on
            timeout: An optional timeout. Default is None. When the timeout is hit, function will return early.

        Returns:
            A list of validation issues discovered when running the passed in tasks against the data warehosue
        """
        tasks = DataWarehouseTaskBuilder.gen_data_source_tasks(model=model)
        return self.run_tasks(tasks=tasks, timeout=timeout)

    def validate_dimensions(self, model: UserConfiguredModel, timeout: Optional[int] = None) -> List[ValidationIssue]:
        """Generates a list of tasks for validating the dimensions of the model and then runs them

        Args:
            model: Model which to run data warehouse validations on
            timeout: An optional timeout. Default is None. When the timeout is hit, function will return early.

        Returns:
            A list of validation issues. If there are no validation issues, an empty list is returned.
        """
        tasks = DataWarehouseTaskBuilder.gen_dimension_tasks(model=model)
        return self.run_tasks(tasks=tasks, timeout=timeout)

    def validate_metrics(self, model: UserConfiguredModel, timeout: Optional[int] = None) -> List[ValidationIssue]:
        """Generates a list of tasks for validating the metrics of the model and then runs them

        Args:
            model: Model which to run data warehouse validations on
            timeout: An optional timeout. Default is None. When the timeout is hit, function will return early.

        Returns:
            A list of validation issues. If there are no validation issues, an empty list is returned.
        """

        tasks = DataWarehouseTaskBuilder.gen_metric_tasks(model=model, mf_engine=self._mf_engine)
        return self.run_tasks(tasks=tasks, timeout=timeout)
