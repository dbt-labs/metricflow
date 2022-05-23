from collections import OrderedDict
from dataclasses import dataclass, field
from typing import List, OrderedDict as ODType

from metricflow.model.objects.data_source import DataSource
from metricflow.model.objects.user_configured_model import UserConfiguredModel
from metricflow.model.validations.validator_helpers import ValidationError, ValidationIssue
from metricflow.protocols.sql_client import SqlClient

@dataclass
class DataWarehouseValidationTask:
    """Dataclass for defining a task to be used in the DataWarehouseModelValidator"""

    query_string: str
    error_message: str
    object_ref: ODType = field(default_factory=lambda: OrderedDict())


class DataWarehouseTaskBuilder:
    """Task builder for standard data warehouse validation tasks"""

    QUERY_TEMPLATE = "SELECT {columns_select} FROM {data_source}"
    PARTITION_TEMPLATE = " WHERE {partition_col} IS NOT NULL"
    WRAPPED_COL_TEMPLATE = "({column}) AS col{unique_id}"

    @staticmethod
    def _wrap_col(col: str, id: int) -> str:  # noqa: D
        return DataWarehouseTaskBuilder.WRAPPED_COL_TEMPLATE.format(column=col, unique_id=id)

    @staticmethod
    def _gen_query(data_source: DataSource, columns: List[str] = ["true"]) -> str:  # noqa: D
        data_source_str = data_source.sql_table if data_source.sql_table else f"({data_source.sql_query})"

        wrapped_cols = [DataWarehouseTaskBuilder._wrap_col(col, index) for index, col in enumerate(columns)]
        columns_select = ", ".join(wrapped_cols)

        query = DataWarehouseTaskBuilder.QUERY_TEMPLATE.format(
            data_source=data_source_str, columns_select=columns_select
        )

        # If the data source has a partition, we need to include it in the
        # query. This is because some data warehouses require the partition as
        # part of the query for any table that has a partition defined
        if data_source.partition:
            partition_col = (
                f"({data_source.partition.expr})"
                if data_source.partition.expr
                else data_source.partition.name.element_name
            )
            query += DataWarehouseTaskBuilder.PARTITION_TEMPLATE.format(partition_col=partition_col)

        return query

    @staticmethod
    def gen_data_source_tasks(model: UserConfiguredModel) -> List[DataWarehouseValidationTask]:
        """Generates a list of tasks for validating the data sources of the model"""
        tasks: List[DataWarehouseValidationTask] = []
        for data_source in model.data_sources:
            tasks.append(
                DataWarehouseValidationTask(
                    query_string=DataWarehouseTaskBuilder._gen_query(data_source=data_source),
                    object_ref=ValidationIssue.make_object_reference(data_source_name=data_source.name),
                    error_message=f"Unable to access data source `{data_source.name}` in data warehouse",
                )
            )
        return tasks


class DataWarehouseModelValidator:
    """A Validator for checking specific tasks for the model against the Data Warehouse"""

    def __init__(self, sql_client: SqlClient) -> None:  # noqa: D
        self._sql_client = sql_client

    def run_tasks(self, tasks: List[DataWarehouseValidationTask]) -> List[ValidationIssue]:
        """Runs the list of tasks as queries agains the data warehouse, returning any found issues"""
        issues: List[ValidationIssue] = []
        # TODO: Asyncio implementation
        for task in tasks:
            try:
                self._sql_client.dry_run(stmt=task.query_string)
            except Exception as e:
                issues.append(
                    ValidationError(
                        model_object_reference=task.object_ref,
                        message=task.error_message + f"\nRecieved following error from data warehouse:\n{e}",
                    )
                )
        return issues

    def validate_data_sources(self, model: UserConfiguredModel) -> List[ValidationIssue]:
        """Generates a list of tasks for validating the data sources of the model and then runs them"""
        tasks = DataWarehouseTaskBuilder.gen_data_source_tasks(model=model)
        return self.run_tasks(tasks=tasks)
