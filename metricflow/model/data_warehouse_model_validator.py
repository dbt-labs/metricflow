from collections import OrderedDict
from dataclasses import dataclass, field
from typing import List, OrderedDict as ODType

from metricflow.model.validations.validator_helpers import ValidationError, ValidationIssue
from metricflow.protocols.sql_client import SqlClient

@dataclass
class DataWarehouseValidationTask:
    """Dataclass for defining a task to be used in the DataWarehouseModelValidator"""

    query_string: str
    error_message: str
    object_ref: ODType = field(default_factory=lambda: OrderedDict())


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
