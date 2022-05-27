from metricflow.model.data_warehouse_model_validator import (
    DataWarehouseModelValidator,
    DataWarehouseTaskBuilder,
    DataWarehouseValidationTask,
)

from metricflow.model.objects.data_source import DataSource, Mutability, MutabilityType
from metricflow.model.objects.elements.dimension import Dimension, DimensionType
from metricflow.model.objects.user_configured_model import UserConfiguredModel
from metricflow.model.validations.validator_helpers import ValidationIssueLevel
from metricflow.protocols.sql_client import SqlClient
from metricflow.specs import DimensionReference


def test_build_data_source_tasks() -> None:  # noqa:D
    dim_ref_1 = DimensionReference(element_name="dim1")
    dim_ref_2 = DimensionReference(element_name="dim1")

    model = UserConfiguredModel(
        data_sources=[
            DataSource(
                name="test_data_source",
                sql_table="test.data_source",
                dimensions=[
                    Dimension(name=dim_ref_1, type=DimensionType.CATEGORICAL, is_partition=True),
                    Dimension(
                        name=dim_ref_2,
                        type=DimensionType.CATEGORICAL,
                        is_partition=True,
                        expr="animal in ('cat', 'dog', 'platypus')",
                    ),
                ],
                mutability=Mutability(type=MutabilityType.IMMUTABLE),
            ),
        ],
        metrics=[],
        materializations=[],
    )

    tasks = DataWarehouseTaskBuilder.gen_data_source_tasks(model=model)
    assert len(tasks) == 1
    assert (
        tasks[0].query_string
        == "SELECT (true) AS col0 FROM test.data_source WHERE dim1 IS NOT NULL AND (animal in ('cat', 'dog', 'platypus')) IS NOT NULL"
    )


def test_task_runner(sql_client: SqlClient) -> None:  # noqa: D
    dw_validator = DataWarehouseModelValidator(sql_client=sql_client)

    tasks = [
        DataWarehouseValidationTask(query_string="SELECT 'foo' AS foo", error_message="Could not select foo"),
        DataWarehouseValidationTask(query_string="SELECT 'bar' as bar", error_message="Could not select bar"),
    ]

    issues = dw_validator.run_tasks(tasks=tasks)
    assert len(issues) == 0

    err_msg_bad = "Could not access table 'doesnt_exist' in data warehouse"
    bad_task = DataWarehouseValidationTask(
        query_string="SELECT (true) AS col1 FROM doesnt_exist", error_message=err_msg_bad
    )

    tasks.append(bad_task)
    issues = dw_validator.run_tasks(tasks=tasks)
    assert len(issues) == 1
    assert issues[0].level == ValidationIssueLevel.ERROR
    assert err_msg_bad in issues[0].message
