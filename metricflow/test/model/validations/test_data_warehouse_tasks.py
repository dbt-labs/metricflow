from metricflow.model.data_warehouse_model_validator import DataWarehouseTaskBuilder

from metricflow.model.objects.data_source import DataSource, Mutability, MutabilityType
from metricflow.model.objects.elements.dimension import Dimension, DimensionType
from metricflow.model.objects.user_configured_model import UserConfiguredModel
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
