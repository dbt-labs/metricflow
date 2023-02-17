import pytest

from metricflow.dataset.entity_adapter import EntityDataSet
from metricflow.model.semantic_model import SemanticModel
from metricflow.plan_conversion.column_resolver import DefaultColumnAssociationResolver
from metricflow.plan_conversion.dataflow_to_sql import DataflowToSqlQueryPlanConverter
from metricflow.plan_conversion.time_spine import TimeSpineSource


@pytest.fixture(scope="session")
def dataflow_to_sql_converter(  # noqa: D
    simple_semantic_model: SemanticModel,
    time_spine_source: TimeSpineSource,
) -> DataflowToSqlQueryPlanConverter[EntityDataSet]:
    return DataflowToSqlQueryPlanConverter[EntityDataSet](
        column_association_resolver=DefaultColumnAssociationResolver(simple_semantic_model),
        semantic_model=simple_semantic_model,
        time_spine_source=time_spine_source,
    )
