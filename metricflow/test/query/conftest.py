import pytest

from metricflow.dataflow.builder.node_data_set import DataflowPlanNodeOutputDataSetResolver
from metricflow.model.semantic_model import SemanticModel
from metricflow.plan_conversion.column_resolver import DefaultColumnAssociationResolver
from metricflow.plan_conversion.time_spine import TimeSpineSource
from metricflow.query.query_parser import MetricFlowQueryParser
from metricflow.test.fixtures.model_fixtures import ConsistentIdObjectRepository


@pytest.fixture(scope="session")
def query_parser(  # noqa: D
    simple_semantic_model: SemanticModel,
    consistent_id_object_repository: ConsistentIdObjectRepository,
    time_spine_source: TimeSpineSource,
) -> MetricFlowQueryParser:
    return MetricFlowQueryParser(
        model=simple_semantic_model,
        source_nodes=consistent_id_object_repository.simple_model_source_nodes,
        node_output_resolver=DataflowPlanNodeOutputDataSetResolver(
            column_association_resolver=DefaultColumnAssociationResolver(simple_semantic_model),
            semantic_model=simple_semantic_model,
            time_spine_source=time_spine_source,
        ),
    )
