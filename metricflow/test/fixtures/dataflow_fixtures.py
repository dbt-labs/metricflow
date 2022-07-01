from __future__ import annotations

import pytest

from metricflow.dataflow.builder.costing import DefaultCostFunction
from metricflow.dataflow.builder.dataflow_plan_builder import DataflowPlanBuilder
from metricflow.dataset.data_source_adapter import DataSourceDataSet
from metricflow.model.semantic_model import SemanticModel
from metricflow.plan_conversion.time_spine import TimeSpineSource, TimeSpineTableBuilder
from metricflow.protocols.sql_client import SqlClient
from metricflow.test.fixtures.model_fixtures import ConsistentIdObjectRepository
from metricflow.test.fixtures.setup_fixtures import MetricFlowTestSessionState
from metricflow.test.fixtures.sql_client_fixtures import sql_client  # noqa: F401, F403

"""
Using 'function' scope to make ID generation more deterministic for the dataflow plan builder fixtures..

Using 'session' scope can result in other 'session' scope fixtures causing ID consistency issues.
"""


@pytest.fixture
def composite_dataflow_plan_builder(  # noqa: D
    composite_identifier_semantic_model: SemanticModel,
    consistent_id_object_repository: ConsistentIdObjectRepository,
    time_spine_source: TimeSpineSource,
) -> DataflowPlanBuilder[DataSourceDataSet]:

    return DataflowPlanBuilder(
        source_nodes=consistent_id_object_repository.composite_model_source_nodes,
        semantic_model=composite_identifier_semantic_model,
        cost_function=DefaultCostFunction[DataSourceDataSet](),
        time_spine_source=time_spine_source,
    )


@pytest.fixture
def dataflow_plan_builder(  # noqa: D
    simple_semantic_model: SemanticModel,
    consistent_id_object_repository: ConsistentIdObjectRepository,
    time_spine_source: TimeSpineSource,
) -> DataflowPlanBuilder[DataSourceDataSet]:

    return DataflowPlanBuilder(
        source_nodes=consistent_id_object_repository.simple_model_source_nodes,
        semantic_model=simple_semantic_model,
        cost_function=DefaultCostFunction[DataSourceDataSet](),
        time_spine_source=time_spine_source,
    )


@pytest.fixture
def multihop_dataflow_plan_builder(  # noqa: D
    multi_hop_join_semantic_model: SemanticModel,
    consistent_id_object_repository: ConsistentIdObjectRepository,
    time_spine_source: TimeSpineSource,
) -> DataflowPlanBuilder[DataSourceDataSet]:

    return DataflowPlanBuilder(
        source_nodes=consistent_id_object_repository.multihop_model_source_nodes,
        semantic_model=multi_hop_join_semantic_model,
        cost_function=DefaultCostFunction[DataSourceDataSet](),
        time_spine_source=time_spine_source,
    )


@pytest.fixture(scope="session")
def time_spine_source(  # noqa: D
    sql_client: SqlClient, mf_test_session_state: MetricFlowTestSessionState  # noqa: F811
) -> TimeSpineSource:
    time_spine_source = TimeSpineSource(schema_name=mf_test_session_state.mf_system_schema)
    time_spine_table_builder = TimeSpineTableBuilder(time_spine_source=time_spine_source, sql_client=sql_client)
    time_spine_table_builder.create_if_necessary()
    return time_spine_source
