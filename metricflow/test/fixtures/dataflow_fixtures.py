from __future__ import annotations

import pytest

from metricflow.dataflow.builder.costing import DefaultCostFunction
from metricflow.dataflow.builder.dataflow_plan_builder import DataflowPlanBuilder
from metricflow.model.semantic_manifest_lookup import SemanticManifestLookup
from metricflow.plan_conversion.column_resolver import DunderColumnAssociationResolver
from metricflow.plan_conversion.time_spine import TimeSpineSource
from metricflow.protocols.sql_client import SqlClient
from metricflow.specs.column_assoc import ColumnAssociationResolver
from metricflow.test.fixtures.model_fixtures import ConsistentIdObjectRepository
from metricflow.test.fixtures.setup_fixtures import MetricFlowTestSessionState
from metricflow.test.fixtures.sql_client_fixtures import sql_client  # noqa: F401, F403

"""
Using 'function' scope to make ID generation more deterministic for the dataflow plan builder fixtures..

Using 'session' scope can result in other 'session' scope fixtures causing ID consistency issues.
"""


@pytest.fixture
def column_association_resolver(  # noqa: D
    simple_semantic_manifest_lookup: SemanticManifestLookup,
) -> ColumnAssociationResolver:
    return DunderColumnAssociationResolver(simple_semantic_manifest_lookup)


@pytest.fixture
def dataflow_plan_builder(  # noqa: D
    simple_semantic_manifest_lookup: SemanticManifestLookup,
    consistent_id_object_repository: ConsistentIdObjectRepository,
) -> DataflowPlanBuilder:
    return DataflowPlanBuilder(
        source_nodes=consistent_id_object_repository.simple_model_source_nodes,
        semantic_manifest_lookup=simple_semantic_manifest_lookup,
        cost_function=DefaultCostFunction(),
    )


@pytest.fixture
def multihop_dataflow_plan_builder(  # noqa: D
    multi_hop_join_semantic_manifest_lookup: SemanticManifestLookup,
    consistent_id_object_repository: ConsistentIdObjectRepository,
    time_spine_source: TimeSpineSource,
) -> DataflowPlanBuilder:
    return DataflowPlanBuilder(
        source_nodes=consistent_id_object_repository.multihop_model_source_nodes,
        semantic_manifest_lookup=multi_hop_join_semantic_manifest_lookup,
        cost_function=DefaultCostFunction(),
    )


@pytest.fixture
def scd_column_association_resolver(  # noqa: D
    scd_semantic_manifest_lookup: SemanticManifestLookup,
) -> ColumnAssociationResolver:
    return DunderColumnAssociationResolver(scd_semantic_manifest_lookup)


@pytest.fixture
def scd_dataflow_plan_builder(  # noqa: D
    scd_semantic_manifest_lookup: SemanticManifestLookup,
    scd_column_association_resolver: ColumnAssociationResolver,
    consistent_id_object_repository: ConsistentIdObjectRepository,
    time_spine_source: TimeSpineSource,
) -> DataflowPlanBuilder:
    return DataflowPlanBuilder(
        source_nodes=consistent_id_object_repository.scd_model_source_nodes,
        semantic_manifest_lookup=scd_semantic_manifest_lookup,
        cost_function=DefaultCostFunction(),
        column_association_resolver=scd_column_association_resolver,
    )


@pytest.fixture(scope="session")
def time_spine_source(  # noqa: D
    sql_client: SqlClient, mf_test_session_state: MetricFlowTestSessionState  # noqa: F811
) -> TimeSpineSource:
    return TimeSpineSource(schema_name=mf_test_session_state.mf_source_schema, table_name="mf_time_spine")
