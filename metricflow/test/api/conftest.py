from __future__ import annotations

import pytest

from metricflow.api.metricflow_client import MetricFlowClient
from metricflow.model.semantic_manifest_lookup import SemanticManifestLookup
from metricflow.plan_conversion.time_spine import TimeSpineSource
from metricflow.protocols.sql_client import SqlClient
from metricflow.test.fixtures.setup_fixtures import MetricFlowTestSessionState


@pytest.fixture
def mf_client(
    create_source_tables: bool,
    sql_client: SqlClient,
    simple_semantic_manifest_lookup: SemanticManifestLookup,
    time_spine_source: TimeSpineSource,
    mf_test_session_state: MetricFlowTestSessionState,
) -> MetricFlowClient:
    """Fixture for MetricFlowClient."""
    return MetricFlowClient(
        sql_client=sql_client,
        semantic_manifest=simple_semantic_manifest_lookup.semantic_manifest,
        system_schema=mf_test_session_state.mf_system_schema,
    )
