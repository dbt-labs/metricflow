from __future__ import annotations

import pytest

from metricflow.api.metricflow_client import MetricFlowClient
from metricflow.model.semantic_manifest_lookup import SemanticManifestLookup
from metricflow.protocols.sql_client import SqlClient


@pytest.fixture
def mf_client(
    create_source_tables: bool,
    sql_client: SqlClient,
    simple_semantic_manifest_lookup: SemanticManifestLookup,
) -> MetricFlowClient:
    """Fixture for MetricFlowClient."""
    return MetricFlowClient(
        sql_client=sql_client,
        semantic_manifest=simple_semantic_manifest_lookup.semantic_manifest,
    )
