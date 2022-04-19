import pytest

from metricflow.api.metricflow_client import MetricFlowClient
from metricflow.model.semantic_model import SemanticModel
from metricflow.plan_conversion.time_spine import TimeSpineSource
from metricflow.protocols.sql_client import SqlClient
from metricflow.test.fixtures.setup_fixtures import MetricFlowTestSessionState


@pytest.fixture
def mf_client(
    create_simple_model_tables: bool,
    sql_client: SqlClient,
    simple_semantic_model: SemanticModel,
    time_spine_source: TimeSpineSource,
    mf_test_session_state: MetricFlowTestSessionState,
) -> MetricFlowClient:
    """Fixture for MetricFlowClient."""
    return MetricFlowClient(
        sql_client=sql_client,
        user_configured_model=simple_semantic_model.user_configured_model,
        system_schema=mf_test_session_state.mf_system_schema,
    )
