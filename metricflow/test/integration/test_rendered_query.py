from __future__ import annotations

import pytest
from _pytest.fixtures import FixtureRequest
from dbt_semantic_interfaces.test_utils import as_datetime

from metricflow.dataflow.sql_table import SqlTable
from metricflow.engine.metricflow_engine import MetricFlowEngine, MetricFlowQueryRequest
from metricflow.model.semantic_manifest_lookup import SemanticManifestLookup
from metricflow.plan_conversion.column_resolver import DunderColumnAssociationResolver
from metricflow.protocols.sql_client import SqlClient
from metricflow.test.fixtures.setup_fixtures import MetricFlowTestSessionState
from metricflow.test.integration.conftest import IntegrationTestHelpers
from metricflow.test.snapshot_utils import (
    assert_sql_snapshot_equal,
)
from metricflow.test.time.configurable_time_source import ConfigurableTimeSource


@pytest.mark.sql_engine_snapshot
def test_render_query(  # noqa: D
    request: FixtureRequest, mf_test_session_state: MetricFlowTestSessionState, it_helpers: IntegrationTestHelpers
) -> None:
    result = it_helpers.mf_engine.explain(
        MetricFlowQueryRequest.create_with_random_request_id(
            metric_names=["bookings"],
            group_by_names=["metric_time"],
        )
    )

    assert_sql_snapshot_equal(
        request=request,
        mf_test_session_state=mf_test_session_state,
        snapshot_id="query0",
        sql=result.rendered_sql.sql_query,
        sql_engine=it_helpers.sql_client.sql_engine_type,
    )


@pytest.mark.sql_engine_snapshot
def test_render_write_to_table_query(  # noqa: D
    request: FixtureRequest, mf_test_session_state: MetricFlowTestSessionState, it_helpers: IntegrationTestHelpers
) -> None:
    output_table = SqlTable(schema_name=it_helpers.mf_system_schema, table_name="test_table")

    result = it_helpers.mf_engine.explain(
        MetricFlowQueryRequest.create_with_random_request_id(
            metric_names=["bookings"], group_by_names=["metric_time"], output_table=output_table.sql
        )
    )

    assert_sql_snapshot_equal(
        request=request,
        mf_test_session_state=mf_test_session_state,
        snapshot_id="query0",
        sql=result.rendered_sql.sql_query,
        sql_engine=it_helpers.sql_client.sql_engine_type,
    )


@pytest.mark.sql_engine_snapshot
def test_id_enumeration(  # noqa: D
    request: FixtureRequest,
    mf_test_session_state: MetricFlowTestSessionState,
    simple_semantic_manifest_lookup: SemanticManifestLookup,
    sql_client: SqlClient,
) -> None:
    mf_engine = MetricFlowEngine(
        semantic_manifest_lookup=simple_semantic_manifest_lookup,
        sql_client=sql_client,
        column_association_resolver=DunderColumnAssociationResolver(
            semantic_manifest_lookup=simple_semantic_manifest_lookup
        ),
        time_source=ConfigurableTimeSource(as_datetime("2020-01-01")),
        consistent_id_enumeration=True,
    )

    result = mf_engine.explain(
        MetricFlowQueryRequest.create_with_random_request_id(
            metric_names=["bookings", "listings"],
            group_by_names=["metric_time"],
        )
    )

    assert_sql_snapshot_equal(
        request=request,
        mf_test_session_state=mf_test_session_state,
        snapshot_id="query",
        sql=result.rendered_sql.sql_query,
        sql_engine=sql_client.sql_engine_type,
    )

    # The resulting snapshot should be the same since mf_engine was created with consistent_id_enumeration=True
    result = mf_engine.explain(
        MetricFlowQueryRequest.create_with_random_request_id(
            metric_names=["bookings", "listings"],
            group_by_names=["metric_time"],
        )
    )

    assert_sql_snapshot_equal(
        request=request,
        mf_test_session_state=mf_test_session_state,
        snapshot_id="query",
        sql=result.rendered_sql.sql_query,
        sql_engine=sql_client.sql_engine_type,
    )
