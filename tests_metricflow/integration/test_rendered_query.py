from __future__ import annotations

import pytest
from _pytest.fixtures import FixtureRequest
from metricflow_semantics.model.semantic_manifest_lookup import SemanticManifestLookup
from metricflow_semantics.specs.dunder_column_association_resolver import DunderColumnAssociationResolver
from metricflow_semantics.test_helpers.config_helpers import MetricFlowTestConfiguration
from metricflow_semantics.test_helpers.time_helpers import ConfigurableTimeSource

from metricflow.engine.metricflow_engine import MetricFlowEngine, MetricFlowQueryRequest
from metricflow.protocols.sql_client import SqlClient
from metricflow_semantic_interfaces.test_utils import as_datetime
from tests_metricflow.integration.conftest import IntegrationTestHelpers
from tests_metricflow.snapshot_utils import (
    assert_sql_snapshot_equal,
)


@pytest.mark.sql_engine_snapshot
def test_render_query(  # noqa: D103
    request: FixtureRequest, mf_test_configuration: MetricFlowTestConfiguration, it_helpers: IntegrationTestHelpers
) -> None:
    result = it_helpers.mf_engine.explain(
        MetricFlowQueryRequest.create(
            metric_names=["bookings"],
            group_by_names=["metric_time"],
        )
    )

    assert_sql_snapshot_equal(
        request=request,
        mf_test_configuration=mf_test_configuration,
        snapshot_id="query0",
        sql=result.sql_statement.sql,
        sql_engine=it_helpers.sql_client.sql_engine_type,
    )


@pytest.mark.sql_engine_snapshot
def test_id_enumeration(  # noqa: D103
    request: FixtureRequest,
    mf_test_configuration: MetricFlowTestConfiguration,
    simple_semantic_manifest_lookup: SemanticManifestLookup,
    sql_client: SqlClient,
) -> None:
    mf_engine = MetricFlowEngine(
        semantic_manifest_lookup=simple_semantic_manifest_lookup,
        sql_client=sql_client,
        column_association_resolver=DunderColumnAssociationResolver(),
        time_source=ConfigurableTimeSource(as_datetime("2020-01-01")),
        consistent_id_enumeration=True,
    )

    result = mf_engine.explain(
        MetricFlowQueryRequest.create(
            metric_names=["bookings", "listings"],
            group_by_names=["metric_time"],
        )
    )

    assert_sql_snapshot_equal(
        request=request,
        mf_test_configuration=mf_test_configuration,
        snapshot_id="query",
        sql=result.sql_statement.sql,
        sql_engine=sql_client.sql_engine_type,
    )

    # The resulting snapshot should be the same since mf_engine was created with consistent_id_enumeration=True
    result = mf_engine.explain(
        MetricFlowQueryRequest.create(
            metric_names=["bookings", "listings"],
            group_by_names=["metric_time"],
        )
    )

    assert_sql_snapshot_equal(
        request=request,
        mf_test_configuration=mf_test_configuration,
        snapshot_id="query",
        sql=result.sql_statement.sql,
        sql_engine=sql_client.sql_engine_type,
    )


def test_id_enumeration_with_multi_hop_join(
    multi_hop_join_semantic_manifest_lookup: SemanticManifestLookup,
    sql_client: SqlClient,
) -> None:
    """Check that the rendered SQL does not change when the dataflow plan builder cache is used.

    Finding the join path for a multi-hop dimension resolves the output data set of candidate nodes. This is skipped
    when the result is cached, so the generated sub-query aliases should not depend on it.
    """
    mf_engine = MetricFlowEngine(
        semantic_manifest_lookup=multi_hop_join_semantic_manifest_lookup,
        sql_client=sql_client,
        column_association_resolver=DunderColumnAssociationResolver(),
        time_source=ConfigurableTimeSource(as_datetime("2020-01-01")),
        consistent_id_enumeration=True,
    )
    query_request = MetricFlowQueryRequest.create(
        metric_names=["txn_count"],
        group_by_names=["account_id__customer_id__customer_name"],
    )

    first_sql = mf_engine.explain(query_request).sql_statement.sql
    second_sql = mf_engine.explain(query_request).sql_statement.sql

    assert first_sql == second_sql
