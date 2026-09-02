from __future__ import annotations

from typing import Mapping

import pytest
from metricflow_semantics.errors.error_classes import InvalidQueryException

from metricflow.engine.metricflow_engine import MetricFlowEngine, MetricFlowQueryRequest
from tests_metricflow.fixtures.manifest_fixtures import MetricFlowEngineTestFixture, SemanticManifestSetup


def test_mfsql_request_matches_the_equivalent_flag_based_request(
    mf_engine_test_fixture_mapping: Mapping[SemanticManifestSetup, MetricFlowEngineTestFixture]
) -> None:
    """A `sql=`-based request should resolve to the exact same query spec as the equivalent flag-based request."""
    mf_engine: MetricFlowEngine = mf_engine_test_fixture_mapping[
        SemanticManifestSetup.SIMPLE_MANIFEST
    ].metricflow_engine

    mfsql_result = mf_engine.explain(
        MetricFlowQueryRequest.create(sql="SELECT bookings, metric_time FROM metrics ORDER BY metric_time LIMIT 5")
    )
    flag_based_result = mf_engine.explain(
        MetricFlowQueryRequest.create(
            metric_names=["bookings"],
            group_by_names=["metric_time"],
            order_by_names=["metric_time"],
            limit=5,
        )
    )

    assert mfsql_result.query_spec == flag_based_result.query_spec


def test_mfsql_request_rejects_combination_with_other_query_params(
    mf_engine_test_fixture_mapping: Mapping[SemanticManifestSetup, MetricFlowEngineTestFixture]
) -> None:
    """`sql` must fully describe the query - combining it with other query-shaping params is rejected."""
    mf_engine: MetricFlowEngine = mf_engine_test_fixture_mapping[
        SemanticManifestSetup.SIMPLE_MANIFEST
    ].metricflow_engine

    with pytest.raises(InvalidQueryException, match="can't be combined"):
        mf_engine.explain(MetricFlowQueryRequest.create(sql="SELECT bookings FROM metrics", metric_names=["bookings"]))
