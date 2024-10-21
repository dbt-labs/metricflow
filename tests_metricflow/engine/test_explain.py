from __future__ import annotations

from concurrent.futures import Future, ThreadPoolExecutor
from typing import Mapping, Sequence

from metricflow.engine.metricflow_engine import MetricFlowEngine, MetricFlowExplainResult, MetricFlowQueryRequest
from tests_metricflow.fixtures.manifest_fixtures import MetricFlowEngineTestFixture, SemanticManifestSetup


def _explain_one_query(mf_engine: MetricFlowEngine) -> str:
    explain_result: MetricFlowExplainResult = mf_engine.explain(
        MetricFlowQueryRequest.create_with_random_request_id(saved_query_name="p0_booking")
    )
    return explain_result.rendered_sql.sql_query


def test_concurrent_explain_consistency(
    mf_engine_test_fixture_mapping: Mapping[SemanticManifestSetup, MetricFlowEngineTestFixture]
) -> None:
    """Tests that concurrent requests for the same query generate the same SQL.

    Prior to consistency fixes for ID generation, this test would fail due to issues with sequentially numbered aliases.
    """
    mf_engine = mf_engine_test_fixture_mapping[SemanticManifestSetup.SIMPLE_MANIFEST].metricflow_engine

    request_count = 4
    with ThreadPoolExecutor(max_workers=2) as executor:
        futures: Sequence[Future] = [executor.submit(_explain_one_query, mf_engine) for _ in range(request_count)]
        results = [future.result() for future in futures]
        for result in results:
            assert result == results[0], "Expected only one unique result / results to be the same"
