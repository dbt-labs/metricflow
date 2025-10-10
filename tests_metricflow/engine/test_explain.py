from __future__ import annotations

import logging
from concurrent.futures import Future, ThreadPoolExecutor
from typing import Mapping, Sequence

import pytest
from _pytest.fixtures import FixtureRequest
from metricflow_semantics.test_helpers.config_helpers import MetricFlowTestConfiguration
from metricflow_semantics.toolkit.mf_logging.pretty_print import PrettyFormatDictOption, mf_pformat_dict

from metricflow.engine.metricflow_engine import MetricFlowEngine, MetricFlowExplainResult, MetricFlowQueryRequest
from metricflow.sql.optimizer.optimization_levels import SqlOptimizationLevel
from tests_metricflow.fixtures.manifest_fixtures import MetricFlowEngineTestFixture, SemanticManifestSetup
from tests_metricflow.snapshot_utils import assert_str_snapshot_equal

logger = logging.getLogger(__name__)


def _explain_one_query(mf_engine: MetricFlowEngine) -> str:
    explain_result: MetricFlowExplainResult = mf_engine.explain(
        MetricFlowQueryRequest.create_with_random_request_id(saved_query_name="p0_booking")
    )
    return explain_result.sql_statement.sql


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


@pytest.mark.sql_engine_snapshot
@pytest.mark.duckdb_only
def test_optimization_level(
    request: FixtureRequest,
    mf_test_configuration: MetricFlowTestConfiguration,
    mf_engine_test_fixture_mapping: Mapping[SemanticManifestSetup, MetricFlowEngineTestFixture],
) -> None:
    """Tests that the results of explain reflect the SQL optimization level in the request."""
    mf_engine = mf_engine_test_fixture_mapping[SemanticManifestSetup.SIMPLE_MANIFEST].metricflow_engine

    results = {}
    for optimization_level in SqlOptimizationLevel:
        # Skip lower optimization levels as they are generally not used.
        if optimization_level <= SqlOptimizationLevel.O3:
            continue

        explain_result: MetricFlowExplainResult = mf_engine.explain(
            MetricFlowQueryRequest.create_with_random_request_id(
                metric_names=("bookings", "views"),
                group_by_names=("metric_time", "listing__country_latest"),
                sql_optimization_level=optimization_level,
            )
        )
        results[optimization_level.value] = explain_result.sql_statement.without_descriptions.sql

    assert_str_snapshot_equal(
        request=request,
        mf_test_configuration=mf_test_configuration,
        snapshot_id="result",
        snapshot_str=mf_pformat_dict(
            description=None,
            obj_dict=results,
            format_option=PrettyFormatDictOption(preserve_raw_strings=True, pad_items_with_newlines=True),
        ),
        expectation_description=f"The result for {SqlOptimizationLevel.O5} should be SQL uses a CTE.",
    )
