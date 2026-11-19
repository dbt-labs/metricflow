from __future__ import annotations

import logging
from concurrent.futures import Future, ThreadPoolExecutor
from datetime import datetime, timedelta
from functools import partial
from typing import Mapping, Sequence

import pytest
from _pytest.fixtures import FixtureRequest
from metricflow_semantics.model.semantic_manifest_lookup import SemanticManifestLookup
from metricflow_semantics.test_helpers.config_helpers import MetricFlowTestConfiguration
from metricflow_semantics.toolkit.mf_logging.pretty_print import PrettyFormatDictOption, mf_pformat_dict

from metricflow.engine import metricflow_engine
from metricflow.engine.metricflow_engine import MetricFlowEngine, MetricFlowExplainResult, MetricFlowQueryRequest
from metricflow.plan_conversion.to_sql_plan.dataflow_to_subquery import DataflowNodeToSqlSubqueryVisitor
from metricflow.protocols.sql_client import SqlClient
from metricflow.sql.optimizer.optimization_levels import SqlOptimizationLevel
from tests_metricflow.fixtures.manifest_fixtures import MetricFlowEngineTestFixture, SemanticManifestSetup
from tests_metricflow.snapshot_utils import assert_str_snapshot_equal

logger = logging.getLogger(__name__)


def _explain_one_query(mf_engine: MetricFlowEngine) -> str:
    explain_result: MetricFlowExplainResult = mf_engine.explain(
        MetricFlowQueryRequest.create(saved_query_name="p0_booking")
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


def _engine_with_small_output_cache(
    semantic_manifest_lookup: SemanticManifestLookup,
    sql_client: SqlClient,
    monkeypatch: pytest.MonkeyPatch,
    cache_limit: int,
) -> MetricFlowEngine:
    """Configure the visitor used by the engine's plan builder."""
    with monkeypatch.context() as patch:
        patch.setattr(
            metricflow_engine,
            "DataflowNodeToSqlSubqueryVisitor",
            partial(DataflowNodeToSqlSubqueryVisitor, max_cached_output_data_sets=cache_limit),
        )
        return MetricFlowEngine(semantic_manifest_lookup=semantic_manifest_lookup, sql_client=sql_client)


def _explain_bookings_for_day(engine: MetricFlowEngine, day_offset: int) -> str:
    day = datetime(2020, 1, 1) + timedelta(days=day_offset)
    result = engine.explain(
        MetricFlowQueryRequest.create(
            metric_names=("bookings",),
            time_constraint_start=day,
            time_constraint_end=day,
        )
    )
    return result.sql_statement.sql


def test_explain_output_data_set_cache_is_bounded(
    simple_semantic_manifest_lookup: SemanticManifestLookup,
    sql_client: SqlClient,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Distinct queries fill the LRU without evicting any source nodes."""
    cache_limit = 10
    engine = _engine_with_small_output_cache(simple_semantic_manifest_lookup, sql_client, monkeypatch, cache_limit)
    builder = engine._dataflow_plan_builder
    resolver = builder._node_data_set_resolver
    pinned = dict(resolver._pinned_output_data_sets)
    assert pinned.keys() == set(builder._source_node_set.all_nodes)

    for day_offset in range(3 * cache_limit):
        _explain_bookings_for_day(engine, day_offset)
        assert len(resolver._output_data_set_cache._cache_dict) <= cache_limit

    assert len(resolver._output_data_set_cache._cache_dict) == cache_limit
    assert resolver._pinned_output_data_sets == pinned


def test_explain_sql_with_output_data_set_eviction(
    simple_semantic_manifest_lookup: SemanticManifestLookup,
    sql_client: SqlClient,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A one-entry cache produces the same SQL as the default under concurrent eviction."""
    default_engine = MetricFlowEngine(semantic_manifest_lookup=simple_semantic_manifest_lookup, sql_client=sql_client)
    small_cache_engine = _engine_with_small_output_cache(
        simple_semantic_manifest_lookup, sql_client, monkeypatch, cache_limit=1
    )
    day_offsets = range(20)
    expected = [_explain_bookings_for_day(default_engine, day_offset) for day_offset in day_offsets]

    with ThreadPoolExecutor(max_workers=4) as executor:
        actual = list(executor.map(partial(_explain_bookings_for_day, small_cache_engine), day_offsets))

    assert actual == expected


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
            MetricFlowQueryRequest.create(
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
