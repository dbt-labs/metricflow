from __future__ import annotations

import logging

import pytest
from _pytest.fixtures import FixtureRequest
from dbt_semantic_interfaces.protocols import SemanticManifest
from dbt_semantic_interfaces.references import MetricReference
from metricflow_semantics.experimental.test_helpers.performance_helpers import BenchmarkFunction, PerformanceBenchmark
from metricflow_semantics.test_helpers.config_helpers import MetricFlowTestConfiguration
from typing_extensions import override

from tests_metricflow_semantics.experimental.semantic_graph.sg_fixtures import SemanticGraphTestFixture

logger = logging.getLogger(__name__)


@pytest.fixture()
def high_complexity_manifest_sg_fixture(  # noqa: D103
    request: FixtureRequest,
    mf_test_configuration: MetricFlowTestConfiguration,
    manifest_with_200_models_100_metrics: SemanticManifest,
) -> SemanticGraphTestFixture:
    return SemanticGraphTestFixture(
        request=request,
        snapshot_configuration=mf_test_configuration,
        semantic_manifest=manifest_with_200_models_100_metrics,
    )


@pytest.mark.skip("The legacy resolver takes ~240 seconds to initialize.")
def test_resolver_init_time(high_complexity_manifest_sg_fixture: SemanticGraphTestFixture) -> None:
    """This test demonstrates the initialization time of the semantic-graph-based resolver w/ a 500x improvement.

    This test uses the high-complexity manifest. On my laptop, the legacy resolver took ~240s to initialize while the
    semantic-graph-based resolver took ~0.4s.
    """

    class _LeftFunction(BenchmarkFunction):
        @override
        def run(self) -> None:
            high_complexity_manifest_sg_fixture.create_legacy_resolver()

    class _RightFunction(BenchmarkFunction):
        @override
        def run(self) -> None:
            high_complexity_manifest_sg_fixture.create_sg_resolver()

    PerformanceBenchmark.assert_function_performance(
        left_function_class=_LeftFunction,
        right_function_class=_RightFunction,
        min_performance_factor=500,
    )


# @pytest.mark.skip("The legacy resolver takes ~240 seconds to initialize.")
def test_resolver_query_time(high_complexity_manifest_sg_fixture: SemanticGraphTestFixture) -> None:
    """This test demonstrates the (cold) resolution time of the semantic-graph-based resolver w/ a 18x improvement.

    This test focuses on the resolution time for a cold query and excludes the initialization time. Using the
    high-complexity manifest, the legacy resolver took ~15s to resolve the group-by items for a 20 metric query
    while the semantic-graph-based resolver took 0.7s.
    """
    metric_references = tuple(MetricReference(f"metric_1_{i:03}") for i in range(20))
    legacy_resolver = high_complexity_manifest_sg_fixture.create_legacy_resolver()
    sg_resolver = high_complexity_manifest_sg_fixture.create_sg_resolver()

    class _LeftFunction(BenchmarkFunction):
        @override
        def run(self) -> None:
            legacy_resolver.get_linkable_elements_for_metrics(metric_references)

    class _RightFunction(BenchmarkFunction):
        @override
        def run(self) -> None:
            sg_resolver.get_linkable_elements_for_metrics(metric_references)

    PerformanceBenchmark.assert_function_performance(
        left_function_class=_LeftFunction,
        right_function_class=_RightFunction,
        min_performance_factor=18,
    )
