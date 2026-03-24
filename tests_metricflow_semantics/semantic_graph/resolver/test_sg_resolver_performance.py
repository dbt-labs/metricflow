from __future__ import annotations

import logging

import pytest
from _pytest.fixtures import FixtureRequest
from dbt_semantic_interfaces.protocols import SemanticManifest
from dbt_semantic_interfaces.references import MetricReference
from metricflow_semantics.model.linkable_element_property import GroupByItemProperty
from metricflow_semantics.model.semantics.element_filter import GroupByItemSetFilter
from metricflow_semantics.test_helpers.config_helpers import MetricFlowTestConfiguration
from metricflow_semantics.test_helpers.performance.benchmark_helpers import (
    BenchmarkFunction,
    OneSecondFunction,
    PerformanceBenchmark,
)
from typing_extensions import override

from tests_metricflow_semantics.semantic_graph.sg_fixtures import SemanticGraphTestFixture

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


def test_resolver_init_time(high_complexity_manifest_sg_fixture: SemanticGraphTestFixture) -> None:
    """Check the performance of initializing the semantic-graph-based resolver."""

    class _RightFunction(BenchmarkFunction):
        @override
        def run(self) -> None:
            high_complexity_manifest_sg_fixture.create_sg_resolver()

    PerformanceBenchmark.assert_function_performance(
        left_function_class=OneSecondFunction,
        right_function_class=_RightFunction,
        min_performance_factor=1.5,
    )


def test_resolver_query_time(high_complexity_manifest_sg_fixture: SemanticGraphTestFixture) -> None:
    """Check the performance of querying the semantic-graph-based resolver."""
    metric_references = tuple(MetricReference(f"metric_1_{i:03}") for i in range(10))

    class _RightFunction(BenchmarkFunction):
        def __init__(self) -> None:
            self._resolver = high_complexity_manifest_sg_fixture.create_sg_resolver()

        @override
        def run(self) -> None:
            # Replicate the behavior of get_linkable_elements_for_metrics which filters out METRIC properties
            base_filter = GroupByItemSetFilter.create(any_properties_denylist=(GroupByItemProperty.METRIC,))
            self._resolver.get_common_set(metric_references=metric_references, set_filter=base_filter)

    PerformanceBenchmark.assert_function_performance(
        left_function_class=OneSecondFunction,
        right_function_class=_RightFunction,
        min_performance_factor=1,
    )
