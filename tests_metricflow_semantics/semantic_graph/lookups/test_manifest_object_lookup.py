from __future__ import annotations

import logging

import pytest
from _pytest.fixtures import FixtureRequest
from dbt_semantic_interfaces.protocols import SemanticManifest
from metricflow_semantics.model.semantics.semantic_model_lookup import SemanticModelLookup
from metricflow_semantics.semantic_graph.lookups.manifest_object_lookup import ManifestObjectLookup
from metricflow_semantics.test_helpers.performance.benchmark_helpers import BenchmarkFunction, PerformanceBenchmark
from metricflow_semantics.test_helpers.snapshot_helpers import SnapshotConfiguration, assert_object_snapshot_equal
from metricflow_semantics.time.time_spine_source import TimeSpineSource

logger = logging.getLogger(__name__)


@pytest.mark.slow
def test_model_lookup_performance(manifest_with_200_models_100_metrics: SemanticManifest) -> None:
    """Benchmark the performance of lookups with a large manifest.

    As an example, this initializes the lookup and iterates through all measures.
    """
    semantic_manifest = manifest_with_200_models_100_metrics
    init_count = 5

    class _LeftFunction(BenchmarkFunction):
        """Using the existing lookup takes ~0.8s."""

        def run(self) -> None:
            for _ in range(init_count):
                time_spine_sources = TimeSpineSource.build_standard_time_spine_sources(semantic_manifest)
                custom_granularities = TimeSpineSource.build_custom_granularities(list(time_spine_sources.values()))
                model_lookup = SemanticModelLookup(semantic_manifest, custom_granularities)
                for _ in model_lookup.get_dimension_references():
                    pass

    class _RightFunction(BenchmarkFunction):
        def run(self) -> None:
            for _ in range(init_count):
                manifest_lookup = ManifestObjectLookup(semantic_manifest)
                for model_lookup in manifest_lookup.simple_metric_model_lookups:
                    for (
                        simple_metric_input_sequence
                    ) in model_lookup.aggregation_time_dimension_name_to_simple_metric_inputs.values():
                        for _ in simple_metric_input_sequence:
                            pass

    PerformanceBenchmark.assert_function_performance(
        left_function_class=_LeftFunction,
        right_function_class=_RightFunction,
        min_performance_factor=40,
    )


def test_lookup_attributes(
    request: FixtureRequest, mf_test_configuration: SnapshotConfiguration, sg_02_single_join_manifest: SemanticManifest
) -> None:
    """Tests the attributes of the object lookup."""
    lookup = ManifestObjectLookup(sg_02_single_join_manifest)
    assert_object_snapshot_equal(
        request=request,
        snapshot_configuration=mf_test_configuration,
        obj=lookup,
    )
