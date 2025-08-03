from __future__ import annotations

import logging

import pytest
from dbt_semantic_interfaces.protocols import SemanticManifest
from dbt_semantic_interfaces.transformations.semantic_manifest_transformer import PydanticSemanticManifestTransformer
from metricflow_semantics.experimental.test_helpers.performance_helpers import BenchmarkFunction, PerformanceBenchmark
from metricflow_semantics.model.semantic_manifest_lookup import SemanticManifestLookup
from metricflow_semantics.test_helpers.synthetic_manifest.semantic_manifest_generator import SyntheticManifestGenerator
from metricflow_semantics.test_helpers.synthetic_manifest.synthetic_manifest_parameter_set import (
    SyntheticManifestParameterSet,
)
from typing_extensions import override

from metricflow.engine.metricflow_engine import MetricFlowEngine, MetricFlowQueryRequest
from metricflow.protocols.sql_client import SqlClient

logger = logging.getLogger(__name__)


@pytest.fixture(scope="session")
def manifest_with_50_models_25_metrics() -> SemanticManifest:
    """A semantic manifest with 200 models (100 with measures) and 100 metrics (50 of them derived)."""
    parameter_set = SyntheticManifestParameterSet(
        measure_semantic_model_count=25,
        measures_per_semantic_model=20,
        dimension_semantic_model_count=25,
        categorical_dimensions_per_semantic_model=20,
        max_metric_depth=2,
        max_metric_width=25,
        saved_query_count=100,
        metrics_per_saved_query=20,
        categorical_dimensions_per_saved_query=20,
    )

    generator = SyntheticManifestGenerator(parameter_set)
    semantic_manifest = generator.generate_manifest()
    return PydanticSemanticManifestTransformer.transform(semantic_manifest)


def test_explain_performance(manifest_with_50_models_25_metrics: SemanticManifest, sql_client: SqlClient) -> None:
    """Test the time to explain a number of queries."""
    semantic_manifest = manifest_with_50_models_25_metrics
    legacy_manifest_lookup = SemanticManifestLookup(semantic_manifest)
    legacy_resolver_engine = MetricFlowEngine(
        semantic_manifest_lookup=legacy_manifest_lookup,
        sql_client=sql_client,
    )
    sg_manifest_lookup = SemanticManifestLookup(semantic_manifest, use_semantic_graph=True)
    sg_resolver_engine = MetricFlowEngine(
        semantic_manifest_lookup=sg_manifest_lookup,
        sql_client=sql_client,
    )

    metric_names = ["metric_1_000"]
    group_by_names = ["metric_time", "common_entity__dimension_000", "common_entity__dimension_010"]
    where_constraints = ["{{ Metric('metric_1_001', group_by=['common_entity']) }}"]
    request = MetricFlowQueryRequest.create_with_random_request_id(
        metric_names=metric_names, group_by_names=group_by_names, where_constraints=where_constraints
    )

    class _LeftFunction(BenchmarkFunction):
        @override
        def run(self) -> None:
            legacy_resolver_engine.explain(request)

    class _RightFunction(BenchmarkFunction):
        @override
        def run(self) -> None:
            sg_resolver_engine.explain(request)

    PerformanceBenchmark.assert_function_performance(
        left_function_class=_LeftFunction,
        right_function_class=_RightFunction,
        min_performance_factor=0,
    )
