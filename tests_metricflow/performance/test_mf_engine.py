from __future__ import annotations

import logging

import pytest
from dbt_semantic_interfaces.test_utils import as_datetime
from dbt_semantic_interfaces.transformations.semantic_manifest_transformer import PydanticSemanticManifestTransformer
from metricflow_semantics.model.semantic_manifest_lookup import SemanticManifestLookup
from metricflow_semantics.query.query_parser import MetricFlowQueryParser
from metricflow_semantics.specs.dunder_column_association_resolver import DunderColumnAssociationResolver
from metricflow_semantics.test_helpers.performance.benchmark_helpers import (
    BenchmarkFunction,
    OneSecondFunction,
    PerformanceBenchmark,
)
from metricflow_semantics.test_helpers.synthetic_manifest.semantic_manifest_generator import SyntheticManifestGenerator
from metricflow_semantics.test_helpers.synthetic_manifest.synthetic_manifest_parameter_set import (
    SyntheticManifestParameterSet,
)
from metricflow_semantics.test_helpers.time_helpers import ConfigurableTimeSource
from typing_extensions import override

from metricflow.engine.metricflow_engine import MetricFlowEngine
from metricflow.protocols.sql_client import SqlClient

logger = logging.getLogger(__name__)


@pytest.mark.slow
def test_init_time(sql_client: SqlClient) -> None:
    """Test that the MF engine init time is 10x faster when initialized with the index."""
    parameter_set = SyntheticManifestParameterSet(
        simple_metric_semantic_model_count=20,
        simple_metrics_per_semantic_model=20,
        dimension_semantic_model_count=20,
        categorical_dimensions_per_semantic_model=10,
        max_metric_depth=1,
        max_metric_width=400,
        saved_query_count=0,
        metrics_per_saved_query=0,
        categorical_dimensions_per_saved_query=0,
    )
    generator = SyntheticManifestGenerator(parameter_set)
    semantic_manifest = generator.generate_manifest()
    semantic_manifest = PydanticSemanticManifestTransformer.transform(semantic_manifest)
    column_association_resolver = DunderColumnAssociationResolver()

    class _RightFunction(BenchmarkFunction):
        @override
        def run(self) -> None:
            semantic_manifest_lookup = SemanticManifestLookup(semantic_manifest)
            query_parser = MetricFlowQueryParser(semantic_manifest_lookup=semantic_manifest_lookup)
            MetricFlowEngine(
                semantic_manifest_lookup=semantic_manifest_lookup,
                sql_client=sql_client,
                time_source=ConfigurableTimeSource(as_datetime("2020-01-01")),
                query_parser=query_parser,
                column_association_resolver=column_association_resolver,
            )

    PerformanceBenchmark.assert_function_performance(
        left_function_class=OneSecondFunction,
        right_function_class=_RightFunction,
        min_performance_factor=4,
    )
