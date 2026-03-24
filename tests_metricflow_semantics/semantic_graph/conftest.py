from __future__ import annotations

import logging

import pytest
from dbt_semantic_interfaces.protocols import SemanticManifest
from dbt_semantic_interfaces.transformations.semantic_manifest_transformer import PydanticSemanticManifestTransformer
from metricflow_semantics.test_helpers.synthetic_manifest.semantic_manifest_generator import SyntheticManifestGenerator
from metricflow_semantics.test_helpers.synthetic_manifest.synthetic_manifest_parameter_set import (
    SyntheticManifestParameterSet,
)

logger = logging.getLogger(__name__)


@pytest.fixture(scope="session")
def manifest_with_200_models_100_metrics() -> SemanticManifest:
    """A semantic manifest with 200 models and 100 metrics (50 of them derived)."""
    parameter_set = SyntheticManifestParameterSet(
        simple_metric_semantic_model_count=100,
        simple_metrics_per_semantic_model=20,
        dimension_semantic_model_count=100,
        categorical_dimensions_per_semantic_model=20,
        max_metric_depth=2,
        max_metric_width=50,
        saved_query_count=100,
        metrics_per_saved_query=20,
        categorical_dimensions_per_saved_query=20,
    )

    generator = SyntheticManifestGenerator(parameter_set)
    semantic_manifest = generator.generate_manifest()
    return PydanticSemanticManifestTransformer.transform(semantic_manifest)
