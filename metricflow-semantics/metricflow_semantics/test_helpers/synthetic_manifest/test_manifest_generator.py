from __future__ import annotations

import logging

from _pytest.fixtures import FixtureRequest
from dbt_semantic_interfaces.implementations.semantic_manifest import PydanticSemanticManifest
from dbt_semantic_interfaces.transformations.semantic_manifest_transformer import PydanticSemanticManifestTransformer
from dbt_semantic_interfaces.validations.semantic_manifest_validator import SemanticManifestValidator

from metricflow_semantics.test_helpers.config_helpers import MetricFlowTestConfiguration
from metricflow_semantics.test_helpers.snapshot_helpers import assert_object_snapshot_equal
from metricflow_semantics.test_helpers.synthetic_manifest.semantic_manifest_generator import SyntheticManifestGenerator
from metricflow_semantics.test_helpers.synthetic_manifest.synthetic_manifest_parameter_set import (
    SyntheticManifestParameterSet,
)
from metricflow_semantics.toolkit.mf_logging.lazy_formattable import LazyFormat

logger = logging.getLogger(__name__)


def test_manifest_generator(  # noqa: D103
    request: FixtureRequest,
    mf_test_configuration: MetricFlowTestConfiguration,
) -> None:
    parameter_set = SyntheticManifestParameterSet(
        simple_metric_semantic_model_count=2,
        simple_metrics_per_semantic_model=2,
        dimension_semantic_model_count=2,
        categorical_dimensions_per_semantic_model=2,
        max_metric_depth=2,
        max_metric_width=2,
        saved_query_count=2,
        metrics_per_saved_query=2,
        categorical_dimensions_per_saved_query=2,
    )
    generator = SyntheticManifestGenerator(parameter_set)
    manifest = generator.generate_manifest()
    assert_object_snapshot_equal(
        request=request,
        snapshot_configuration=mf_test_configuration,
        obj=manifest,
    )

    manifest = PydanticSemanticManifestTransformer.transform(manifest)
    validator = SemanticManifestValidator[PydanticSemanticManifest]()
    validation_result = validator.validate_semantic_manifest(manifest)
    logger.debug(LazyFormat("Generated manifest", manifest=manifest))

    assert not validation_result.has_blocking_issues, LazyFormat(
        "Found validation issues with the generated manifest",
        validation_result=validation_result,
        manifest=manifest,
    ).evaluated_value
