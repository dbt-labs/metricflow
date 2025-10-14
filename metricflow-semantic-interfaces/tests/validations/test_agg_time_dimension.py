from __future__ import annotations

from copy import deepcopy

import pytest
from metricflow_semantic_interfaces.implementations.semantic_manifest import (
    PydanticSemanticManifest,
)
from metricflow_semantic_interfaces.test_utils import find_semantic_model_with
from metricflow_semantic_interfaces.type_enums import DimensionType
from metricflow_semantic_interfaces.validations.agg_time_dimension import (
    AggregationTimeDimensionRule,
)
from metricflow_semantic_interfaces.validations.semantic_manifest_validator import (
    SemanticManifestValidator,
)
from metricflow_semantic_interfaces.validations.validator_helpers import (
    SemanticManifestValidationException,
)


def test_invalid_aggregation_time_dimension(simple_semantic_manifest: PydanticSemanticManifest) -> None:  # noqa: D103
    model = deepcopy(simple_semantic_manifest)
    semantic_model_with_measures, _ = find_semantic_model_with(
        model,
        lambda semantic_model: len(semantic_model.measures) > 0,
    )

    semantic_model_with_measures.measures[0].agg_time_dimension = "invalid_time_dimension"

    with pytest.raises(
        SemanticManifestValidationException,
        match=(
            "has the aggregation time dimension set to 'invalid_time_dimension', which is not a valid time dimension "
            "in the semantic model"
        ),
    ):
        model_validator = SemanticManifestValidator[PydanticSemanticManifest]([AggregationTimeDimensionRule()])
        model_validator.checked_validations(model)


def test_unset_aggregation_time_dimension(simple_semantic_manifest: PydanticSemanticManifest) -> None:  # noqa: D103
    model = deepcopy(simple_semantic_manifest)
    semantic_model_with_measures, _ = find_semantic_model_with(
        model,
        lambda semantic_model: len(semantic_model.measures) > 0,
    )

    if semantic_model_with_measures.defaults is not None:
        semantic_model_with_measures.defaults.agg_time_dimension = None

    semantic_model_with_measures.measures[0].agg_time_dimension = None

    with pytest.raises(
        SemanticManifestValidationException,
        match=("Aggregation time dimension for measure \\w+ is not set!"),
    ):
        model_validator = SemanticManifestValidator[PydanticSemanticManifest]([AggregationTimeDimensionRule()])
        model_validator.checked_validations(model)


def test_missing_primary_time_ok_if_all_measures_have_agg_time_dim(  # noqa: D103
    simple_semantic_manifest: PydanticSemanticManifest,
) -> None:
    model = deepcopy(simple_semantic_manifest)
    semantic_model_with_measures, _ = find_semantic_model_with(
        model,
        lambda semantic_model: len(semantic_model.measures) > 0,
    )

    for dimension in semantic_model_with_measures.dimensions:
        if dimension.type == DimensionType.TIME:
            assert dimension.type_params, f"Time dimension `{dimension.name}` is missing `type_params`"

    model_validator = SemanticManifestValidator[PydanticSemanticManifest]([AggregationTimeDimensionRule()])
    model_validator.checked_validations(model)
