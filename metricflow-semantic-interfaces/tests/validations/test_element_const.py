from __future__ import annotations

import copy
from typing import Tuple

import pytest
from metricflow_semantic_interfaces.implementations.elements.dimension import PydanticDimension
from metricflow_semantic_interfaces.implementations.semantic_manifest import (
    PydanticSemanticManifest,
)
from metricflow_semantic_interfaces.implementations.semantic_model import PydanticSemanticModel
from metricflow_semantic_interfaces.test_utils import find_semantic_model_with
from metricflow_semantic_interfaces.type_enums import DimensionType
from metricflow_semantic_interfaces.validations.element_const import ElementConsistencyRule
from metricflow_semantic_interfaces.validations.semantic_manifest_validator import (
    SemanticManifestValidator,
)
from metricflow_semantic_interfaces.validations.validator_helpers import (
    SemanticManifestValidationException,
    SemanticModelElementType,
)


def _categorical_dimensions(semantic_model: PydanticSemanticModel) -> Tuple[PydanticDimension, ...]:
    return tuple(dim for dim in semantic_model.dimensions if dim.type == DimensionType.CATEGORICAL)


def test_cross_element_names(  # noqa: D103
    simple_semantic_manifest__with_primary_transforms: PydanticSemanticManifest,
) -> None:
    model = copy.deepcopy(simple_semantic_manifest__with_primary_transforms)

    # ensure we have a usable semantic model for the test
    usable_ds, usable_ds_index = find_semantic_model_with(
        model,
        lambda semantic_model: len(semantic_model.measures) > 0
        and len(semantic_model.entities) > 0
        and len(_categorical_dimensions(semantic_model=semantic_model)) > 0,
    )

    measure_reference = usable_ds.measures[0].reference
    # If the matching dimension is a time dimension we can accidentally create two primary time dimensions, and then
    # the validation will throw a different error and fail the test
    dimension_reference = _categorical_dimensions(semantic_model=usable_ds)[0].reference

    ds_measure_x_dimension = copy.deepcopy(usable_ds)
    ds_measure_x_entity = copy.deepcopy(usable_ds)
    ds_dimension_x_entity = copy.deepcopy(usable_ds)

    # We update the matching categorical dimension by reference for convenience
    ds_measure_x_dimension.get_dimension(dimension_reference).name = measure_reference.element_name
    ds_measure_x_entity.entities[0].name = measure_reference.element_name
    ds_dimension_x_entity.entities[0].name = dimension_reference.element_name

    model.semantic_models[usable_ds_index] = ds_measure_x_dimension
    with pytest.raises(
        SemanticManifestValidationException,
        match=(
            f"element `{measure_reference.element_name}` is of type {SemanticModelElementType.DIMENSION}, but it is "
            "used as types .*?SemanticModelElementType.DIMENSION.*?SemanticModelElementType.MEASURE.*? across the "
            "model"
        ),
    ):
        SemanticManifestValidator[PydanticSemanticManifest]([ElementConsistencyRule()]).checked_validations(model)

    model.semantic_models[usable_ds_index] = ds_measure_x_entity
    with pytest.raises(
        SemanticManifestValidationException,
        match=(
            f"element `{measure_reference.element_name}` is of type {SemanticModelElementType.ENTITY}, but it is "
            "used as types .*?SemanticModelElementType.ENTITY.*?SemanticModelElementType.MEASURE.*? across the model"
        ),
    ):
        SemanticManifestValidator[PydanticSemanticManifest]([ElementConsistencyRule()]).checked_validations(model)

    model.semantic_models[usable_ds_index] = ds_dimension_x_entity
    with pytest.raises(
        SemanticManifestValidationException,
        match=(
            f"element `{dimension_reference.element_name}` is of type {SemanticModelElementType.DIMENSION}, but it is "
            "used as types .*?SemanticModelElementType.DIMENSION.*?SemanticModelElementType.ENTITY.*? across the model"
        ),
    ):
        SemanticManifestValidator[PydanticSemanticManifest]([ElementConsistencyRule()]).checked_validations(model)
