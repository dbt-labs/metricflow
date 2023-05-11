import copy
import pytest
from typing import Tuple

from dbt_semantic_interfaces.model_validator import ModelValidator
from dbt_semantic_interfaces.objects.semantic_model import SemanticModel
from dbt_semantic_interfaces.objects.elements.dimension import Dimension, DimensionType
from dbt_semantic_interfaces.objects.semantic_manifest import SemanticManifest
from dbt_semantic_interfaces.validations.element_const import ElementConsistencyRule
from dbt_semantic_interfaces.validations.validator_helpers import SemanticModelElementType, ModelValidationException
from dbt_semantic_interfaces.test_utils import find_semantic_model_with


def _categorical_dimensions(semantic_model: SemanticModel) -> Tuple[Dimension, ...]:
    return tuple(dim for dim in semantic_model.dimensions if dim.type == DimensionType.CATEGORICAL)


def test_cross_element_names(simple_model__with_primary_transforms: SemanticManifest) -> None:  # noqa:D
    model = copy.deepcopy(simple_model__with_primary_transforms)

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
        ModelValidationException,
        match=(
            f"element `{measure_reference.element_name}` is of type {SemanticModelElementType.DIMENSION}, but it is used as "
            f"types .*?SemanticModelElementType.DIMENSION.*?SemanticModelElementType.MEASURE.*? across the model"
        ),
    ):
        ModelValidator([ElementConsistencyRule()]).checked_validations(model)

    model.semantic_models[usable_ds_index] = ds_measure_x_entity
    with pytest.raises(
        ModelValidationException,
        match=(
            f"element `{measure_reference.element_name}` is of type {SemanticModelElementType.ENTITY}, but it is used as "
            f"types .*?SemanticModelElementType.ENTITY.*?SemanticModelElementType.MEASURE.*? across the model"
        ),
    ):
        ModelValidator([ElementConsistencyRule()]).checked_validations(model)

    model.semantic_models[usable_ds_index] = ds_dimension_x_entity
    with pytest.raises(
        ModelValidationException,
        match=(
            f"element `{dimension_reference.element_name}` is of type {SemanticModelElementType.DIMENSION}, but it is used as "
            f"types .*?SemanticModelElementType.DIMENSION.*?SemanticModelElementType.ENTITY.*? across the model"
        ),
    ):
        ModelValidator([ElementConsistencyRule()]).checked_validations(model)
