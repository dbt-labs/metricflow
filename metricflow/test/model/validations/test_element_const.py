import copy
import pytest
from typing import Tuple

from dbt.semantic.validations.model_validator import ModelValidator
from dbt.contracts.graph.nodes import Entity
from dbt.contracts.graph.dimensions import Dimension, DimensionType
from dbt.contracts.graph.manifest import UserConfiguredModel
from metricflow.model.validations.element_const import ElementConsistencyRule
from metricflow.model.validations.validator_helpers import EntityElementType, ModelValidationException
from metricflow.test.test_utils import find_entity_with


def _categorical_dimensions(entity: Entity) -> Tuple[Dimension, ...]:
    return tuple(dim for dim in entity.dimensions if dim.type == DimensionType.CATEGORICAL)


def test_cross_element_names(simple_model__with_primary_transforms: UserConfiguredModel) -> None:  # noqa:D
    model = copy.deepcopy(simple_model__with_primary_transforms)

    # ensure we have a usable entity for the test
    usable_ds, usable_ds_index = find_entity_with(
        model,
        lambda entity: len(entity.measures) > 0
        and len(entity.identifiers) > 0
        and len(_categorical_dimensions(entity=entity)) > 0,
    )

    measure_reference = usable_ds.measures[0].reference
    # If the matching dimension is a time dimension we can accidentally create two primary time dimensions, and then
    # the validation will throw a different error and fail the test
    dimension_reference = _categorical_dimensions(entity=usable_ds)[0].reference

    ds_measure_x_dimension = copy.deepcopy(usable_ds)
    ds_measure_x_identifier = copy.deepcopy(usable_ds)
    ds_dimension_x_identifier = copy.deepcopy(usable_ds)

    # We update the matching categorical dimension by reference for convenience
    ds_measure_x_dimension.get_dimension(dimension_reference).name = measure_reference.element_name
    ds_measure_x_identifier.identifiers[0].name = measure_reference.element_name
    ds_dimension_x_identifier.identifiers[0].name = dimension_reference.element_name

    model.entities[usable_ds_index] = ds_measure_x_dimension
    with pytest.raises(
        ModelValidationException,
        match=(
            f"element `{measure_reference.element_name}` is of type {EntityElementType.DIMENSION}, but it is used as "
            f"types .*?EntityElementType.DIMENSION.*?EntityElementType.MEASURE.*? across the model"
        ),
    ):
        ModelValidator([ElementConsistencyRule()]).checked_validations(model)

    model.entities[usable_ds_index] = ds_measure_x_identifier
    with pytest.raises(
        ModelValidationException,
        match=(
            f"element `{measure_reference.element_name}` is of type {EntityElementType.IDENTIFIER}, but it is used as "
            f"types .*?EntityElementType.IDENTIFIER.*?EntityElementType.MEASURE.*? across the model"
        ),
    ):
        ModelValidator([ElementConsistencyRule()]).checked_validations(model)

    model.entities[usable_ds_index] = ds_dimension_x_identifier
    with pytest.raises(
        ModelValidationException,
        match=(
            f"element `{dimension_reference.element_name}` is of type {EntityElementType.DIMENSION}, but it is used as "
            f"types .*?EntityElementType.DIMENSION.*?EntityElementType.IDENTIFIER.*? across the model"
        ),
    ):
        ModelValidator([ElementConsistencyRule()]).checked_validations(model)
