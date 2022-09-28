import copy
import pytest
from typing import Tuple

from metricflow.model.model_validator import ModelValidator
from metricflow.model.objects.data_source import DataSource
from metricflow.model.objects.elements.dimension import Dimension, DimensionType
from metricflow.model.objects.user_configured_model import UserConfiguredModel
from metricflow.model.validations.validator_helpers import DataSourceElementType, ModelValidationException
from metricflow.test.test_utils import find_data_source_with


def _categorical_dimensions(data_source: DataSource) -> Tuple[Dimension, ...]:
    return tuple(dim for dim in data_source.dimensions if dim.type == DimensionType.CATEGORICAL)


def test_cross_element_names(simple_model__pre_transforms: UserConfiguredModel) -> None:  # noqa:D
    model = copy.deepcopy(simple_model__pre_transforms)

    # ensure we have a usable data source for the test
    usable_ds, usable_ds_index = find_data_source_with(
        model,
        lambda data_source: len(data_source.measures) > 0
        and len(data_source.identifiers) > 0
        and len(_categorical_dimensions(data_source=data_source)) > 0,
    )

    measure_reference = usable_ds.measures[0].reference
    # If the matching dimension is a time dimension we can accidentally create two primary time dimensions, and then
    # the validation will throw a different error and fail the test
    dimension_reference = _categorical_dimensions(data_source=usable_ds)[0].reference

    ds_measure_x_dimension = copy.deepcopy(usable_ds)
    ds_measure_x_identifier = copy.deepcopy(usable_ds)
    ds_dimension_x_identifier = copy.deepcopy(usable_ds)

    # We update the matching categorical dimension by reference for convenience
    ds_measure_x_dimension.get_dimension(dimension_reference).name = measure_reference.element_name
    ds_measure_x_identifier.identifiers[0].name = measure_reference.element_name
    ds_dimension_x_identifier.identifiers[0].name = dimension_reference.element_name

    model.data_sources[usable_ds_index] = ds_measure_x_dimension
    with pytest.raises(
        ModelValidationException,
        match=(
            f"element `{measure_reference.element_name}` is of type {DataSourceElementType.DIMENSION}, but it is used as "
            f"types .*?DataSourceElementType.DIMENSION.*?DataSourceElementType.MEASURE.*? across the model"
        ),
    ):
        ModelValidator().checked_validations(model)

    model.data_sources[usable_ds_index] = ds_measure_x_identifier
    with pytest.raises(
        ModelValidationException,
        match=(
            f"element `{measure_reference.element_name}` is of type {DataSourceElementType.IDENTIFIER}, but it is used as "
            f"types .*?DataSourceElementType.IDENTIFIER.*?DataSourceElementType.MEASURE.*? across the model"
        ),
    ):
        ModelValidator().checked_validations(model)

    model.data_sources[usable_ds_index] = ds_dimension_x_identifier
    with pytest.raises(
        ModelValidationException,
        match=(
            f"element `{dimension_reference.element_name}` is of type {DataSourceElementType.DIMENSION}, but it is used as "
            f"types .*?DataSourceElementType.DIMENSION.*?DataSourceElementType.IDENTIFIER.*? across the model"
        ),
    ):
        ModelValidator().checked_validations(model)
