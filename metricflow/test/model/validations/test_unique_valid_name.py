import pytest
import copy

from metricflow.model.model_validator import ModelValidator
from metricflow.model.validations.validator_helpers import ModelValidationException
from metricflow.model.objects.user_configured_model import UserConfiguredModel
from metricflow.model.validations.unique_valid_name import UniqueAndValidNameRule
from metricflow.object_utils import flatten_nested_sequence
from metricflow.test.test_utils import find_data_source_with


def copied_model(simple_model__pre_transforms: UserConfiguredModel) -> UserConfiguredModel:  # noqa: D
    return copy.deepcopy(simple_model__pre_transforms)


"""
    Top Level Tests
    Top level elements include
    - Data Sources
    - Metrics
    - Metric Sets
    - Dimension Sets

    A name for any of these elements must be unique to all other top level elements
    except metrics. Metric names only need to be unique in comparison to other metric
    names.
"""


def test_duplicate_data_source_name(simple_model__pre_transforms: UserConfiguredModel) -> None:  # noqa: D
    model = copied_model(simple_model__pre_transforms)
    duplicated_data_source = model.data_sources[0]
    model.data_sources.append(duplicated_data_source)
    with pytest.raises(
        ModelValidationException,
        match=rf"Can't use name `{duplicated_data_source.name}` for a data source when it was already used for a data source",
    ):
        ModelValidator.checked_validations(model)


def test_duplicate_metric_name(simple_model__pre_transforms: UserConfiguredModel) -> None:  # noqa:D
    model = copied_model(simple_model__pre_transforms)
    duplicated_metric = model.metrics[0]
    model.metrics.append(duplicated_metric)
    with pytest.raises(
        ModelValidationException,
        match=rf"Can't use name `{duplicated_metric.name}` for a metric when it was already used for a metric",
    ):
        ModelValidator.checked_validations(model)


def test_duplicate_materalization_name(simple_model__pre_transforms: UserConfiguredModel) -> None:  # noqa:D
    model = copied_model(simple_model__pre_transforms)
    duplicated_materialization = model.materializations[0]
    model.materializations.append(duplicated_materialization)
    with pytest.raises(
        ModelValidationException,
        match=rf"Can't use name `{duplicated_materialization.name}` for a materialization when it was already used for a materialization",
    ):
        ModelValidator.checked_validations(model)


def test_top_level_metric_can_have_same_name_as_any_other_top_level_item(
    simple_model__pre_transforms: UserConfiguredModel,
) -> None:  # noqa:D
    metric_name = simple_model__pre_transforms.metrics[0].name

    model_data_source = copied_model(simple_model__pre_transforms)
    model_materialization = copied_model(simple_model__pre_transforms)

    model_data_source.data_sources[0].name = metric_name
    model_materialization.materializations[0].name = model_data_source.metrics[0].name

    ModelValidator.checked_validations(model_data_source)
    ModelValidator.checked_validations(model_materialization)


def test_top_level_elements_cant_share_names_except_with_metrics(
    simple_model__pre_transforms: UserConfiguredModel,
) -> None:  # noqa:D
    data_source_name = simple_model__pre_transforms.data_sources[0].name
    model_ds_and_mat = copied_model(simple_model__pre_transforms)
    model_ds_and_mat.materializations[0].name = data_source_name

    with pytest.raises(
        ModelValidationException,
        match=rf"Can't use name `{data_source_name}` for a materialization when it was already used for a data source",
    ):
        ModelValidator.checked_validations(model_ds_and_mat)


"""
    Data Source Element Tests
    There are three types of data source elements
    - measures
    - identifiers
    - dimensions

    A name for any of these elements must be unique to all other element names
    for the given data source.
"""


def test_cross_element_names(simple_model__pre_transforms: UserConfiguredModel) -> None:  # noqa:D
    model = copied_model(simple_model__pre_transforms)

    # ensure we have a usable data source for the test
    usable_ds, usable_ds_index = find_data_source_with(
        model,
        lambda data_source: len(data_source.measures) > 0
        and len(data_source.identifiers) > 0
        and len(data_source.dimensions) > 0,
    )

    measure_reference = usable_ds.measures[0].name
    dimension_reference = usable_ds.dimensions[1].name

    ds_measure_x_dimension = copy.deepcopy(usable_ds)
    ds_measure_x_identifier = copy.deepcopy(usable_ds)
    ds_dimension_x_identifier = copy.deepcopy(usable_ds)

    ds_measure_x_dimension.dimensions[1].name = measure_reference
    ds_measure_x_identifier.identifiers[1].name = measure_reference
    ds_dimension_x_identifier.identifiers[1].name = dimension_reference

    model.data_sources[usable_ds_index] = ds_measure_x_dimension
    with pytest.raises(
        ModelValidationException,
        match=rf"can't use name `{measure_reference.element_name}` for a dimension when it was already used for a measure",
    ):
        ModelValidator.checked_validations(model)

    model.data_sources[usable_ds_index] = ds_measure_x_identifier
    with pytest.raises(
        ModelValidationException,
        match=rf"can't use name `{measure_reference.element_name}` for a identifier when it was already used for a measure",
    ):
        ModelValidator.checked_validations(model)

    model.data_sources[usable_ds_index] = ds_dimension_x_identifier
    with pytest.raises(
        ModelValidationException,
        match=rf"can't use name `{dimension_reference.element_name}` for a dimension when it was already used for a identifier",
    ):
        ModelValidator.checked_validations(model)


def test_duplicate_measure_name(simple_model__pre_transforms: UserConfiguredModel) -> None:  # noqa:D
    model = copied_model(simple_model__pre_transforms)

    # Ensure we have a usable data source for the test
    data_source_with_measures, _ = find_data_source_with(model, lambda data_source: len(data_source.measures) > 0)

    duplicated_measure = data_source_with_measures.measures[0]
    duplicated_measures_tuple = (data_source_with_measures.measures, (duplicated_measure,))
    data_source_with_measures.measures = flatten_nested_sequence(duplicated_measures_tuple)

    with pytest.raises(
        ModelValidationException,
        match=rf"can't use name `{duplicated_measure.name.element_name}` for a measure when it was already used for a measure",
    ):
        ModelValidator.checked_validations(model)


def test_duplicate_dimension_name(simple_model__pre_transforms: UserConfiguredModel) -> None:  # noqa:D
    model = copied_model(simple_model__pre_transforms)

    # Ensure we have a usable data source for the test
    data_source_with_dimensions, _ = find_data_source_with(model, lambda data_source: len(data_source.dimensions) > 0)

    duplicated_dimension = data_source_with_dimensions.dimensions[0]
    duplicated_dimensions_tuple = (data_source_with_dimensions.dimensions, (duplicated_dimension,))
    data_source_with_dimensions.dimensions = flatten_nested_sequence(duplicated_dimensions_tuple)

    with pytest.raises(
        ModelValidationException,
        match=rf"can't use name `{duplicated_dimension.name.element_name}` for a "
        rf"dimension when it was already used for a dimension",
    ):
        ModelValidator.checked_validations(model)


def test_duplicate_identifier_name(simple_model__pre_transforms: UserConfiguredModel) -> None:  # noqa:D
    model = copied_model(simple_model__pre_transforms)

    # Ensure we have a usable data source for the test
    data_source_with_identifiers, _ = find_data_source_with(model, lambda data_source: len(data_source.identifiers) > 0)

    duplicated_identifier = data_source_with_identifiers.identifiers[0]
    duplicated_identifiers_tuple = (data_source_with_identifiers.identifiers, (duplicated_identifier,))
    data_source_with_identifiers.identifiers = flatten_nested_sequence(duplicated_identifiers_tuple)

    with pytest.raises(
        ModelValidationException,
        match=rf"can't use name `{duplicated_identifier.name.element_name}` for a identifier when it was already used for a identifier",
    ):
        ModelValidator.checked_validations(model)


"""
    Tests for valid naming
"""


def test_name_is_valid() -> None:  # noqa:D
    assert UniqueAndValidNameRule.check_valid_name("this_is_valid1") == []


def test_invalid_names() -> None:  # noqa:D
    # This is non-exhaustive, but covers some "common" error cases
    assert UniqueAndValidNameRule.check_valid_name("this is invalid") != []
    assert UniqueAndValidNameRule.check_valid_name("100%invalid") != []
    assert UniqueAndValidNameRule.check_valid_name("no-hyphens-allowed") != []
    assert UniqueAndValidNameRule.check_valid_name("punctuation.is.bad") != []
    assert UniqueAndValidNameRule.check_valid_name("cantwildstarme*") != []
    assert UniqueAndValidNameRule.check_valid_name("noemails@hellowdotcom") != []
    assert UniqueAndValidNameRule.check_valid_name("(no)(ordering)(operations)") != []
    assert UniqueAndValidNameRule.check_valid_name("path/to/invalid/name") != []
    assert UniqueAndValidNameRule.check_valid_name("#notwitterhere") != []
    assert UniqueAndValidNameRule.check_valid_name("_no_leading_underscore") != []
    assert UniqueAndValidNameRule.check_valid_name("no_trailing_underscore_") != []
    assert UniqueAndValidNameRule.check_valid_name("_definitely_no_leading_and_trailing_underscore_") != []

    # time granularity values are reserved
    assert UniqueAndValidNameRule.check_valid_name("day") != []
    assert UniqueAndValidNameRule.check_valid_name("week") != []
    assert UniqueAndValidNameRule.check_valid_name("month") != []
    assert UniqueAndValidNameRule.check_valid_name("quarter") != []
    assert UniqueAndValidNameRule.check_valid_name("year") != []
