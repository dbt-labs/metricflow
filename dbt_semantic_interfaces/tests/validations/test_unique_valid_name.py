import more_itertools
import pytest
from copy import deepcopy

from dbt_semantic_interfaces.model_validator import ModelValidator
from dbt_semantic_interfaces.validations.validator_helpers import ModelValidationException
from dbt_semantic_interfaces.objects.user_configured_model import UserConfiguredModel
from dbt_semantic_interfaces.validations.unique_valid_name import MetricFlowReservedKeywords, UniqueAndValidNameRule
from dbt_semantic_interfaces.test_utils import find_data_source_with


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


def test_duplicate_data_source_name(simple_model__with_primary_transforms: UserConfiguredModel) -> None:  # noqa: D
    model = deepcopy(simple_model__with_primary_transforms)
    duplicated_data_source = model.data_sources[0]
    model.data_sources.append(duplicated_data_source)
    with pytest.raises(
        ModelValidationException,
        match=rf"Can't use name `{duplicated_data_source.name}` for a data source when it was already used for a data source",
    ):
        ModelValidator([UniqueAndValidNameRule()]).checked_validations(model)


def test_duplicate_metric_name(simple_model__with_primary_transforms: UserConfiguredModel) -> None:  # noqa:D
    model = deepcopy(simple_model__with_primary_transforms)
    duplicated_metric = model.metrics[0]
    model.metrics.append(duplicated_metric)
    with pytest.raises(
        ModelValidationException,
        match=rf"Can't use name `{duplicated_metric.name}` for a metric when it was already used for a metric",
    ):
        ModelValidator([UniqueAndValidNameRule()]).checked_validations(model)


def test_top_level_metric_can_have_same_name_as_any_other_top_level_item(
    simple_model__with_primary_transforms: UserConfiguredModel,
) -> None:  # noqa:D
    metric_name = simple_model__with_primary_transforms.metrics[0].name

    model_data_source = deepcopy(simple_model__with_primary_transforms)

    model_data_source.data_sources[0].name = metric_name

    ModelValidator([UniqueAndValidNameRule()]).checked_validations(model_data_source)


"""
    Data Source Element Tests
    There are three types of data source elements
    - measures
    - identifiers
    - dimensions

    A name for any of these elements must be unique to all other element names
    for the given data source.
"""


def test_duplicate_measure_name(simple_model__with_primary_transforms: UserConfiguredModel) -> None:  # noqa:D
    model = deepcopy(simple_model__with_primary_transforms)

    # Ensure we have a usable data source for the test
    data_source_with_measures, _ = find_data_source_with(model, lambda data_source: len(data_source.measures) > 0)

    duplicated_measure = data_source_with_measures.measures[0]
    duplicated_measures_tuple = (data_source_with_measures.measures, (duplicated_measure,))
    data_source_with_measures.measures = tuple(more_itertools.flatten(duplicated_measures_tuple))

    with pytest.raises(
        ModelValidationException,
        match=rf"can't use name `{duplicated_measure.reference.element_name}` for a measure when it was already used for a measure",
    ):
        ModelValidator([UniqueAndValidNameRule()]).checked_validations(model)


def test_duplicate_dimension_name(simple_model__with_primary_transforms: UserConfiguredModel) -> None:  # noqa:D
    model = deepcopy(simple_model__with_primary_transforms)

    # Ensure we have a usable data source for the test
    data_source_with_dimensions, _ = find_data_source_with(model, lambda data_source: len(data_source.dimensions) > 0)

    duplicated_dimension = data_source_with_dimensions.dimensions[0]
    duplicated_dimensions_tuple = (data_source_with_dimensions.dimensions, (duplicated_dimension,))
    data_source_with_dimensions.dimensions = tuple(more_itertools.flatten(duplicated_dimensions_tuple))

    with pytest.raises(
        ModelValidationException,
        match=rf"can't use name `{duplicated_dimension.reference.element_name}` for a "
        rf"dimension when it was already used for a dimension",
    ):
        ModelValidator([UniqueAndValidNameRule()]).checked_validations(model)


def test_duplicate_entity_name(simple_model__with_primary_transforms: UserConfiguredModel) -> None:  # noqa:D
    model = deepcopy(simple_model__with_primary_transforms)

    # Ensure we have a usable data source for the test
    data_source_with_identifiers, _ = find_data_source_with(model, lambda data_source: len(data_source.identifiers) > 0)

    duplicated_identifier = data_source_with_identifiers.identifiers[0]
    duplicated_identifiers_tuple = (data_source_with_identifiers.identifiers, (duplicated_identifier,))
    data_source_with_identifiers.identifiers = tuple(more_itertools.flatten(duplicated_identifiers_tuple))

    with pytest.raises(
        ModelValidationException,
        match=rf"can't use name `{duplicated_identifier.reference.element_name}` for a entity when it was already used for a entity",
    ):
        ModelValidator([UniqueAndValidNameRule()]).checked_validations(model)


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


def test_reserved_name() -> None:  # noqa: D
    reserved_keyword = MetricFlowReservedKeywords.METRIC_TIME
    reserved_reason = MetricFlowReservedKeywords.get_reserved_reason(reserved_keyword)
    issues = UniqueAndValidNameRule.check_valid_name(reserved_keyword.value.lower())
    match = False
    for issue in issues:
        if issue.message.find(reserved_reason) != -1:
            match = True
    assert (
        match
    ), f"Did not find reason: '{reserved_reason}' in issues: {issues} for name: '{reserved_keyword.value.lower()}'"
