import copy
import random
from metricflow.model.objects.elements.identifier import CompositeSubIdentifier
from metricflow.model.objects.user_configured_model import UserConfiguredModel
from metricflow.model.validations.reserved_keywords import RESERVED_KEYWORDS, ReservedKeywordsRule
from metricflow.model.validations.validator_helpers import ValidationIssueLevel
from metricflow.test.test_utils import find_data_source_with


def random_keyword() -> str:  # noqa: D
    return random.choice(RESERVED_KEYWORDS)


def copied_model(simple_model__pre_transforms: UserConfiguredModel) -> UserConfiguredModel:  # noqa: D
    return copy.deepcopy(simple_model__pre_transforms)


def test_no_reserved_keywords(simple_model__pre_transforms: UserConfiguredModel) -> None:  # noqa: D
    issues = ReservedKeywordsRule.validate_model(simple_model__pre_transforms)
    assert len(issues) == 0


def test_reserved_keywords_in_sql_table(simple_model__pre_transforms: UserConfiguredModel) -> None:  # noqa: D
    model = copied_model(simple_model__pre_transforms)
    (data_source_with_sql_table, _index) = find_data_source_with(
        model=model, function=lambda data_source: data_source.sql_table is not None
    )
    data_source_with_sql_table.sql_table = f"{random_keyword()}.{random_keyword()}"

    issues = ReservedKeywordsRule.validate_model(model)
    assert len(issues) == 1
    assert issues[0].level == ValidationIssueLevel.ERROR


def test_reserved_keywords_in_dimensions(simple_model__pre_transforms: UserConfiguredModel) -> None:  # noqa: D
    model = copied_model(simple_model__pre_transforms)
    (data_source, _index) = find_data_source_with(
        model=model, function=lambda data_source: len(data_source.dimensions) > 0
    )
    dimension = data_source.dimensions[0]
    dimension.name = random_keyword()

    issues = ReservedKeywordsRule.validate_model(model)
    assert len(issues) == 1
    assert issues[0].level == ValidationIssueLevel.ERROR


def test_reserved_keywords_in_measures(simple_model__pre_transforms: UserConfiguredModel) -> None:  # noqa: D
    model = copied_model(simple_model__pre_transforms)
    (data_source, _index) = find_data_source_with(
        model=model, function=lambda data_source: len(data_source.measures) > 0
    )
    measure = data_source.measures[0]
    measure.name = random_keyword()

    issues = ReservedKeywordsRule.validate_model(model)
    assert len(issues) == 1
    assert issues[0].level == ValidationIssueLevel.ERROR


def test_reserved_keywords_in_identifiers(simple_model__pre_transforms: UserConfiguredModel) -> None:  # noqa: D
    model = copied_model(simple_model__pre_transforms)
    (data_source, _index) = find_data_source_with(
        model=model, function=lambda data_source: len(data_source.identifiers) > 0
    )
    identifier = data_source.identifiers[0]
    identifier.name = random_keyword()
    identifier.identifiers = []

    issues = ReservedKeywordsRule.validate_model(model)
    assert len(issues) == 1
    assert issues[0].level == ValidationIssueLevel.ERROR


def test_reserved_keywords_in_composite_identifiers(  # noqa: D
    simple_model__pre_transforms: UserConfiguredModel,
) -> None:
    model = copied_model(simple_model__pre_transforms)
    (data_source, _index) = find_data_source_with(
        model=model, function=lambda data_source: len(data_source.identifiers) > 0
    )
    identifier = data_source.identifiers[0]
    identifier.identifiers = [
        CompositeSubIdentifier(name=random_keyword()),  # should error
        CompositeSubIdentifier(name=random_keyword()),  # should error
        CompositeSubIdentifier(expr="SELECT TRUE AS col1"),  # shouldn't error
    ]

    issues = ReservedKeywordsRule.validate_model(model)
    assert len(issues) == 2
    assert issues[0].level == ValidationIssueLevel.ERROR
    assert issues[1].level == ValidationIssueLevel.ERROR
