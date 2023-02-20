import copy
import random
from dbt.contracts.graph.identifiers import CompositeSubIdentifier
from dbt.contracts.graph.manifest import UserConfiguredModel
from metricflow.model.validations.reserved_keywords import RESERVED_KEYWORDS, ReservedKeywordsRule
from metricflow.model.validations.validator_helpers import ValidationIssueLevel
from metricflow.test.test_utils import find_entity_with


def random_keyword() -> str:  # noqa: D
    return random.choice(RESERVED_KEYWORDS)


def copied_model(simple_model__with_primary_transforms: UserConfiguredModel) -> UserConfiguredModel:  # noqa: D
    return copy.deepcopy(simple_model__with_primary_transforms)


def test_no_reserved_keywords(simple_model__with_primary_transforms: UserConfiguredModel) -> None:  # noqa: D
    issues = ReservedKeywordsRule.validate_model(simple_model__with_primary_transforms)
    assert len(issues) == 0


def test_reserved_keywords_in_sql_table(simple_model__with_primary_transforms: UserConfiguredModel) -> None:  # noqa: D
    model = copied_model(simple_model__with_primary_transforms)
    (entity_with_sql_table, _index) = find_entity_with(
        model=model, function=lambda entity: entity.sql_table is not None
    )
    entity_with_sql_table.sql_table = f"{random_keyword()}.{random_keyword()}"

    issues = ReservedKeywordsRule.validate_model(model)
    assert len(issues) == 1
    assert issues[0].level == ValidationIssueLevel.ERROR


def test_reserved_keywords_in_dimensions(simple_model__with_primary_transforms: UserConfiguredModel) -> None:  # noqa: D
    model = copied_model(simple_model__with_primary_transforms)
    (entity, _index) = find_entity_with(
        model=model, function=lambda entity: len(entity.dimensions) > 0
    )
    dimension = entity.dimensions[0]
    dimension.name = random_keyword()

    issues = ReservedKeywordsRule.validate_model(model)
    assert len(issues) == 1
    assert issues[0].level == ValidationIssueLevel.ERROR


def test_reserved_keywords_in_measures(simple_model__with_primary_transforms: UserConfiguredModel) -> None:  # noqa: D
    model = copied_model(simple_model__with_primary_transforms)
    (entity, _index) = find_entity_with(
        model=model, function=lambda entity: len(entity.measures) > 0
    )
    measure = entity.measures[0]
    measure.name = random_keyword()

    issues = ReservedKeywordsRule.validate_model(model)
    assert len(issues) == 1
    assert issues[0].level == ValidationIssueLevel.ERROR


def test_reserved_keywords_in_identifiers(  # noqa: D
    simple_model__with_primary_transforms: UserConfiguredModel,
) -> None:
    model = copied_model(simple_model__with_primary_transforms)
    (entity, _index) = find_entity_with(
        model=model, function=lambda entity: len(entity.identifiers) > 0
    )
    identifier = entity.identifiers[0]
    identifier.name = random_keyword()
    identifier.identifiers = []

    issues = ReservedKeywordsRule.validate_model(model)
    assert len(issues) == 1
    assert issues[0].level == ValidationIssueLevel.ERROR


def test_reserved_keywords_in_composite_identifiers(  # noqa: D
    simple_model__with_primary_transforms: UserConfiguredModel,
) -> None:
    model = copied_model(simple_model__with_primary_transforms)
    (entity, _index) = find_entity_with(
        model=model, function=lambda entity: len(entity.identifiers) > 0
    )
    identifier = entity.identifiers[0]
    identifier.identifiers = [
        CompositeSubIdentifier(name=random_keyword()),  # should error
        CompositeSubIdentifier(name=random_keyword()),  # should error
        CompositeSubIdentifier(expr="SELECT TRUE AS col1"),  # shouldn't error
    ]

    issues = ReservedKeywordsRule.validate_model(model)
    assert len(issues) == 2
    assert issues[0].level == ValidationIssueLevel.ERROR
    assert issues[1].level == ValidationIssueLevel.ERROR
