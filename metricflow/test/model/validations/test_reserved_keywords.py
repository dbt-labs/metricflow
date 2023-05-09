import copy
import random
from dbt_semantic_interfaces.objects.user_configured_model import UserConfiguredModel
from dbt_semantic_interfaces.objects.semantic_model import NodeRelation
from metricflow.model.validations.reserved_keywords import RESERVED_KEYWORDS, ReservedKeywordsRule
from metricflow.model.validations.validator_helpers import ValidationIssueLevel
from metricflow.test.test_utils import find_semantic_model_with


def random_keyword() -> str:  # noqa: D
    return random.choice(RESERVED_KEYWORDS)


def copied_model(simple_model__with_primary_transforms: UserConfiguredModel) -> UserConfiguredModel:  # noqa: D
    return copy.deepcopy(simple_model__with_primary_transforms)


def test_no_reserved_keywords(simple_model__with_primary_transforms: UserConfiguredModel) -> None:  # noqa: D
    issues = ReservedKeywordsRule.validate_model(simple_model__with_primary_transforms)
    assert len(issues) == 0


def test_reserved_keywords_in_dimensions(simple_model__with_primary_transforms: UserConfiguredModel) -> None:  # noqa: D
    model = copied_model(simple_model__with_primary_transforms)
    (semantic_model, _index) = find_semantic_model_with(
        model=model, function=lambda semantic_model: len(semantic_model.dimensions) > 0
    )
    dimension = semantic_model.dimensions[0]
    dimension.name = random_keyword()

    issues = ReservedKeywordsRule.validate_model(model)
    assert len(issues) == 1
    assert issues[0].level == ValidationIssueLevel.ERROR


def test_reserved_keywords_in_measures(simple_model__with_primary_transforms: UserConfiguredModel) -> None:  # noqa: D
    model = copied_model(simple_model__with_primary_transforms)
    (semantic_model, _index) = find_semantic_model_with(
        model=model, function=lambda semantic_model: len(semantic_model.measures) > 0
    )
    measure = semantic_model.measures[0]
    measure.name = random_keyword()

    issues = ReservedKeywordsRule.validate_model(model)
    assert len(issues) == 1
    assert issues[0].level == ValidationIssueLevel.ERROR


def test_reserved_keywords_in_entities(  # noqa: D
    simple_model__with_primary_transforms: UserConfiguredModel,
) -> None:
    model = copied_model(simple_model__with_primary_transforms)
    (semantic_model, _index) = find_semantic_model_with(
        model=model, function=lambda semantic_model: len(semantic_model.entities) > 0
    )
    entity = semantic_model.entities[0]
    entity.name = random_keyword()

    issues = ReservedKeywordsRule.validate_model(model)
    assert len(issues) == 1
    assert issues[0].level == ValidationIssueLevel.ERROR


def test_reserved_keywords_in_node_relation(  # noqa: D
    simple_model__with_primary_transforms: UserConfiguredModel,
) -> None:
    model = copied_model(simple_model__with_primary_transforms)
    (semantic_model_with_node_relation, _index) = find_semantic_model_with(
        model=model, function=lambda semantic_model: semantic_model.node_relation is not None
    )
    semantic_model_with_node_relation.node_relation = NodeRelation(
        alias=random_keyword(),
        schema_name="some_schema",
    )
    issues = ReservedKeywordsRule.validate_model(model)
    assert len(issues) == 1
    assert issues[0].level == ValidationIssueLevel.ERROR
