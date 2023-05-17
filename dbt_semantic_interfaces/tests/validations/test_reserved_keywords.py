import random

from copy import deepcopy
from dbt_semantic_interfaces.objects.semantic_manifest import SemanticManifest
from dbt_semantic_interfaces.objects.semantic_model import NodeRelation
from dbt_semantic_interfaces.validations.reserved_keywords import RESERVED_KEYWORDS, ReservedKeywordsRule
from dbt_semantic_interfaces.validations.validator_helpers import ValidationIssueLevel
from dbt_semantic_interfaces.test_utils import find_semantic_model_with


def random_keyword() -> str:  # noqa: D
    return random.choice(RESERVED_KEYWORDS)


def test_no_reserved_keywords(simple_model__with_primary_transforms: SemanticManifest) -> None:  # noqa: D
    issues = ReservedKeywordsRule.validate_model(simple_model__with_primary_transforms)
    assert len(issues) == 0


def test_reserved_keywords_in_dimensions(simple_model__with_primary_transforms: SemanticManifest) -> None:  # noqa: D
    model = deepcopy(simple_model__with_primary_transforms)
    (semantic_model, _index) = find_semantic_model_with(
        model=model, function=lambda semantic_model: len(semantic_model.dimensions) > 0
    )
    dimension = semantic_model.dimensions[0]
    dimension.name = random_keyword()

    issues = ReservedKeywordsRule.validate_model(model)
    assert len(issues) == 1
    assert issues[0].level == ValidationIssueLevel.ERROR


def test_reserved_keywords_in_measures(simple_model__with_primary_transforms: SemanticManifest) -> None:  # noqa: D
    model = deepcopy(simple_model__with_primary_transforms)
    (semantic_model, _index) = find_semantic_model_with(
        model=model, function=lambda semantic_model: len(semantic_model.measures) > 0
    )
    measure = semantic_model.measures[0]
    measure.name = random_keyword()

    issues = ReservedKeywordsRule.validate_model(model)
    assert len(issues) == 1
    assert issues[0].level == ValidationIssueLevel.ERROR


def test_reserved_keywords_in_entities(  # noqa: D
    simple_model__with_primary_transforms: SemanticManifest,
) -> None:
    model = deepcopy(simple_model__with_primary_transforms)
    (semantic_model, _index) = find_semantic_model_with(
        model=model, function=lambda semantic_model: len(semantic_model.entities) > 0
    )
    entity = semantic_model.entities[0]
    entity.name = random_keyword()

    issues = ReservedKeywordsRule.validate_model(model)
    assert len(issues) == 1
    assert issues[0].level == ValidationIssueLevel.ERROR


def test_reserved_keywords_in_node_relation(  # noqa: D
    simple_model__with_primary_transforms: SemanticManifest,
) -> None:
    model = deepcopy(simple_model__with_primary_transforms)
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
