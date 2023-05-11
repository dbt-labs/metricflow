import copy
import re
from typing import Callable

from dbt_semantic_interfaces.model_validator import ModelValidator
from dbt_semantic_interfaces.objects.semantic_model import SemanticModel
from dbt_semantic_interfaces.objects.semantic_manifest import SemanticManifest
from dbt_semantic_interfaces.validations.common_entities import CommonEntitysRule
from dbt_semantic_interfaces.test_utils import find_semantic_model_with


def test_lonely_entity_raises_issue(simple_model__with_primary_transforms: SemanticManifest) -> None:  # noqa: D
    model = copy.deepcopy(simple_model__with_primary_transforms)
    lonely_entity_name = "hi_im_lonely"

    func: Callable[[SemanticModel], bool] = lambda semantic_model: len(semantic_model.entities) > 0
    semantic_model_with_entities, _ = find_semantic_model_with(model, func)
    semantic_model_with_entities.entities[0].name = lonely_entity_name
    model_validator = ModelValidator([CommonEntitysRule()])
    model_issues = model_validator.validate_model(model)

    found_warning = False
    warning = (
        f"Entity `{lonely_entity_name}` only found in one semantic model `{semantic_model_with_entities.name}` "
        f"which means it will be unused in joins."
    )
    if model_issues is not None:
        for issue in model_issues.all_issues:
            if re.search(warning, issue.message):
                found_warning = True

    assert found_warning
