from __future__ import annotations

import copy
import re

from metricflow_semantic_interfaces.implementations.semantic_manifest import (
    PydanticSemanticManifest,
)
from metricflow_semantic_interfaces.implementations.semantic_model import PydanticSemanticModel
from metricflow_semantic_interfaces.test_utils import find_semantic_model_with
from metricflow_semantic_interfaces.validations.common_entities import CommonEntitysRule
from metricflow_semantic_interfaces.validations.semantic_manifest_validator import (
    SemanticManifestValidator,
)


def test_lonely_entity_raises_issue(  # noqa: D103
    simple_semantic_manifest__with_primary_transforms: PydanticSemanticManifest,
) -> None:
    model = copy.deepcopy(simple_semantic_manifest__with_primary_transforms)
    lonely_entity_name = "hi_im_lonely"

    def func(semantic_model: PydanticSemanticModel) -> bool:
        return len(semantic_model.entities) > 0

    semantic_model_with_entities, _ = find_semantic_model_with(model, func)
    semantic_model_with_entities.entities[0].name = lonely_entity_name
    model_validator = SemanticManifestValidator[PydanticSemanticManifest]([CommonEntitysRule()])
    model_issues = model_validator.validate_semantic_manifest(model)

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
