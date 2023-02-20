import copy
import pytest
import re
from typing import Callable

from metricflow.model.model_validator import ModelValidator
from dbt.contracts.graph.nodes import Entity
from dbt.contracts.graph.manifest import UserConfiguredModel
from metricflow.model.validations.common_identifiers import CommonIdentifiersRule
from metricflow.specs import IdentifierSpec
from metricflow.test.test_utils import find_entity_with


@pytest.mark.skip("TODO: re-enforce after validations improvements")
def test_lonely_identifier_raises_issue(simple_model__with_primary_transforms: UserConfiguredModel) -> None:  # noqa: D
    model = copy.deepcopy(simple_model__with_primary_transforms)
    lonely_identifier_name = "hi_im_lonely"

    func: Callable[[Entity], bool] = lambda entity: len(entity.identifiers) > 0
    entity_with_identifiers, _ = find_entity_with(model, func)
    entity_with_identifiers.identifiers[0].name = IdentifierSpec.from_name(lonely_identifier_name).element_name
    model_validator = ModelValidator([CommonIdentifiersRule()])
    build = model_validator.validate_model(model)

    found_warning = False
    warning = (
        f"Identifier `{lonely_identifier_name}` only found in one entity `{entity_with_identifiers.name}` "
        f"which means it will be unused in joins."
    )
    if build.issues is not None:
        for issue in build.issues:
            if re.search(warning, issue.message):
                found_warning = True

    assert found_warning
