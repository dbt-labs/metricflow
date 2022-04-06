import copy
import re
from typing import Callable

from metricflow.model.model_validator import ModelValidator
from metricflow.model.objects.data_source import DataSource
from metricflow.model.objects.user_configured_model import UserConfiguredModel
from metricflow.specs import IdentifierSpec
from metricflow.test.test_utils import find_data_source_with


def test_lonely_identifier_raises_issue(simple_model__pre_transforms: UserConfiguredModel) -> None:  # noqa: D
    model = copy.deepcopy(simple_model__pre_transforms)
    lonely_identifier_name = "hi_im_lonely"

    func: Callable[[DataSource], bool] = lambda data_source: len(data_source.identifiers) > 0
    data_source_with_identifiers, _ = find_data_source_with(model, func)
    data_source_with_identifiers.identifiers[0].name = IdentifierSpec.parse(lonely_identifier_name)
    build = ModelValidator.validate_model(model)

    found_warning = False
    warning = f"Identifier `{lonely_identifier_name}` only found in one data source `{data_source_with_identifiers.name}` which means it will be unused in joins."
    if build.issues is not None:
        for issue in build.issues:
            if re.search(warning, issue.message):
                found_warning = True

    assert found_warning
