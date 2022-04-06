import copy
import re

from metricflow.model.objects.user_configured_model import UserConfiguredModel
from metricflow.model.model_validator import ModelValidator
from metricflow.object_utils import flatten_nested_sequence
from metricflow.test.test_utils import find_data_source_with


def test_measures_only_exist_in_one_data_source(simple_model__pre_transforms: UserConfiguredModel) -> None:  # noqa: D
    model = copy.deepcopy(simple_model__pre_transforms)

    build = ModelValidator.validate_model(model)
    duplicate_measure_message = "Found measure with name .* in multiple data sources with names"
    found_issue = False

    if build.issues is not None:
        for issue in build.issues:
            if re.search(duplicate_measure_message, issue.message):
                found_issue = True

    assert found_issue is False

    # add measure present in one data source to another
    first_data_source, _ = find_data_source_with(
        model, lambda data_source: data_source.measures is not None and len(data_source.measures) > 0
    )

    second_data_source, _ = find_data_source_with(
        model, lambda data_source: data_source.measures is not None and data_source.name != first_data_source.name
    )

    measure = first_data_source.measures[0]
    second_data_source.measures = list(flatten_nested_sequence([second_data_source.measures, [measure]]))

    build = ModelValidator.validate_model(model)

    if build.issues is not None:
        for issue in build.issues:
            if re.search(duplicate_measure_message, issue.message):
                found_issue = True

    assert found_issue is True
