import re
from collections import OrderedDict
from typing import Dict, Tuple, List, Optional

from metricflow.model.objects.data_source import DataSource
from metricflow.model.objects.user_configured_model import UserConfiguredModel
from metricflow.model.validations.validator_helpers import (
    ModelValidationRule,
    ValidationIssue,
    ValidationIssueLevel,
    validate_safely,
)
from metricflow.specs import ElementReference
from metricflow.time.time_granularity import TimeGranularity


class UniqueAndValidNameRule(ModelValidationRule):
    """Check that names are unique and valid.

    * Names of elements in data sources are unique / valid within the data source.
    * Names of data sources, dimension sets, metric sets, and materializations in the model are unique / valid.
    """

    NAME_REGEX = re.compile(r"\A[a-z][a-z0-9_]*[a-z0-9]\Z")

    @staticmethod
    def check_valid_name(name: str, data_source_name: Optional[str] = None) -> List[ValidationIssue]:  # noqa: D
        issues = []
        if not UniqueAndValidNameRule.NAME_REGEX.match(name):
            issues.append(
                ValidationIssue(
                    level=ValidationIssueLevel.ERROR,
                    model_object_reference=ValidationIssue.make_object_reference(
                        data_source_name=data_source_name,
                    )
                    if data_source_name
                    else OrderedDict(),
                    message=f"Invalid name `{name}` - names should only consist of lower case letters, numbers, "
                    f"and underscores. In addition, names should start with a lower case letter, and should not end "
                    f"with an underscore, and they must be at least 2 characters long.",
                )
            )
        if name.upper() in TimeGranularity.list_names():
            issues.append(
                ValidationIssue(
                    level=ValidationIssueLevel.ERROR,
                    model_object_reference=ValidationIssue.make_object_reference(
                        data_source_name=data_source_name,
                    )
                    if data_source_name
                    else OrderedDict(),
                    message=f"Invalid name `{name}` - names cannot match reserved time granularity keywords "
                    f"({TimeGranularity.list_names()})",
                )
            )
        return issues

    @staticmethod
    @validate_safely(whats_being_done="checking data source sub element names are unique")
    def _validate_data_source_elements(data_source: DataSource) -> List[ValidationIssue]:
        issues = []
        name_and_type_tuples: List[Tuple[ElementReference, str]] = []

        if data_source.measures:
            for measure in data_source.measures:
                name_and_type_tuples.append((measure.name, "measure"))
        if data_source.identifiers:
            for identifier in data_source.identifiers:
                name_and_type_tuples.append((identifier.name, "identifier"))
        if data_source.dimensions:
            for dimension in data_source.dimensions:
                name_and_type_tuples.append((dimension.name, "dimension"))

        name_to_type: Dict[ElementReference, str] = {}

        for name, _type in name_and_type_tuples:
            if name in name_to_type:
                issues.append(
                    ValidationIssue(
                        level=ValidationIssueLevel.FATAL,
                        model_object_reference=ValidationIssue.make_object_reference(
                            data_source_name=data_source.name,
                        ),
                        message=f"In data source `{data_source.name}`, can't use name `{name.element_name}` for a "
                        f"{_type} when it was already used for a {name_to_type[name]}",
                    )
                )
            else:
                name_to_type[name] = _type

        for name, _ in name_and_type_tuples:
            issues += UniqueAndValidNameRule.check_valid_name(data_source_name=data_source.name, name=name.element_name)

        return issues

    @staticmethod
    @validate_safely(whats_being_done="checking model top level element names are sufficiently unique")
    def _validate_top_level_objects(model: UserConfiguredModel) -> List[ValidationIssue]:
        """Checks names of objects that are not nested."""
        name_and_type_tuples = []
        if model.data_sources:
            for data_source in model.data_sources:
                name_and_type_tuples.append((data_source.name, "data source"))
        if model.materializations:
            for materialization in model.materializations:
                name_and_type_tuples.append((materialization.name, "materialization"))

        name_to_type: Dict[str, str] = {}

        issues = []

        for name, type_ in name_and_type_tuples:
            if name in name_to_type:
                issues.append(
                    ValidationIssue(
                        level=ValidationIssueLevel.FATAL,
                        model_object_reference=OrderedDict(),
                        message=f"Can't use name `{name}` for a {type_} when it was already used for a "
                        f"{name_to_type[name]}",
                    )
                )
            else:
                name_to_type[name] = type_

        if model.metrics:
            metric_names = set()
            for metric in model.metrics:
                if metric.name in metric_names:
                    issues.append(
                        ValidationIssue(
                            level=ValidationIssueLevel.FATAL,
                            model_object_reference=OrderedDict(),
                            message=f"Can't use name `{metric.name}` for a metric when it was already used for a metric",
                        )
                    )
                else:
                    metric_names.add(metric.name)

        if any([x.level == ValidationIssueLevel.FATAL for x in issues]):
            return issues

        for name, _ in name_and_type_tuples:
            issues += UniqueAndValidNameRule.check_valid_name(name)

        return issues

    @staticmethod
    @validate_safely(whats_being_done="running model validation ensuring elements have adequately unique names")
    def validate_model(model: UserConfiguredModel) -> List[ValidationIssue]:  # noqa: D
        issues = []
        issues += UniqueAndValidNameRule._validate_top_level_objects(model=model)

        for data_source in model.data_sources:
            issues += UniqueAndValidNameRule._validate_data_source_elements(data_source=data_source)

        return issues
