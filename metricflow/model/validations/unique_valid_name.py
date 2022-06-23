import re
from typing import Dict, Tuple, List, Optional

from metricflow.model.objects.data_source import DataSource
from metricflow.model.objects.user_configured_model import UserConfiguredModel
from metricflow.model.validations.validator_helpers import (
    DataSourceContext,
    DimensionContext,
    IdentifierContext,
    MaterializationContext,
    MeasureContext,
    MetricContext,
    ModelValidationRule,
    ValidationContext,
    ValidationError,
    ValidationFatal,
    ValidationIssueLevel,
    ValidationIssueType,
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
    def check_valid_name(  # noqa: D
        name: str, context: Optional[ValidationContext] = None
    ) -> List[ValidationIssueType]:
        issues: List[ValidationIssueType] = []

        if not UniqueAndValidNameRule.NAME_REGEX.match(name):
            issues.append(
                ValidationError(
                    context=context,
                    message=f"Invalid name `{name}` - names should only consist of lower case letters, numbers, "
                    f"and underscores. In addition, names should start with a lower case letter, and should not end "
                    f"with an underscore, and they must be at least 2 characters long.",
                )
            )
        if name.upper() in TimeGranularity.list_names():
            issues.append(
                ValidationError(
                    context=context,
                    message=f"Invalid name `{name}` - names cannot match reserved time granularity keywords "
                    f"({TimeGranularity.list_names()})",
                )
            )
        return issues

    @staticmethod
    @validate_safely(whats_being_done="checking data source sub element names are unique")
    def _validate_data_source_elements(data_source: DataSource) -> List[ValidationIssueType]:
        issues: List[ValidationIssueType] = []
        element_info_tuples: List[Tuple[ElementReference, str, ValidationContext]] = []

        if data_source.measures:
            for measure in data_source.measures:
                element_info_tuples.append(
                    (
                        measure.name,
                        "measure",
                        MeasureContext(
                            file_name=data_source.metadata.file_slice.filename if data_source.metadata else None,
                            line_number=data_source.metadata.file_slice.start_line_number
                            if data_source.metadata
                            else None,
                            data_source_name=data_source.name,
                            measure_name=measure.name.element_name,
                        ),
                    )
                )
        if data_source.identifiers:
            for identifier in data_source.identifiers:
                element_info_tuples.append(
                    (
                        identifier.name,
                        "identifier",
                        IdentifierContext(
                            file_name=data_source.metadata.file_slice.filename if data_source.metadata else None,
                            line_number=data_source.metadata.file_slice.start_line_number
                            if data_source.metadata
                            else None,
                            data_source_name=data_source.name,
                            identifier_name=identifier.name.element_name,
                        ),
                    )
                )
        if data_source.dimensions:
            for dimension in data_source.dimensions:
                element_info_tuples.append(
                    (
                        dimension.name,
                        "dimension",
                        DimensionContext(
                            file_name=data_source.metadata.file_slice.filename if data_source.metadata else None,
                            line_number=data_source.metadata.file_slice.start_line_number
                            if data_source.metadata
                            else None,
                            data_source_name=data_source.name,
                            dimension_name=dimension.name.element_name,
                        ),
                    )
                )

        name_to_type: Dict[ElementReference, str] = {}

        for name, _type, context in element_info_tuples:
            if name in name_to_type:
                issues.append(
                    ValidationFatal(
                        context=context,
                        message=f"In data source `{data_source.name}`, can't use name `{name.element_name}` for a "
                        f"{_type} when it was already used for a {name_to_type[name]}",
                    )
                )
            else:
                name_to_type[name] = _type

        for name, _, context in element_info_tuples:
            issues += UniqueAndValidNameRule.check_valid_name(name=name.element_name, context=context)

        return issues

    @staticmethod
    @validate_safely(whats_being_done="checking model top level element names are sufficiently unique")
    def _validate_top_level_objects(model: UserConfiguredModel) -> List[ValidationIssueType]:
        """Checks names of objects that are not nested."""
        object_info_tuples = []
        if model.data_sources:
            for data_source in model.data_sources:
                object_info_tuples.append(
                    (
                        data_source.name,
                        "data source",
                        DataSourceContext(
                            file_name=data_source.metadata.file_slice.filename if data_source.metadata else None,
                            line_number=data_source.metadata.file_slice.start_line_number
                            if data_source.metadata
                            else None,
                            data_source_name=data_source.name,
                        ),
                    )
                )
        if model.materializations:
            for materialization in model.materializations:
                object_info_tuples.append(
                    (
                        materialization.name,
                        "materialization",
                        MaterializationContext(
                            file_name=materialization.metadata.file_slice.filename
                            if materialization.metadata
                            else None,
                            line_number=materialization.metadata.file_slice.start_line_number
                            if materialization.metadata
                            else None,
                            materialization_name=materialization.name,
                        ),
                    )
                )

        name_to_type: Dict[str, str] = {}

        issues: List[ValidationIssueType] = []

        for name, type_, context in object_info_tuples:
            if name in name_to_type:
                issues.append(
                    ValidationFatal(
                        context=context,
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
                        ValidationFatal(
                            context=MetricContext(
                                file_name=metric.metadata.file_slice.filename if metric.metadata else None,
                                line_number=metric.metadata.file_slice.start_line_number if metric.metadata else None,
                                metric_name=metric.name,
                            ),
                            message=f"Can't use name `{metric.name}` for a metric when it was already used for a metric",
                        )
                    )
                else:
                    metric_names.add(metric.name)

        if any([x.level == ValidationIssueLevel.FATAL for x in issues]):
            return issues

        for name, _, context in object_info_tuples:
            issues += UniqueAndValidNameRule.check_valid_name(name=name, context=context)

        return issues

    @staticmethod
    @validate_safely(whats_being_done="running model validation ensuring elements have adequately unique names")
    def validate_model(model: UserConfiguredModel) -> List[ValidationIssueType]:  # noqa: D
        issues = []
        issues += UniqueAndValidNameRule._validate_top_level_objects(model=model)

        for data_source in model.data_sources:
            issues += UniqueAndValidNameRule._validate_data_source_elements(data_source=data_source)

        return issues
