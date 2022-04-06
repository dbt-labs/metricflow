import copy
import logging
from typing import List

from metricflow.model.objects.user_configured_model import UserConfiguredModel
from metricflow.model.parsing.dir_to_model import ModelBuildResult
from metricflow.model.validations.common_identifiers import CommonIdentifiersRule
from metricflow.model.validations.data_sources import (
    DataSourceMeasuresUniqueRule,
    DataSourceTimeDimensionWarningsRule,
)
from metricflow.model.validations.dimension_const import DimensionConsistencyRule
from metricflow.model.validations.element_const import ElementConsistencyRule
from metricflow.model.validations.identifiers import IdentifierConfigRule, OnePrimaryIdentifierPerDataSourceRule
from metricflow.model.validations.metrics import MetricMeasuresRule, CumulativeMetricRule
from metricflow.model.validations.non_empty import NonEmptyRule
from metricflow.model.validations.unique_valid_name import UniqueAndValidNameRule
from metricflow.model.validations.validator_helpers import (
    ValidationIssueType,
    ModelValidationRule,
    ValidationIssueLevel,
    ModelValidationException,
)

logger = logging.getLogger(__name__)


class ModelValidator:
    """A Validator that acts on UserConfiguredModel"""

    VALIDATION_RULES: List[ModelValidationRule] = [
        CommonIdentifiersRule(),
        DataSourceMeasuresUniqueRule(),
        DataSourceTimeDimensionWarningsRule(),
        DimensionConsistencyRule(),
        ElementConsistencyRule(),
        IdentifierConfigRule(),
        OnePrimaryIdentifierPerDataSourceRule(),
        MetricMeasuresRule(),
        CumulativeMetricRule(),
        NonEmptyRule(),
        UniqueAndValidNameRule(),
    ]

    @staticmethod
    def validate_model(model: UserConfiguredModel) -> ModelBuildResult:
        """Validate a model according to configured rules."""
        model_copy = copy.deepcopy(model)

        issues: List[ValidationIssueType] = []
        for validation_rule in ModelValidator.VALIDATION_RULES:
            issues.extend(validation_rule.validate_model(model_copy))
            # If there are any fatal errors, stop the validation process.
            if any([x.level == ValidationIssueLevel.FATAL for x in issues]):
                return ModelBuildResult(model=model_copy, issues=tuple(issues))

        # If there are any errors, don't run any transforms and return the issues found.
        if any([x.level == ValidationIssueLevel.ERROR for x in issues]):
            return ModelBuildResult(model=model_copy, issues=tuple(issues))

        return ModelBuildResult(model=model_copy, issues=tuple(issues))

    @staticmethod
    def checked_validations(model: UserConfiguredModel) -> UserConfiguredModel:  # chTODO: remember checked_build
        """Similar to validate(), but throws an exception if validation fails."""
        model_copy = copy.deepcopy(model)
        build_result = ModelValidator.validate_model(model_copy)
        if build_result.issues is not None:
            if any(
                [
                    x.level == ValidationIssueLevel.WARNING or x.level == ValidationIssueLevel.FUTURE_ERROR
                    for x in build_result.issues
                ]
            ):
                issues_str = "\n".join([x.as_readable_str() for x in build_result.issues])
                logger.warning(f"Found some validation warnings in the model:\n{issues_str}")
            if any(
                [
                    x.level == ValidationIssueLevel.ERROR or x.level == ValidationIssueLevel.FATAL
                    for x in build_result.issues
                ]
            ):
                raise ModelValidationException(issues=build_result.issues)
        return model

    # chTODO: This is commented out in case during migration I learn I need it. If that happens, I'll move it
    # appropriately. Before the final migration of validations, this long comment will disappear from here.

    # def __init__(self, model: UserConfiguredModel, _called_from_build: bool = False):
    #     """Create a validated and transformed model from the given input model.
    #
    #     This shouldn't be called directly. Trying to make a private constructor by using _called_from_build.
    #     """
    #
    #     if not _called_from_build:
    #         raise RuntimeError("This should only be called from *build methods.")
    #     # Following sections depend on validations above being run, so no checks are included.
    #     # Stores the names of top level objects (like data sources, and metrics, that should be unique)
    #     self._top_level_object_names = set()
    #     if model.data_sources:
    #         for data_source in model.data_sources:
    #             self._top_level_object_names.add(data_source.name)
    #     if model.materializations:
    #         for materialization in model.materializations:
    #             self._top_level_object_names.add(materialization.name)
    #
    #     self._primary_time_dimension_name: DimensionSpec
    #     # Store dimension invariants
    #     self._dimension_invariants: Dict[DimensionReference, DimensionInvariants] = {}
    #     for data_source in model.data_sources:
    #         dimensions = data_source.dimensions
    #         for dimension in dimensions or []:
    #             self._dimension_invariants[dimension.name] = DimensionInvariants(
    #                 dimension.type, dimension.is_partition or False
    #             )
    #             if (
    #                 dimension.type == DimensionType.TIME
    #                 and dimension.type_params
    #                 and dimension.type_params.is_primary is True
    #             ):
    #                 self._primary_time_dimension_name = dimension.name
    #
    #     # Store the element types
    #     self._element_types: Dict[str, ModelObjectType] = {}
    #     for data_source in model.data_sources:
    #         if data_source.measures:
    #             for measure in data_source.measures:
    #                 self._element_types[measure.name.element_name] = ModelObjectType.MEASURE
    #         if data_source.dimensions:
    #             for dimension in data_source.dimensions:
    #                 self._element_types[dimension.name.element_name] = ModelObjectType.DIMENSION
    #         if data_source.identifiers:
    #             for identifier in data_source.identifiers:
    #                 self._element_types[identifier.name.element_name] = ModelObjectType.IDENTIFIER
    #
    #     super().__init__(
    #         data_sources=model.data_sources,
    #         materializations=model.materializations,
    #         metrics=model.metrics,
    #         derived_group_by_elements=model.derived_group_by_elements,
    #     )
