import copy
import logging
from typing import List, Sequence

from metricflow.model.objects.user_configured_model import UserConfiguredModel
from metricflow.model.parsing.dir_to_model import ModelBuildResult
from metricflow.model.validations.data_sources import (
    DataSourceMeasuresUniqueRule,
    DataSourceTimeDimensionWarningsRule,
)
from metricflow.model.validations.dimension_const import DimensionConsistencyRule
from metricflow.model.validations.element_const import ElementConsistencyRule
from metricflow.model.validations.identifiers import (
    IdentifierConfigRule,
    OnePrimaryIdentifierPerDataSourceRule,
    IdentifierConsistencyRule,
)
from metricflow.model.validations.materializations import ValidMaterializationRule
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

    DEFAULT_RULES = (
        DataSourceMeasuresUniqueRule(),
        DataSourceTimeDimensionWarningsRule(),
        DimensionConsistencyRule(),
        ElementConsistencyRule(),
        IdentifierConfigRule(),
        IdentifierConsistencyRule(),
        OnePrimaryIdentifierPerDataSourceRule(),
        MetricMeasuresRule(),
        CumulativeMetricRule(),
        NonEmptyRule(),
        UniqueAndValidNameRule(),
        ValidMaterializationRule(),
    )

    def __init__(self, rules: Sequence[ModelValidationRule] = DEFAULT_RULES) -> None:
        """Constructor.

        Args:
            rules: List of validation rules to run. Defaults to DEFAULT_RULES
        """

        # Raises an error if 'rules' is an empty sequence or None
        if not rules:
            raise ValueError("ModelValidator 'rules' must be a sequence with at least one ModelValidationRule.")

        self._rules = rules

    def validate_model(self, model: UserConfiguredModel) -> ModelBuildResult:
        """Validate a model according to configured rules."""
        model_copy = copy.deepcopy(model)

        issues: List[ValidationIssueType] = []
        for validation_rule in self._rules:
            issues.extend(validation_rule.validate_model(model_copy))
            # If there are any fatal errors, stop the validation process.
            if any([x.level == ValidationIssueLevel.FATAL for x in issues]):
                break

        return ModelBuildResult(model=model_copy, issues=tuple(issues))

    def checked_validations(self, model: UserConfiguredModel) -> UserConfiguredModel:  # chTODO: remember checked_build
        """Similar to validate(), but throws an exception if validation fails."""
        model_copy = copy.deepcopy(model)
        build_result = self.validate_model(model_copy)
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
