from concurrent.futures import ProcessPoolExecutor, as_completed
import copy
import logging
from typing import List, Sequence

from dbt_semantic_interfaces.objects.semantic_manifest import SemanticManifest
from dbt_semantic_interfaces.validations.agg_time_dimension import AggregationTimeDimensionRule
from dbt_semantic_interfaces.validations.semantic_models import (
    SemanticModelTimeDimensionWarningsRule,
    SemanticModelValidityWindowRule,
)
from dbt_semantic_interfaces.validations.dimension_const import DimensionConsistencyRule
from dbt_semantic_interfaces.validations.element_const import ElementConsistencyRule
from dbt_semantic_interfaces.validations.entities import (
    NaturalEntityConfigurationRule,
    OnePrimaryEntityPerSemanticModelRule,
)
from dbt_semantic_interfaces.validations.measures import (
    PercentileAggregationRule,
    CountAggregationExprRule,
    SemanticModelMeasuresUniqueRule,
    MeasureConstraintAliasesRule,
    MetricMeasuresRule,
    MeasuresNonAdditiveDimensionRule,
)
from dbt_semantic_interfaces.validations.metrics import CumulativeMetricRule, DerivedMetricRule
from dbt_semantic_interfaces.validations.non_empty import NonEmptyRule
from dbt_semantic_interfaces.validations.reserved_keywords import ReservedKeywordsRule
from dbt_semantic_interfaces.validations.unique_valid_name import UniqueAndValidNameRule
from dbt_semantic_interfaces.validations.validator_helpers import (
    ModelValidationResults,
    ModelValidationRule,
    ModelValidationException,
)

logger = logging.getLogger(__name__)


class ModelValidator:
    """A Validator that acts on SemanticManifest"""

    DEFAULT_RULES = (
        PercentileAggregationRule(),
        DerivedMetricRule(),
        CountAggregationExprRule(),
        SemanticModelMeasuresUniqueRule(),
        SemanticModelTimeDimensionWarningsRule(),
        SemanticModelValidityWindowRule(),
        DimensionConsistencyRule(),
        ElementConsistencyRule(),
        NaturalEntityConfigurationRule(),
        OnePrimaryEntityPerSemanticModelRule(),
        MeasureConstraintAliasesRule(),
        MetricMeasuresRule(),
        CumulativeMetricRule(),
        NonEmptyRule(),
        UniqueAndValidNameRule(),
        AggregationTimeDimensionRule(),
        ReservedKeywordsRule(),
        MeasuresNonAdditiveDimensionRule(),
    )

    def __init__(self, rules: Sequence[ModelValidationRule] = DEFAULT_RULES, max_workers: int = 1) -> None:
        """Constructor.

        Args:
            rules: List of validation rules to run. Defaults to DEFAULT_RULES
            max_workers: sets the max number of rules to run against the model concurrently
        """

        # Raises an error if 'rules' is an empty sequence or None
        if not rules:
            raise ValueError("ModelValidator 'rules' must be a sequence with at least one ModelValidationRule.")

        self._rules = rules
        self._executor = ProcessPoolExecutor(max_workers=max_workers)

    def validate_model(self, model: SemanticManifest) -> ModelValidationResults:
        """Validate a model according to configured rules."""
        serialized_model = model.json()

        results: List[ModelValidationResults] = []

        futures = [
            self._executor.submit(validation_rule.validate_model_serialized_for_multiprocessing, serialized_model)
            for validation_rule in self._rules
        ]
        for future in as_completed(futures):
            res = future.result()
            result = ModelValidationResults.parse_raw(res)
            results.append(result)

        return ModelValidationResults.merge(results)

    def checked_validations(self, model: SemanticManifest) -> None:
        """Similar to validate(), but throws an exception if validation fails."""
        model_copy = copy.deepcopy(model)
        model_issues = self.validate_model(model_copy)
        if model_issues.has_blocking_issues:
            raise ModelValidationException(issues=tuple(model_issues.all_issues))
