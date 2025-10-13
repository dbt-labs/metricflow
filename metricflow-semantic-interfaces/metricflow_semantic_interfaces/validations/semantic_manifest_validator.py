from __future__ import annotations

import copy
import logging
from concurrent.futures import ProcessPoolExecutor, as_completed
from typing import Generic, List, Sequence

from metricflow_semantic_interfaces.protocols import SemanticManifest, SemanticManifestT
from metricflow_semantic_interfaces.validations.agg_time_dimension import (
    AggregationTimeDimensionRule,
)
from metricflow_semantic_interfaces.validations.dimension_const import DimensionConsistencyRule
from metricflow_semantic_interfaces.validations.element_const import ElementConsistencyRule
from metricflow_semantic_interfaces.validations.entities import NaturalEntityConfigurationRule
from metricflow_semantic_interfaces.validations.labels import (
    EntityLabelsRule,
    MetricLabelsRule,
    SemanticModelLabelsRule,
)
from metricflow_semantic_interfaces.validations.measures import (
    CountAggregationExprRule,
    MeasureConstraintAliasesRule,
    MeasuresNonAdditiveDimensionRule,
    MetricMeasuresRule,
    PercentileAggregationRule,
    SemanticModelMeasuresUniqueRule,
)
from metricflow_semantic_interfaces.validations.metrics import (
    ConversionMetricRule,
    CumulativeMetricRule,
    DerivedMetricRule,
)
from metricflow_semantic_interfaces.validations.non_empty import NonEmptyRule
from metricflow_semantic_interfaces.validations.primary_entity import PrimaryEntityRule
from metricflow_semantic_interfaces.validations.reserved_keywords import ReservedKeywordsRule
from metricflow_semantic_interfaces.validations.saved_query import SavedQueryRule
from metricflow_semantic_interfaces.validations.semantic_models import (
    SemanticModelDefaultsRule,
    SemanticModelValidityWindowRule,
)
from metricflow_semantic_interfaces.validations.time_spines import TimeSpineRule
from metricflow_semantic_interfaces.validations.unique_valid_name import (
    PrimaryEntityDimensionPairs,
    UniqueAndValidNameRule,
)
from metricflow_semantic_interfaces.validations.validator_helpers import (
    SemanticManifestValidationException,
    SemanticManifestValidationResults,
    SemanticManifestValidationRule,
)
from metricflow_semantic_interfaces.validations.where_filters import WhereFiltersAreParseable

logger = logging.getLogger(__name__)


def _validate_manifest_with_one_rule(
    validation_rule: SemanticManifestValidationRule, semantic_manifest: SemanticManifest
) -> str:
    """Helper function to run a single validation rule on a semantic mode.

    Result is returned as a serialized object as there are pickling issues with SemanticManifestValidationResults.
    """
    return SemanticManifestValidationResults.from_issues_sequence(
        validation_rule.validate_manifest(semantic_manifest)
    ).json()


class SemanticManifestValidator(Generic[SemanticManifestT]):
    """A Validator that acts on SemanticManifest."""

    DEFAULT_RULES: Sequence[SemanticManifestValidationRule[SemanticManifestT]] = (
        PercentileAggregationRule[SemanticManifestT](),
        DerivedMetricRule[SemanticManifestT](),
        CountAggregationExprRule[SemanticManifestT](),
        SemanticModelMeasuresUniqueRule[SemanticManifestT](),
        SemanticModelValidityWindowRule[SemanticManifestT](),
        DimensionConsistencyRule[SemanticManifestT](),
        ElementConsistencyRule[SemanticManifestT](),
        NaturalEntityConfigurationRule[SemanticManifestT](),
        MeasureConstraintAliasesRule[SemanticManifestT](),
        MetricMeasuresRule[SemanticManifestT](),
        CumulativeMetricRule[SemanticManifestT](),
        NonEmptyRule[SemanticManifestT](),
        UniqueAndValidNameRule[SemanticManifestT](),
        AggregationTimeDimensionRule[SemanticManifestT](),
        ReservedKeywordsRule[SemanticManifestT](),
        MeasuresNonAdditiveDimensionRule[SemanticManifestT](),
        SemanticModelDefaultsRule[SemanticManifestT](),
        PrimaryEntityRule[SemanticManifestT](),
        PrimaryEntityDimensionPairs[SemanticManifestT](),
        WhereFiltersAreParseable[SemanticManifestT](),
        SavedQueryRule[SemanticManifestT](),
        MetricLabelsRule[SemanticManifestT](),
        SemanticModelLabelsRule[SemanticManifestT](),
        EntityLabelsRule[SemanticManifestT](),
        ConversionMetricRule[SemanticManifestT](),
        TimeSpineRule[SemanticManifestT](),
    )

    def __init__(
        self, rules: Sequence[SemanticManifestValidationRule[SemanticManifestT]] = DEFAULT_RULES, max_workers: int = 1
    ) -> None:
        """Constructor.

        Args:
            rules: List of validation rules to run. Defaults to DEFAULT_RULES
            max_workers: sets the max number of rules to run against the semantic_manifest concurrently
        """
        # Raises an error if 'rules' is an empty sequence or None
        if not rules:
            raise ValueError(
                "SemanticManifestValidator 'rules' must be a sequence with at least one SemanticManifestValidationRule."
            )

        self._rules = rules
        self._executor = ProcessPoolExecutor(max_workers=max_workers)

    def validate_semantic_manifest(
        self, semantic_manifest: SemanticManifestT, multi_process: bool = False
    ) -> SemanticManifestValidationResults:
        """Validate a manifest according to configured rules."""
        if multi_process:
            return self._validate_multi_process(semantic_manifest=semantic_manifest)
        else:
            return self._validate_sync(semantic_manifest=semantic_manifest)

    def _validate_sync(self, semantic_manifest: SemanticManifestT) -> SemanticManifestValidationResults:  # noqa: D102
        results: List[SemanticManifestValidationResults] = []

        for rule in self._rules:
            issues = rule.validate_manifest(semantic_manifest=semantic_manifest)
            results.append(SemanticManifestValidationResults.from_issues_sequence(issues))

        return SemanticManifestValidationResults.merge(results)

    def _validate_multi_process(  # noqa: D102
        self, semantic_manifest: SemanticManifestT
    ) -> SemanticManifestValidationResults:
        results: List[SemanticManifestValidationResults] = []

        futures = [
            self._executor.submit(_validate_manifest_with_one_rule, validation_rule, semantic_manifest)
            for validation_rule in self._rules
        ]
        for future in as_completed(futures):
            res = future.result()
            result = SemanticManifestValidationResults.parse_raw(res)
            results.append(result)

        return SemanticManifestValidationResults.merge(results)

    def checked_validations(self, semantic_manifest: SemanticManifestT) -> None:
        """Similar to validate(), but throws an exception if validation fails."""
        semantic_manifest_copy = copy.deepcopy(semantic_manifest)
        semantic_manifest_issues = self.validate_semantic_manifest(semantic_manifest_copy)
        if semantic_manifest_issues.has_blocking_issues:
            raise SemanticManifestValidationException(issues=tuple(semantic_manifest_issues.all_issues))
