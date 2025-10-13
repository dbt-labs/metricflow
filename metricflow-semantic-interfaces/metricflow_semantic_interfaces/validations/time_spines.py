from __future__ import annotations

from typing import Dict, Generic, List, Sequence, Set

from metricflow_semantic_interfaces.protocols import SemanticManifestT, TimeSpine
from metricflow_semantic_interfaces.type_enums import TimeGranularity
from metricflow_semantic_interfaces.validations.validator_helpers import (
    SemanticManifestValidationRule,
    ValidationIssue,
    ValidationWarning,
    validate_safely,
)


class TimeSpineRule(SemanticManifestValidationRule[SemanticManifestT], Generic[SemanticManifestT]):
    """Checks that time spines are configured properly."""

    @staticmethod
    @validate_safely(whats_being_done="running model validation to ensure that time spines are valid")
    def validate_manifest(semantic_manifest: SemanticManifestT) -> Sequence[ValidationIssue]:
        """Validate time spine configs.

        Note that some validation happens separately in the core parser before building this object:
        - error if no time spine configured and legacy time spine model doeesn't exist
        - error if granularity is missing for primary column
        - error if primary column does not exist in the model
        """
        issues: List[ValidationIssue] = []

        if not semantic_manifest.semantic_models:
            return issues

        time_spines = semantic_manifest.project_configuration.time_spines
        if not time_spines:
            return issues

        # Verify that there is only one time spine per granularity
        time_spines_by_granularity: Dict[TimeGranularity, List[TimeSpine]] = {}
        granularities_with_multiple_time_spines: Set[TimeGranularity] = set()
        for time_spine in time_spines:
            granularity = time_spine.primary_column.time_granularity
            if granularity in time_spines_by_granularity:
                time_spines_by_granularity[granularity].append(time_spine)
            else:
                time_spines_by_granularity[granularity] = [time_spine]
            if len(time_spines_by_granularity[granularity]) > 1:
                granularities_with_multiple_time_spines.add(granularity)

        if granularities_with_multiple_time_spines:
            duplicate_granularity_time_spines: Dict[str, List[str]] = {}
            for granularity in granularities_with_multiple_time_spines:
                duplicate_granularity_time_spines[granularity.name] = [
                    time_spine.node_relation.relation_name for time_spine in time_spines_by_granularity[granularity]
                ]
            issues.append(
                ValidationWarning(
                    message=f"Only one time spine is supported per granularity. Got duplicates: "
                    f"{duplicate_granularity_time_spines}"
                )
            )

        # Warn if there is a time dimension configured with a smaller granularity than the smallest time spine
        dimension_granularities = {
            dimension.type_params.time_granularity
            for semantic_model in semantic_manifest.semantic_models
            for dimension in semantic_model.dimensions
            if dimension.type_params
        }
        smallest_dim_granularity = min(dimension_granularities)
        smallest_time_spine_granularity = min(time_spines_by_granularity.keys())
        if smallest_dim_granularity < smallest_time_spine_granularity:
            issues.append(
                ValidationWarning(
                    message=f"To avoid unexpected query errors, configuring a time spine at or below the smallest time "
                    f"dimension granularity is recommended. Smallest time dimension granularity: "
                    f"{smallest_dim_granularity.name}; Smallest time spine granularity: "
                    f"{smallest_time_spine_granularity}"
                )
            )

        return issues
