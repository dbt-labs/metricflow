from __future__ import annotations

from typing import Dict, Generic, List, Optional, Sequence, Set

from metricflow_semantic_interfaces.protocols import SemanticManifestT, TimeSpine
from metricflow_semantic_interfaces.type_enums import DimensionType, TimeGranularity
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

        # Only dimensions used as an `agg_time_dimension` can trigger a time-spine join, so only they can hit a real
        # granularity gap with the time spine. See https://github.com/dbt-labs/metricflow/issues/2115.
        default_agg_time_dimension_by_model: Dict[str, Optional[str]] = {
            semantic_model.name: (
                semantic_model.defaults.agg_time_dimension if semantic_model.defaults is not None else None
            )
            for semantic_model in semantic_manifest.semantic_models
        }
        agg_time_dimension_names_by_model: Dict[str, Set[str]] = {
            semantic_model.name: set() for semantic_model in semantic_manifest.semantic_models
        }
        for semantic_model in semantic_manifest.semantic_models:
            default_agg_time_dimension = default_agg_time_dimension_by_model[semantic_model.name]
            for measure in semantic_model.measures:
                agg_time_dimension_name = measure.agg_time_dimension or default_agg_time_dimension
                if agg_time_dimension_name is not None:
                    agg_time_dimension_names_by_model[semantic_model.name].add(agg_time_dimension_name)
        # Simple metrics defined without a measure carry their own `agg_time_dimension` referencing a model.
        for metric in semantic_manifest.metrics:
            agg_params = metric.type_params.metric_aggregation_params
            if agg_params is None:
                continue
            agg_time_dimension_name = agg_params.agg_time_dimension or default_agg_time_dimension_by_model.get(
                agg_params.semantic_model
            )
            if agg_time_dimension_name is not None:
                agg_time_dimension_names_by_model.setdefault(agg_params.semantic_model, set()).add(
                    agg_time_dimension_name
                )

        # `time_dimension_granularities` tracks all time dimensions so the "no time dimensions configured" warning
        # still fires, while the granularity-gap check below only considers agg_time_dimensions.
        time_dimension_granularities: Set[TimeGranularity] = set()
        agg_time_dimension_granularities: Set[TimeGranularity] = set()
        for semantic_model in semantic_manifest.semantic_models:
            agg_time_dimension_names = agg_time_dimension_names_by_model[semantic_model.name]
            for dimension in semantic_model.dimensions:
                if dimension.type is not DimensionType.TIME or dimension.type_params is None:
                    continue
                granularity = dimension.type_params.time_granularity
                time_dimension_granularities.add(granularity)
                if dimension.name in agg_time_dimension_names:
                    agg_time_dimension_granularities.add(granularity)

        if len(time_dimension_granularities) == 0:
            issues.append(
                ValidationWarning(
                    message="No time dimensions configured. To avoid unexpected query errors, configuring a "
                    "time spine at or below the smallest time dimension granularity is recommended."
                )
            )
            return issues

        if agg_time_dimension_granularities:
            smallest_dim_granularity = min(agg_time_dimension_granularities)
            smallest_time_spine_granularity = min(time_spines_by_granularity.keys())
            if smallest_dim_granularity < smallest_time_spine_granularity:
                issues.append(
                    ValidationWarning(
                        message=f"To avoid unexpected query errors, configuring a time spine at or below the smallest "
                        f"time dimension granularity is recommended. Smallest time dimension granularity: "
                        f"{smallest_dim_granularity.name}; Smallest time spine granularity: "
                        f"{smallest_time_spine_granularity}"
                    )
                )

        return issues
