from __future__ import annotations

import logging
from collections import defaultdict
from dataclasses import dataclass
from typing import DefaultDict, Dict, Generic, List, Sequence

from metricflow_semantic_interfaces.protocols import Metric, SemanticManifestT, SemanticModel
from metricflow_semantic_interfaces.validations.validator_helpers import (
    FileContext,
    SemanticManifestValidationRule,
    ValidationError,
    ValidationIssue,
    validate_safely,
)

logger = logging.getLogger(__name__)


class MetricLabelsRule(SemanticManifestValidationRule[SemanticManifestT], Generic[SemanticManifestT]):
    """Checks that the labels are unique across metrics."""

    @staticmethod
    @validate_safely("Checking that a metric has a unique label")
    def _check_metric(metric: Metric, existing_labels: Dict[str, str]) -> Sequence[ValidationIssue]:  # noqa: D102
        if metric.label in existing_labels:
            return (
                ValidationError(
                    context=FileContext.from_metadata(metric.metadata),
                    message=f"Can't use label `{metric.label}` for  metric `{metric.name}` "
                    f"as it's already used for metric `{existing_labels[metric.label]}`",
                ),
            )
        elif metric.label is not None:
            existing_labels[metric.label] = metric.name

        return ()

    @staticmethod
    @validate_safely("Checking labels are unique across metrics")
    def validate_manifest(semantic_manifest: SemanticManifestT) -> Sequence[ValidationIssue]:  # noqa: D102
        issues: List[ValidationIssue] = []
        labels_to_metrics: Dict[str, str] = {}
        for metric in semantic_manifest.metrics:
            issues += MetricLabelsRule._check_metric(metric=metric, existing_labels=labels_to_metrics)

        return issues


class SemanticModelLabelsRule(SemanticManifestValidationRule[SemanticManifestT], Generic[SemanticManifestT]):
    """Checks that the labels are unique across semantic models."""

    @staticmethod
    @validate_safely("checking that a semantic model has a unique label")
    def _check_semantic_model(
        semantic_model: SemanticModel, existing_labels: Dict[str, str]
    ) -> Sequence[ValidationIssue]:  # noqa: D101
        if semantic_model.label in existing_labels:
            return (
                ValidationError(
                    context=FileContext.from_metadata(semantic_model.metadata),
                    message=f"Can't use label `{semantic_model.label}` for  semantic model `{semantic_model.name}` "
                    f"as it's already used for semantic model `{existing_labels[semantic_model.label]}`",
                ),
            )
        elif semantic_model.label is not None:
            existing_labels[semantic_model.label] = semantic_model.name

        return ()

    @staticmethod
    @validate_safely("checking that a semantic model's dimension labels are unique within itself")
    def _check_semantic_model_dimensions(semantic_model: SemanticModel) -> Sequence[ValidationIssue]:
        issues: List[ValidationIssue] = []
        label_counts: DefaultDict[str, int] = defaultdict(lambda: 0)
        for dimension in semantic_model.dimensions:
            if dimension.label is not None:
                label_counts[dimension.label] = label_counts[dimension.label] + 1

        for label, count in label_counts.items():
            if count > 1:
                issues.append(
                    ValidationError(
                        context=FileContext.from_metadata(semantic_model.metadata),
                        message=f"Dimension labels must be unique within a semantic model. The label `{label}` was "
                        f"used for {count} dimensions on semantic model `{semantic_model.name}",
                    )
                )

        return issues

    @staticmethod
    @validate_safely("checking that a semantic model's entity labels are unique within itself")
    def _check_semantic_model_entities(semantic_model: SemanticModel) -> Sequence[ValidationIssue]:
        issues: List[ValidationIssue] = []
        label_counts: DefaultDict[str, int] = defaultdict(lambda: 0)
        for entity in semantic_model.entities:
            if entity.label is not None:
                label_counts[entity.label] = label_counts[entity.label] + 1

        for label, count in label_counts.items():
            if count > 1:
                issues.append(
                    ValidationError(
                        context=FileContext.from_metadata(semantic_model.metadata),
                        message=f"Entity labels must be unique within a semantic model. The label `{label}` was used "
                        f"for {count} entities on semantic model `{semantic_model.name}",
                    )
                )

        return issues

    @staticmethod
    @validate_safely("checking that a semantic model's measure labels are unique within itself")
    def _check_semantic_model_measures(semantic_model: SemanticModel) -> Sequence[ValidationIssue]:
        issues: List[ValidationIssue] = []
        label_counts: DefaultDict[str, int] = defaultdict(lambda: 0)
        for measure in semantic_model.measures:
            if measure.label is not None:
                label_counts[measure.label] = label_counts[measure.label] + 1

        for label, count in label_counts.items():
            if count > 1:
                issues.append(
                    ValidationError(
                        context=FileContext.from_metadata(semantic_model.metadata),
                        message=f"Measure labels must be unique within a semantic model. The label `{label}` was used "
                        f"for {count} measures on semantic model `{semantic_model.name}",
                    )
                )

        return issues

    @staticmethod
    @validate_safely("checking labels on semantic models and their sub objects")
    def validate_manifest(semantic_manifest: SemanticManifestT) -> Sequence[ValidationIssue]:  # noqa: D102
        issues: List[ValidationIssue] = []
        labels_to_semantic_models: Dict[str, str] = {}
        for semantic_model in semantic_manifest.semantic_models:
            issues += SemanticModelLabelsRule._check_semantic_model(
                semantic_model=semantic_model, existing_labels=labels_to_semantic_models
            )
            issues += SemanticModelLabelsRule._check_semantic_model_dimensions(semantic_model=semantic_model)
            issues += SemanticModelLabelsRule._check_semantic_model_entities(semantic_model=semantic_model)
            issues += SemanticModelLabelsRule._check_semantic_model_measures(semantic_model=semantic_model)

        return issues


class EntityLabelsRule(SemanticManifestValidationRule[SemanticManifestT], Generic[SemanticManifestT]):
    """Checks that the entity labels are consistent across semantic models."""

    @dataclass
    class EntityInfo:
        """Class used in validating of entity labels across semantic models."""

        semantic_model_name: str
        label: str

    @staticmethod
    @validate_safely("Checking entities of the same name have the same label (or None for the label)")
    def _check_semantic_model_entities(
        semantic_model: SemanticModel, existing_labels: Dict[str, EntityInfo]
    ) -> Sequence[ValidationIssue]:  # noqa: D103
        issues: List[ValidationIssue] = []
        for entity in semantic_model.entities:
            if entity.label is not None:
                if entity.name not in existing_labels:
                    existing_labels[entity.name] = EntityLabelsRule.EntityInfo(
                        semantic_model_name=semantic_model.name, label=entity.label
                    )
                elif existing_labels[entity.name].label != entity.label:
                    issues.append(
                        ValidationError(
                            context=FileContext.from_metadata(semantic_model.metadata),
                            message="Entities with the same name must have the same label or the label must be "
                            f"`None`. Entity `{entity.name}` on semantic model `{semantic_model.name}` has label "
                            f"`{entity.label}` but the same entity on semantic model "
                            f"`{existing_labels[entity.name].semantic_model_name}`",
                        )
                    )

        return issues

    @staticmethod
    @validate_safely("Checking entity labels are consistent across semantic models")
    def validate_manifest(semantic_manifest: SemanticManifestT) -> Sequence[ValidationIssue]:  # noqa: D102
        issues: List[ValidationIssue] = []
        entity_label_map: Dict[str, EntityLabelsRule.EntityInfo] = {}

        for semantic_model in semantic_manifest.semantic_models:
            issues += EntityLabelsRule._check_semantic_model_entities(
                semantic_model=semantic_model, existing_labels=entity_label_map
            )

        return issues
