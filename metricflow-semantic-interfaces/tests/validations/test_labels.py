from __future__ import annotations

from copy import deepcopy

import pytest
from metricflow_semantic_interfaces.implementations.elements.entity import PydanticEntity
from metricflow_semantic_interfaces.implementations.semantic_manifest import (
    PydanticSemanticManifest,
)
from metricflow_semantic_interfaces.test_utils import (
    find_metric_with,
    find_semantic_model_with,
)
from metricflow_semantic_interfaces.type_enums import EntityType
from metricflow_semantic_interfaces.validations.labels import (
    EntityLabelsRule,
    MetricLabelsRule,
    SemanticModelLabelsRule,
)
from metricflow_semantic_interfaces.validations.semantic_manifest_validator import (
    SemanticManifestValidator,
)
from metricflow_semantic_interfaces.validations.validator_helpers import (
    SemanticManifestValidationException,
)


def test_metric_label_happy_path(  # noqa: D103
    simple_semantic_manifest__with_primary_transforms: PydanticSemanticManifest,
) -> None:
    manifest = deepcopy(simple_semantic_manifest__with_primary_transforms)
    SemanticManifestValidator[PydanticSemanticManifest](
        [MetricLabelsRule[PydanticSemanticManifest]()]
    ).checked_validations(manifest)


def test_duplicate_metric_label(  # noqa: D103
    simple_semantic_manifest__with_primary_transforms: PydanticSemanticManifest,
) -> None:
    manifest = deepcopy(simple_semantic_manifest__with_primary_transforms)
    metric = find_metric_with(manifest, lambda metric: metric.label is not None)
    duplicated_metric, _ = deepcopy(metric)
    duplicated_metric.name = duplicated_metric.name + "_copy"
    manifest.metrics.append(duplicated_metric)
    with pytest.raises(
        SemanticManifestValidationException,
        match=rf"Can't use label `{duplicated_metric.label}` for  metric",
    ):
        SemanticManifestValidator[PydanticSemanticManifest](
            [MetricLabelsRule[PydanticSemanticManifest]()]
        ).checked_validations(manifest)


def test_semantic_model_label_happy_path(  # noqa: D103
    simple_semantic_manifest__with_primary_transforms: PydanticSemanticManifest,
) -> None:
    manifest = deepcopy(simple_semantic_manifest__with_primary_transforms)
    SemanticManifestValidator[PydanticSemanticManifest](
        [SemanticModelLabelsRule[PydanticSemanticManifest]()]
    ).checked_validations(manifest)


def test_semantic_model_with_duplicate_labels(  # noqa: D103
    simple_semantic_manifest__with_primary_transforms: PydanticSemanticManifest,
) -> None:
    manifest = deepcopy(simple_semantic_manifest__with_primary_transforms)
    semantic_model, _ = find_semantic_model_with(manifest, lambda semantic_model: semantic_model.label is not None)
    duplicate = deepcopy(semantic_model)
    duplicate.name = duplicate.name + "_duplicate"
    manifest.semantic_models.append(duplicate)
    with pytest.raises(
        SemanticManifestValidationException,
        match=rf"Can't use label `{semantic_model.label}` for  semantic model",
    ):
        SemanticManifestValidator[PydanticSemanticManifest](
            [SemanticModelLabelsRule[PydanticSemanticManifest]()]
        ).checked_validations(manifest)


def test_semantic_model_with_duplicate_dimension_labels(  # noqa: D103
    simple_semantic_manifest__with_primary_transforms: PydanticSemanticManifest,
) -> None:
    manifest = deepcopy(simple_semantic_manifest__with_primary_transforms)
    semantic_model, _ = find_semantic_model_with(manifest, lambda semantic_model: len(semantic_model.dimensions) >= 2)
    label = "Duplicate Label Name"
    semantic_model.dimensions[0].label = label
    semantic_model.dimensions[1].label = label
    with pytest.raises(
        SemanticManifestValidationException,
        match=rf"Dimension labels must be unique within a semantic model. The label `{label}`",
    ):
        SemanticManifestValidator[PydanticSemanticManifest](
            [SemanticModelLabelsRule[PydanticSemanticManifest]()]
        ).checked_validations(manifest)


def test_semantic_model_with_duplicate_entity_labels(  # noqa: D103
    simple_semantic_manifest__with_primary_transforms: PydanticSemanticManifest,
) -> None:
    manifest = deepcopy(simple_semantic_manifest__with_primary_transforms)
    semantic_model, _ = find_semantic_model_with(manifest, lambda semantic_model: len(semantic_model.entities) >= 2)
    label = "Duplicate Label Name"
    semantic_model.entities[0].label = label
    semantic_model.entities[1].label = label
    with pytest.raises(
        SemanticManifestValidationException,
        match=rf"Entity labels must be unique within a semantic model. The label `{label}`",
    ):
        SemanticManifestValidator[PydanticSemanticManifest](
            [SemanticModelLabelsRule[PydanticSemanticManifest]()]
        ).checked_validations(manifest)


def test_semantic_model_with_duplicate_measure_labels(  # noqa: D103
    simple_semantic_manifest__with_primary_transforms: PydanticSemanticManifest,
) -> None:
    manifest = deepcopy(simple_semantic_manifest__with_primary_transforms)
    semantic_model, _ = find_semantic_model_with(manifest, lambda semantic_model: len(semantic_model.measures) >= 2)
    label = "Duplicate Label Name"
    semantic_model.measures[0].label = label
    semantic_model.measures[1].label = label
    with pytest.raises(
        SemanticManifestValidationException,
        match=rf"Measure labels must be unique within a semantic model. The label `{label}`",
    ):
        SemanticManifestValidator[PydanticSemanticManifest](
            [SemanticModelLabelsRule[PydanticSemanticManifest]()]
        ).checked_validations(manifest)


def test_entity_labels_happy_path(  # noqa: D103
    simple_semantic_manifest__with_primary_transforms: PydanticSemanticManifest,
) -> None:
    manifest = deepcopy(simple_semantic_manifest__with_primary_transforms)
    SemanticManifestValidator[PydanticSemanticManifest](
        [EntityLabelsRule[PydanticSemanticManifest]()]
    ).checked_validations(manifest)


def test_entities_with_same_name_but_different_labels(  # noqa: D103
    simple_semantic_manifest__with_primary_transforms: PydanticSemanticManifest,
) -> None:
    manifest = deepcopy(simple_semantic_manifest__with_primary_transforms)
    entity = PydanticEntity(name="random_entity", type=EntityType.FOREIGN, label="Random Entity")
    entity_conflict = PydanticEntity(name="random_entity", type=EntityType.FOREIGN, label="Random Entity Scoped")
    manifest.semantic_models[0].entities = list(manifest.semantic_models[0].entities) + [entity]
    manifest.semantic_models[1].entities = list(manifest.semantic_models[1].entities) + [entity_conflict]

    with pytest.raises(
        SemanticManifestValidationException,
        match=rf"Entities with the same name must have the same label or the label must be `None`. Entity "
        f"`{entity.name}`",
    ):
        SemanticManifestValidator[PydanticSemanticManifest](
            [EntityLabelsRule[PydanticSemanticManifest]()]
        ).checked_validations(manifest)
