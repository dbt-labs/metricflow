from __future__ import annotations

import logging
from copy import deepcopy

from metricflow_semantic_interfaces.implementations.semantic_manifest import (
    PydanticSemanticManifest,
)
from metricflow_semantic_interfaces.test_utils import find_semantic_model_with
from metricflow_semantic_interfaces.validations.primary_entity import PrimaryEntityRule
from metricflow_semantic_interfaces.validations.semantic_manifest_validator import (
    SemanticManifestValidator,
)

logger = logging.getLogger(__name__)


def test_primary_entity_not_set(simple_semantic_manifest: PydanticSemanticManifest) -> None:  # noqa: D103
    semantic_manifest_copy = deepcopy(simple_semantic_manifest)
    bookings_source_model, _ = find_semantic_model_with(
        semantic_manifest_copy,
        lambda semantic_model: semantic_model.name == "bookings_source",
    )

    bookings_source_model.primary_entity = None

    model_validator = SemanticManifestValidator[PydanticSemanticManifest]([PrimaryEntityRule()])

    errors = model_validator.validate_semantic_manifest(semantic_manifest_copy).errors
    assert len(errors) == 1
    assert errors[0].message.find("does not define a primary entity") != -1


def test_primary_entity_conflict(simple_semantic_manifest: PydanticSemanticManifest) -> None:  # noqa: D103
    semantic_manifest_copy = deepcopy(simple_semantic_manifest)
    listings_latest_model, _ = find_semantic_model_with(
        semantic_manifest_copy,
        lambda semantic_model: semantic_model.name == "listings_latest",
    )

    listings_latest_model.primary_entity = "conflicting_entity"

    model_validator = SemanticManifestValidator[PydanticSemanticManifest]([PrimaryEntityRule()])
    errors = model_validator.validate_semantic_manifest(semantic_manifest_copy).errors
    assert len(errors) == 1
    assert errors[0].message.find("Both should not be present in the model.") != -1
