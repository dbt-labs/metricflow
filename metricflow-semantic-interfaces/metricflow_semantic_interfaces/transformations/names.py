from __future__ import annotations

import logging

from typing_extensions import override

from metricflow_semantic_interfaces.implementations.semantic_manifest import (
    PydanticSemanticManifest,
)
from metricflow_semantic_interfaces.implementations.semantic_model import PydanticSemanticModel
from metricflow_semantic_interfaces.protocols import ProtocolHint
from metricflow_semantic_interfaces.transformations.transform_rule import (
    SemanticManifestTransformRule,
)

logger = logging.getLogger(__name__)


class LowerCaseNamesRule(ProtocolHint[SemanticManifestTransformRule[PydanticSemanticManifest]]):
    """Lowercases the names of both top level objects and semantic model elements in a model."""

    @override
    def _implements_protocol(self) -> SemanticManifestTransformRule[PydanticSemanticManifest]:  # noqa: D102
        return self

    @staticmethod
    def transform_model(semantic_manifest: PydanticSemanticManifest) -> PydanticSemanticManifest:  # noqa: D102
        LowerCaseNamesRule._lowercase_top_level_objects(semantic_manifest)
        for semantic_model in semantic_manifest.semantic_models:
            LowerCaseNamesRule._lowercase_semantic_model_elements(semantic_model)

        return semantic_manifest

    @staticmethod
    def _lowercase_semantic_model_elements(semantic_model: PydanticSemanticModel) -> None:
        """Lowercases the names of semantic model elements."""
        if semantic_model.measures:
            for measure in semantic_model.measures:
                measure.name = measure.name.lower()
        if semantic_model.entities:
            for entity in semantic_model.entities:
                entity.name = entity.name.lower()
        if semantic_model.dimensions:
            for dimension in semantic_model.dimensions:
                dimension.name = dimension.name.lower()
        if semantic_model.defaults and semantic_model.defaults.agg_time_dimension:
            semantic_model.defaults.agg_time_dimension = semantic_model.defaults.agg_time_dimension.lower()

    @staticmethod
    def _lowercase_top_level_objects(model: PydanticSemanticManifest) -> None:
        """Lowercases the names of model objects."""
        if model.semantic_models:
            for semantic_model in model.semantic_models:
                semantic_model.name = semantic_model.name.lower()
