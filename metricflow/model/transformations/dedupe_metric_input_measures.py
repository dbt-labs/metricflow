from __future__ import annotations

from dbt_semantic_interfaces.implementations.semantic_manifest import (
    PydanticSemanticManifest,
)
from dbt_semantic_interfaces.protocols import ProtocolHint
from dbt_semantic_interfaces.transformations.transform_rule import (
    SemanticManifestTransformRule,
)
from typing_extensions import override


class DedupeMetricInputMeasuresRule(ProtocolHint[SemanticManifestTransformRule[PydanticSemanticManifest]]):
    """Dedupe the input measures within a metric.

    This can be removed once the fix is in the dbt-core transformation.
    """

    @override
    def _implements_protocol(self) -> SemanticManifestTransformRule[PydanticSemanticManifest]:  # noqa: D
        return self

    @staticmethod
    def transform_model(semantic_manifest: PydanticSemanticManifest) -> PydanticSemanticManifest:  # noqa: D
        for metric in semantic_manifest.metrics:
            metric.type_params.input_measures = list(dict.fromkeys(metric.input_measures).keys())
        return semantic_manifest
