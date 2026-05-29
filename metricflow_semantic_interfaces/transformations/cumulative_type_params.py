from __future__ import annotations

from typing_extensions import override

from metricflow_semantic_interfaces.implementations.metric import PydanticCumulativeTypeParams
from metricflow_semantic_interfaces.implementations.semantic_manifest import (
    PydanticSemanticManifest,
)
from metricflow_semantic_interfaces.protocols import ProtocolHint
from metricflow_semantic_interfaces.transformations.transform_rule import (
    SemanticManifestTransformRule,
)
from metricflow_semantic_interfaces.type_enums import MetricType


class SetCumulativeTypeParamsRule(ProtocolHint[SemanticManifestTransformRule[PydanticSemanticManifest]]):
    """Ensure cumulative type params are populated from deprecated type params fields.

    All type params specific to cumulative metrics were originally set in `metric.type_params`. As we've added
    more, they've been moved to `metric.type_params.cumulative_type_params`, and the old fields will eventually
    be deprecated. In the meantime, here we populate the new fields with the old field values, if set, to ensure
    backward compatibility.
    Also populates cumulative_type_params for all cumulative metrics with PydanticCumulativeTypeParams if not set,
    which ensures the default `period_agg` value is set.
    """

    @override
    def _implements_protocol(self) -> SemanticManifestTransformRule[PydanticSemanticManifest]:  # noqa: D102
        return self

    @staticmethod
    def transform_model(semantic_manifest: PydanticSemanticManifest) -> PydanticSemanticManifest:  # noqa: D102
        for metric in semantic_manifest.metrics:
            if metric.type == MetricType.CUMULATIVE:
                if not metric.type_params.cumulative_type_params:
                    metric.type_params.cumulative_type_params = PydanticCumulativeTypeParams()

                if metric.type_params.window and not metric.type_params.cumulative_type_params.window:
                    metric.type_params.cumulative_type_params.window = metric.type_params.window
                if metric.type_params.grain_to_date and not metric.type_params.cumulative_type_params.grain_to_date:
                    metric.type_params.cumulative_type_params.grain_to_date = metric.type_params.grain_to_date.value

        return semantic_manifest
