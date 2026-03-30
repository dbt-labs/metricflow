from __future__ import annotations

from typing import Set

from typing_extensions import override

from metricflow_semantic_interfaces.enum_extension import assert_values_exhausted
from metricflow_semantic_interfaces.errors import ModelTransformError
from metricflow_semantic_interfaces.implementations.metric import (
    PydanticMetric,
    PydanticMetricInputMeasure,
)
from metricflow_semantic_interfaces.implementations.semantic_manifest import (
    PydanticSemanticManifest,
)
from metricflow_semantic_interfaces.protocols import ProtocolHint
from metricflow_semantic_interfaces.transformations.transform_rule import (
    SemanticManifestTransformRule,
)
from metricflow_semantic_interfaces.type_enums import MetricType


class AddInputMetricMeasuresRule(ProtocolHint[SemanticManifestTransformRule[PydanticSemanticManifest]]):
    """Add all measures corresponding to the input metrics of the derived metric."""

    @override
    def _implements_protocol(self) -> SemanticManifestTransformRule[PydanticSemanticManifest]:  # noqa: D102
        return self

    @staticmethod
    def _get_measures_for_metric(
        semantic_manifest: PydanticSemanticManifest, metric_name: str
    ) -> Set[PydanticMetricInputMeasure]:
        """Returns a unique set of input measures for a given metric."""
        measures: Set = set()
        matched_metric = next((metric for metric in semantic_manifest.metrics if metric.name == metric_name), None)
        if matched_metric:
            if matched_metric.type is MetricType.SIMPLE or matched_metric.type is MetricType.CUMULATIVE:
                if matched_metric.type_params.measure is not None:
                    measures.add(matched_metric.type_params.measure)
            elif matched_metric.type is MetricType.DERIVED or matched_metric.type is MetricType.RATIO:
                for input_metric in matched_metric.input_metrics:
                    measures.update(
                        AddInputMetricMeasuresRule._get_measures_for_metric(semantic_manifest, input_metric.name)
                    )
            elif matched_metric.type is MetricType.CONVERSION:
                conversion_type_params = PydanticMetric.get_checked_conversion_type_params(matched_metric)
                # TODO SL-4116: this logic will need to change when we auto-transform
                # away measures into simple metrics.
                if conversion_type_params.base_measure is not None:
                    measures.add(conversion_type_params.base_measure)
                if conversion_type_params.conversion_measure is not None:
                    measures.add(conversion_type_params.conversion_measure)
            else:
                assert_values_exhausted(matched_metric.type)
        else:
            raise ModelTransformError(f"Metric '{metric_name}' is not configured as a metric in the model.")
        return measures

    @staticmethod
    def transform_model(semantic_manifest: PydanticSemanticManifest) -> PydanticSemanticManifest:  # noqa: D102
        for metric in semantic_manifest.metrics:
            if len(metric.type_params.input_measures) > 0:
                # These aren't missing and have already been added by an enterprising parser or earlier
                # transformation rule.
                continue
            measures = AddInputMetricMeasuresRule._get_measures_for_metric(semantic_manifest, metric.name)
            metric.type_params.input_measures = list(measures)

        return semantic_manifest
