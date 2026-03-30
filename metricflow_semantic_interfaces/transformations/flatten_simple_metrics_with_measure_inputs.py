from __future__ import annotations

import logging

from typing_extensions import override

from metricflow_semantic_interfaces.implementations.semantic_manifest import (
    PydanticSemanticManifest,
)
from metricflow_semantic_interfaces.protocols import ProtocolHint
from metricflow_semantic_interfaces.transformations.measure_to_metric_transformation_pieces.measure_features_to_metric_name import (  # noqa: E501
    MeasureFeaturesToMetricNameMapper,
)
from metricflow_semantic_interfaces.transformations.transform_rule import (
    SemanticManifestTransformRule,
)
from metricflow_semantic_interfaces.type_enums import MetricType

logger = logging.getLogger(__name__)


class FlattenSimpleMetricsWithMeasureInputsRule(ProtocolHint[SemanticManifestTransformRule[PydanticSemanticManifest]]):
    """Flattens simple metrics with measure inputs into a single metric with a measure input."""

    @override
    def _implements_protocol(self) -> SemanticManifestTransformRule[PydanticSemanticManifest]:  # noqa: D102
        return self

    @staticmethod
    def transform_model(semantic_manifest: PydanticSemanticManifest) -> PydanticSemanticManifest:  # noqa: D102
        measure_info_map = semantic_manifest.build_measure_name_to_model_and_measure_map()
        for metric in semantic_manifest.metrics:
            if metric.type == MetricType.SIMPLE:
                # If this is a simple metric with a measure input that does NOT already have some
                # sort of metric input information overriding that measure input
                input_measure = metric.type_params.measure
                if input_measure is None:
                    continue

                #  or metric.type_params.metric_aggregation_params is not None:

                model_and_measure = measure_info_map.get(input_measure.name)
                if model_and_measure is None:
                    # Should be validated; see test_metric_missing_measure for tests that show that this
                    # is the case.
                    logger.warning(
                        f"Measure {input_measure.name} not found in any semantic model; skipping flattening of metric. "
                        "(This should also be caught by validations.)"
                    )
                    continue
                semantic_model, measure = model_and_measure

                MeasureFeaturesToMetricNameMapper.update_required_measure_features_in_simple_model(
                    measure=measure,
                    semantic_model_name=semantic_model.name,
                    metric=metric,
                    fill_nulls_with=input_measure.fill_nulls_with,
                    join_to_timespine=input_measure.join_to_timespine,
                    measure_input_filters=input_measure.filter,
                )

        return semantic_manifest
