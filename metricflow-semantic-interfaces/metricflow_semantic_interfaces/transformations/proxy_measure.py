from __future__ import annotations

import logging

from typing_extensions import override

from metricflow_semantic_interfaces.errors import ModelTransformError
from metricflow_semantic_interfaces.implementations.metric import PydanticMetricInputMeasure
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


class CreateProxyMeasureRule(ProtocolHint[SemanticManifestTransformRule[PydanticSemanticManifest]]):
    """Adds a proxy metric for measures that have the create_metric flag set, if it does not already exist.

    Also checks that a defined metric with the same name as a measure is a proxy metric.
    """

    @override
    def _implements_protocol(self) -> SemanticManifestTransformRule[PydanticSemanticManifest]:  # noqa: D102
        return self

    @staticmethod
    def transform_model(semantic_manifest: PydanticSemanticManifest) -> PydanticSemanticManifest:
        """Creates measure proxy metrics for measures with `create_metric==True`."""
        for semantic_model in semantic_manifest.semantic_models:
            for measure in semantic_model.measures:
                if not measure.create_metric:
                    continue

                add_metric = True
                for metric in semantic_manifest.metrics:
                    if metric.name == measure.name:
                        if metric.type != MetricType.SIMPLE:
                            raise ModelTransformError(
                                f"Cannot have metric with the same name as a measure ({measure.name}) that is not a "
                                f"created mechanically from that measure using create_metric=True"
                            )
                        logger.warning(
                            f"Metric already exists with name ({measure.name}). *Not* adding measure proxy metric for "
                            f"that measure"
                        )
                        add_metric = False

                if add_metric is True:
                    metric = MeasureFeaturesToMetricNameMapper.build_metric_from_measure_configuration(
                        measure=measure,
                        semantic_model_name=semantic_model.name,
                        fill_nulls_with=None,
                        join_to_timespine=False,
                        # we override the default here; this metric was explicitly created by the user.
                        is_private=False,
                        measure_input_filters=None,
                    )
                    metric.name = measure.name
                    metric.type_params.measure = PydanticMetricInputMeasure(name=measure.name)
                    semantic_manifest.metrics.append(metric)

        return semantic_manifest
