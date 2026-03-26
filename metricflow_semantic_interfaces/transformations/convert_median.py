from __future__ import annotations

from typing import Literal, NoReturn

from typing_extensions import override

from metricflow_semantic_interfaces.errors import ModelTransformError
from metricflow_semantic_interfaces.implementations.elements.measure import (
    PydanticMeasureAggregationParameters,
)
from metricflow_semantic_interfaces.implementations.semantic_manifest import (
    PydanticSemanticManifest,
)
from metricflow_semantic_interfaces.protocols import ProtocolHint
from metricflow_semantic_interfaces.transformations.transform_rule import (
    SemanticManifestTransformRule,
)
from metricflow_semantic_interfaces.type_enums import AggregationType
from metricflow_semantic_interfaces.type_enums.metric_type import MetricType


class ConvertMedianMetricToPercentile(ProtocolHint[SemanticManifestTransformRule[PydanticSemanticManifest]]):
    """Converts any MEDIAN metrics to percentile equivalent.

    Applies to SIMPLE metrics that aggregate an expression directly via
    `type_params.metric_aggregation_params`.
    """

    TRANSFORMED_AGG_TYPE = AggregationType.PERCENTILE
    MEDIAN_PERCENTILE = 0.5

    @override
    def _implements_protocol(self) -> SemanticManifestTransformRule[PydanticSemanticManifest]:  # noqa: D102
        return self

    @staticmethod
    def _throw_conflicting_percentile_error(
        object_name: str,
        object_type: Literal["Metric", "Measure"],
        percentile: float,
    ) -> NoReturn:
        raise ModelTransformError(
            f"{object_type} '{object_name}' uses a MEDIAN aggregation, while percentile "
            f"is set to '{percentile}', a conflicting value. Please remove the parameter "
            "or set to '0.5'."
        )

    @staticmethod
    def _throw_conflicting_discrete_percentile_error(
        object_name: str,
        object_type: Literal["Metric", "Measure"],
    ) -> NoReturn:
        raise ModelTransformError(
            f"{object_type} '{object_name}' uses a MEDIAN aggregation, while use_discrete_percentile "
            f"is set to true. Please remove the parameter or set to False."
        )

    @staticmethod
    def transform_model(semantic_manifest: PydanticSemanticManifest) -> PydanticSemanticManifest:  # noqa: D102
        for metric in semantic_manifest.metrics:
            if (
                metric.type == MetricType.SIMPLE
                and metric.type_params.metric_aggregation_params is not None
                and metric.type_params.metric_aggregation_params.agg == AggregationType.MEDIAN
            ):
                # Update aggregation type first
                metric.type_params.metric_aggregation_params.agg = ConvertMedianMetricToPercentile.TRANSFORMED_AGG_TYPE

                # Ensure aggregation parameters exist, then validate
                if metric.type_params.metric_aggregation_params.agg_params is None:
                    metric.type_params.metric_aggregation_params.agg_params = PydanticMeasureAggregationParameters()
                else:
                    agg_params = metric.type_params.metric_aggregation_params.agg_params
                    if agg_params.percentile is not None and agg_params.percentile != MEDIAN_PERCENTILE:
                        ConvertMedianMetricToPercentile._throw_conflicting_percentile_error(
                            object_name=metric.name,
                            object_type="Metric",
                            percentile=agg_params.percentile,
                        )
                    if agg_params.use_discrete_percentile:
                        ConvertMedianMetricToPercentile._throw_conflicting_discrete_percentile_error(
                            object_name=metric.name,
                            object_type="Metric",
                        )

                # Set the median percentile
                metric.type_params.metric_aggregation_params.agg_params.percentile = MEDIAN_PERCENTILE

        return semantic_manifest


class ConvertMedianToPercentileRule(ConvertMedianMetricToPercentile):
    """Converts any MEDIAN measures to percentile equivalent."""

    @override
    def _implements_protocol(self) -> SemanticManifestTransformRule[PydanticSemanticManifest]:  # noqa: D102
        return self

    @staticmethod
    def transform_model(semantic_manifest: PydanticSemanticManifest) -> PydanticSemanticManifest:  # noqa: D102
        for semantic_model in semantic_manifest.semantic_models:
            for measure in semantic_model.measures:
                if measure.agg == AggregationType.MEDIAN:
                    measure.agg = ConvertMedianToPercentileRule.TRANSFORMED_AGG_TYPE

                    if not measure.agg_params:
                        measure.agg_params = PydanticMeasureAggregationParameters()
                    else:
                        if measure.agg_params.percentile is not None and measure.agg_params.percentile != 0.5:
                            ConvertMedianToPercentileRule._throw_conflicting_percentile_error(
                                object_name=measure.name,
                                object_type="Measure",
                                percentile=measure.agg_params.percentile,
                            )
                        if measure.agg_params.use_discrete_percentile:
                            ConvertMedianToPercentileRule._throw_conflicting_discrete_percentile_error(
                                object_name=measure.name,
                                object_type="Measure",
                            )
                    measure.agg_params.percentile = ConvertMedianToPercentileRule.MEDIAN_PERCENTILE
                    # let's not set use_approximate_percentile to be false due to valid performance reasons
        return semantic_manifest


# Just left here for legacy reasons.  Maybe we can get rid of this when we remove
# measures?
MEDIAN_PERCENTILE = ConvertMedianMetricToPercentile.MEDIAN_PERCENTILE
