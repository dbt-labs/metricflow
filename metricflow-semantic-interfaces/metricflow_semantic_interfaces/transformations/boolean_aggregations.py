from __future__ import annotations

import logging
from typing import Optional

from typing_extensions import override

from metricflow_semantic_interfaces.implementations.semantic_manifest import (
    PydanticSemanticManifest,
)
from metricflow_semantic_interfaces.protocols import ProtocolHint
from metricflow_semantic_interfaces.transformations.transform_rule import (
    SemanticManifestTransformRule,
)
from metricflow_semantic_interfaces.type_enums import AggregationType
from metricflow_semantic_interfaces.type_enums.metric_type import MetricType

logger = logging.getLogger(__name__)


class BooleanMeasureAggregationRule(ProtocolHint[SemanticManifestTransformRule[PydanticSemanticManifest]]):
    """Converts the expression used in boolean measures so that it can be aggregated.

    This is only used for legacy-style models; updated models should use metrics
    and rely solely on BooleanAggregationRule.
    """

    @override
    def _implements_protocol(self) -> SemanticManifestTransformRule[PydanticSemanticManifest]:  # noqa: D102
        return self

    @staticmethod
    def transform_model(semantic_manifest: PydanticSemanticManifest) -> PydanticSemanticManifest:  # noqa: D102
        for semantic_model in semantic_manifest.semantic_models:
            for measure in semantic_model.measures:
                if measure.agg == AggregationType.SUM_BOOLEAN:
                    measure.expr = BooleanAggregationRule.build_new_expr_value(
                        name=measure.name,
                        expr=measure.expr,
                    )
                    measure.agg = AggregationType.SUM

        return semantic_manifest


class BooleanAggregationRule(ProtocolHint[SemanticManifestTransformRule[PydanticSemanticManifest]]):
    """Converts the expression in simple metrics with boolean aggregations so they can be aggregated.

    Notes:
    * This only applies to SIMPLE metrics that do not rely on a measure input (i.e. has a value
      for type_params.metric_aggregation_params)
    """

    @override
    def _implements_protocol(self) -> SemanticManifestTransformRule[PydanticSemanticManifest]:  # noqa: D102
        return self

    @staticmethod
    def build_new_expr_value(*, name: str, expr: Optional[str]) -> str:  # noqa: D102
        sub_value = expr if expr else name
        return f"CASE WHEN {sub_value} THEN 1 ELSE 0 END"

    @staticmethod
    def transform_model(semantic_manifest: PydanticSemanticManifest) -> PydanticSemanticManifest:  # noqa: D102
        for metric in semantic_manifest.metrics:
            if (
                metric.type == MetricType.SIMPLE
                and metric.type_params.metric_aggregation_params is not None
                and metric.type_params.metric_aggregation_params.agg == AggregationType.SUM_BOOLEAN
            ):
                metric.type_params.expr = BooleanAggregationRule.build_new_expr_value(
                    name=metric.name,
                    expr=metric.type_params.expr,
                )
                metric.type_params.metric_aggregation_params.agg = AggregationType.SUM

        return semantic_manifest
