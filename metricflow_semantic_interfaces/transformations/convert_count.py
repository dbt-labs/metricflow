from __future__ import annotations

from typing import Literal, NoReturn

from typing_extensions import override

from metricflow_semantic_interfaces.errors import ModelTransformError
from metricflow_semantic_interfaces.implementations.semantic_manifest import (
    PydanticSemanticManifest,
)
from metricflow_semantic_interfaces.protocols import ProtocolHint
from metricflow_semantic_interfaces.transformations.transform_rule import (
    SemanticManifestTransformRule,
)
from metricflow_semantic_interfaces.type_enums import AggregationType
from metricflow_semantic_interfaces.type_enums.metric_type import MetricType

ONE = "1"


class ConvertCountMetricToSumRule(ProtocolHint[SemanticManifestTransformRule[PydanticSemanticManifest]]):
    """Converts any COUNT metrics to SUM equivalent.

    This only applies to SIMPLE metrics.
    """

    TRANSFORMED_AGG_TYPE = AggregationType.SUM

    @override
    def _implements_protocol(self) -> SemanticManifestTransformRule[PydanticSemanticManifest]:  # noqa: D102
        return self

    @staticmethod
    def transform_model(semantic_manifest: PydanticSemanticManifest) -> PydanticSemanticManifest:  # noqa: D102
        for metric in semantic_manifest.metrics:
            if (
                metric.type == MetricType.SIMPLE
                and metric.type_params.metric_aggregation_params is not None
                and metric.type_params.metric_aggregation_params.agg == AggregationType.COUNT
            ):
                if metric.type_params.expr is None:
                    ConvertCountMetricToSumRule._throw_missing_expr_error(
                        object_name=metric.name,
                        object_type="Metric",
                    )
                metric.type_params.expr = ConvertCountMetricToSumRule._maybe_transform_expression(
                    metric.type_params.expr
                )
                metric.type_params.metric_aggregation_params.agg = ConvertCountMetricToSumRule.TRANSFORMED_AGG_TYPE
        return semantic_manifest

    @staticmethod
    def _maybe_transform_expression(expr: str) -> str:
        """Transforms the expression if it is not ONE, otherwise returns the expression unchanged."""
        if expr == ONE:
            # Just leave it as SUM(1) if we want to count all
            return expr
        return f"CASE WHEN {expr} IS NOT NULL THEN 1 ELSE 0 END"

    @staticmethod
    def _throw_missing_expr_error(  # noqa: D102
        object_name: str,
        object_type: Literal["Metric", "Measure"],
    ) -> NoReturn:
        raise ModelTransformError(
            f"{object_type} '{object_name}' uses a COUNT aggregation, which requires an expr to be "
            f"provided. Provide 'expr: 1' if a count of all rows is desired."
        )


class ConvertCountToSumRule(ConvertCountMetricToSumRule):
    """Converts any COUNT measures to SUM equivalent.

    This is a *legacy* behavior that will be irrelevant once measures are no longer supported.
    """

    @override
    def _implements_protocol(self) -> SemanticManifestTransformRule[PydanticSemanticManifest]:  # noqa: D102
        return self

    @staticmethod
    def transform_model(semantic_manifest: PydanticSemanticManifest) -> PydanticSemanticManifest:  # noqa: D102
        for semantic_model in semantic_manifest.semantic_models:
            for measure in semantic_model.measures:
                if measure.agg == AggregationType.COUNT:
                    if measure.expr is None:
                        ConvertCountMetricToSumRule._throw_missing_expr_error(
                            object_name=measure.name,
                            object_type="Metric",
                        )
                    measure.expr = ConvertCountMetricToSumRule._maybe_transform_expression(measure.expr)
                    measure.agg = ConvertCountMetricToSumRule.TRANSFORMED_AGG_TYPE
        return semantic_manifest
