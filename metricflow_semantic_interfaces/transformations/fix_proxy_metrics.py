from __future__ import annotations

import logging

from typing_extensions import override

from metricflow_semantic_interfaces.implementations.semantic_manifest import (
    PydanticSemanticManifest,
)
from metricflow_semantic_interfaces.protocols import ProtocolHint
from metricflow_semantic_interfaces.transformations.transform_rule import (
    SemanticManifestTransformRule,
)
from metricflow_semantic_interfaces.type_enums import MetricType

logger = logging.getLogger(__name__)


class FixProxyMetricsRule(ProtocolHint[SemanticManifestTransformRule[PydanticSemanticManifest]]):
    """Fixes simple metrics expr.

    Currently, we allow users to set an expr on simple metrics that just references a measure.
    This is technically allowed in the spec, but we don't actually use it to do anything as
    the expr that gets rendered will always be the referenced measure's expr. With the migration
    to the new spec where measures are removed, we are now using the metric.expr field to render
    the SQL. However, this ends up breaking old specs where the metric.expr is now being rendered
    which causes unexpected changes. This transformation will just set the expr to the measure expr
    if it exists to conform with the current behaviour.
    """

    @override
    def _implements_protocol(self) -> SemanticManifestTransformRule[PydanticSemanticManifest]:  # noqa: D102
        return self

    @staticmethod
    def transform_model(semantic_manifest: PydanticSemanticManifest) -> PydanticSemanticManifest:
        """Fixes simple metrics expr."""
        all_measures = {
            measure.name: measure
            for semantic_model in semantic_manifest.semantic_models
            for measure in semantic_model.measures
        }

        for metric in semantic_manifest.metrics:
            if metric.type != MetricType.SIMPLE:
                # Only fix simple metrics
                continue
            if metric.type_params.measure is None:
                # No measure input, so no expr to fix
                # Likely the new spec where measures are removed
                continue

            # Override the expr to the measure expr or name if it is not set.
            referenced_measure = all_measures.get(metric.type_params.measure.name)

            if referenced_measure is None:
                logger.warning(f"Measure {metric.type_params.measure.name} not found")
                continue

            if metric.type_params.expr is not None and metric.type_params.expr not in [
                referenced_measure.expr,
                referenced_measure.name,
            ]:
                logger.warning(
                    f"Metric {metric.name} should not have an expr set if it's proxy from measures, "
                    "overriding with measure"
                )

            metric.type_params.expr = referenced_measure.expr or referenced_measure.name
        return semantic_manifest
