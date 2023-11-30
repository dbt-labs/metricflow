from __future__ import annotations

from dbt_semantic_interfaces.implementations.filters.where_filter import PydanticWhereFilterIntersection
from dbt_semantic_interfaces.implementations.semantic_manifest import PydanticSemanticManifest
from dbt_semantic_interfaces.references import MetricReference
from dbt_semantic_interfaces.transformations.transform_rule import SemanticManifestTransformRule
from typing_extensions import override


class ModifyMetricFilterTransform(SemanticManifestTransformRule[PydanticSemanticManifest]):
    """Modifies the specified metric to have the given filter.

    This is useful for programmatically generating different manifests to create different test cases.
    """

    def __init__(  # noqa: D
        self,
        metric_reference: MetricReference,
        where_filter_intersection: PydanticWhereFilterIntersection,
    ) -> None:
        self._metric_reference = metric_reference
        self._where_filter_intersection = where_filter_intersection

    @override
    def transform_model(self, semantic_manifest: PydanticSemanticManifest) -> PydanticSemanticManifest:
        updated_metrics = []

        for metric in semantic_manifest.metrics:
            if MetricReference(element_name=metric.name) != self._metric_reference:
                continue
            metric.filter = self._where_filter_intersection
            updated_metrics.append(metric)

        if len(updated_metrics) != 1:
            raise RuntimeError(f"Did not update exactly 1 metric. Updated: {updated_metrics}")

        return semantic_manifest
