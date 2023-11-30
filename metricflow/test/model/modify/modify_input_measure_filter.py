from __future__ import annotations

import logging

from dbt_semantic_interfaces.implementations.filters.where_filter import PydanticWhereFilterIntersection
from dbt_semantic_interfaces.implementations.semantic_manifest import PydanticSemanticManifest
from dbt_semantic_interfaces.references import MetricReference
from dbt_semantic_interfaces.transformations.transform_rule import SemanticManifestTransformRule
from typing_extensions import override

logger = logging.getLogger(__name__)


class ModifyInputMeasureFilterTransform(SemanticManifestTransformRule[PydanticSemanticManifest]):
    """Modifies all input measures of the specified metric to have the given filter.

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
        updated_input_measures = []

        for metric in semantic_manifest.metrics:
            if MetricReference(element_name=metric.name) != self._metric_reference:
                continue
            for input_measure in metric.input_measures:
                input_measure.filter = self._where_filter_intersection
                updated_input_measures.append(input_measure)

        if len(updated_input_measures) == 0:
            raise RuntimeError("Did not update any input measures.")

        return semantic_manifest
