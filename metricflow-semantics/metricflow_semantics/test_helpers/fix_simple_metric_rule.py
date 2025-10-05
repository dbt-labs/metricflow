from __future__ import annotations

import copy
import logging
from collections.abc import Mapping

from dbt_semantic_interfaces.implementations.elements.measure import PydanticMeasure
from dbt_semantic_interfaces.implementations.filters.where_filter import (
    PydanticWhereFilter,
    PydanticWhereFilterIntersection,
)
from dbt_semantic_interfaces.implementations.metric import (
    PydanticMetric,
    PydanticMetricInputMeasure,
)
from dbt_semantic_interfaces.implementations.semantic_manifest import PydanticSemanticManifest
from dbt_semantic_interfaces.type_enums import MetricType

logger = logging.getLogger(__name__)


class FixSimpleMetricRule:
    """Fixes some potential issues with the measure -> simple metric transform.

    * `expr` needs to be populated as the name of the measure can be different from the name of the simple metric.
    * Filters for the measure are combined with the metric filters.
    """

    @staticmethod
    def _fix_simple_metric_expr(
        metric: PydanticMetric,
        input_measure: PydanticMetricInputMeasure,
        measure_name_to_measure: Mapping[str, PydanticMeasure],
    ) -> None:
        metric_expr = metric.type_params.expr
        measure_name = input_measure.name
        measure = measure_name_to_measure[measure_name]
        measure_expr = measure.expr or measure_name
        if metric_expr is None:
            metric.type_params.expr = measure_expr

    @staticmethod
    def transform_manifest(semantic_manifest: PydanticSemanticManifest) -> PydanticSemanticManifest:
        """See class docstring."""
        measure_name_to_measure: Mapping[str, PydanticMeasure] = {
            measure.name: measure
            for semantic_model in semantic_manifest.semantic_models
            for measure in semantic_model.measures
        }

        new_metrics: list[PydanticMetric] = []

        for metric in semantic_manifest.metrics:
            metric_to_add = copy.deepcopy(metric)
            metric_type = metric_to_add.type

            if metric_type is MetricType.SIMPLE:
                if metric_to_add.type_params.measure is None:
                    continue

                FixSimpleMetricRule._fix_simple_metric_expr(
                    metric=metric_to_add,
                    input_measure=metric_to_add.type_params.measure,
                    measure_name_to_measure=measure_name_to_measure,
                )

                filter_items: list[PydanticWhereFilter] = []
                if metric_to_add.type_params.measure.filter is not None:
                    filter_items.extend(metric_to_add.type_params.measure.filter.where_filters)
                    metric_to_add.type_params.measure.filter = None
                if metric_to_add.filter is not None:
                    filter_items.extend(metric_to_add.filter.where_filters)
                    metric_to_add.filter = None

                if len(filter_items) > 0:
                    metric_to_add.filter = PydanticWhereFilterIntersection(where_filters=filter_items)
            elif (
                metric_type is MetricType.CONVERSION
                or metric_type is MetricType.RATIO
                or metric_type is MetricType.DERIVED
                or metric_type is MetricType.CUMULATIVE
            ):
                pass

            new_metrics.append(metric_to_add)

        new_semantic_manifest = copy.deepcopy(semantic_manifest)
        new_semantic_manifest.metrics = new_metrics
        return new_semantic_manifest
