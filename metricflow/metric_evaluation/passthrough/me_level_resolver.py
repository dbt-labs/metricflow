from __future__ import annotations

import logging

from dbt_semantic_interfaces.enum_extension import assert_values_exhausted
from dbt_semantic_interfaces.type_enums import MetricType
from metricflow_semantics.semantic_graph.lookups.manifest_object_lookup import ManifestObjectLookup
from metricflow_semantics.toolkit.cache.result_cache import ResultCache
from metricflow_semantics.toolkit.collections.ordered_set import FrozenOrderedSet

logger = logging.getLogger(__name__)


class MetricEvaluationLevelResolver:
    """Resolves the evaluation level for a given metric.

    * The evaluation level describes the order of evaluation for metrics in the metric dependency graph.
    * The evaluation level of a given metric is one more than the evaluation level of its input metrics.
    * Simple, cumulative, and conversion metric have an evaluation level of 0.

    For example, the `bookings_per_listing` metric has `bookings` and `listings` as input metrics. `bookings` and
    `listings` have an evaluation level of 0 and `bookings_per_listing` has an evaluation level of 1.
    """

    def __init__(self, manifest_object_lookup: ManifestObjectLookup) -> None:  # noqa: D107
        self._metric_evaluation_level_cache: ResultCache[str, int] = ResultCache()
        self._manifest_object_lookup = manifest_object_lookup

    def resolve_evaluation_level(self, metric_name: str) -> int:
        """Return the evaluation level of the given metric."""
        cache_key = metric_name
        result = self._metric_evaluation_level_cache.get(cache_key)
        if result:
            return result.value

        metric = self._manifest_object_lookup.get_metric(metric_name)
        metric_type = metric.type

        if (
            metric_type is MetricType.SIMPLE
            or metric_type is MetricType.CUMULATIVE
            or metric_type is MetricType.CONVERSION
        ):
            evaluation_level = 0
        elif metric_type is MetricType.RATIO or metric_type is MetricType.DERIVED:
            input_metric_names = FrozenOrderedSet(metric.name for metric in metric.input_metrics)
            evaluation_level = (
                max(self.resolve_evaluation_level(input_metric_name) for input_metric_name in input_metric_names) + 1
            )
        else:
            assert_values_exhausted(metric_type)

        return self._metric_evaluation_level_cache.set_and_get(cache_key, evaluation_level)
