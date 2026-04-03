from __future__ import annotations

import logging

from metricflow_semantics.errors.error_classes import MetricFlowInternalError
from metricflow_semantics.semantic_graph.lookups.manifest_object_lookup import ManifestObjectLookup
from metricflow_semantics.toolkit.cache.result_cache import ResultCache
from metricflow_semantics.toolkit.collections.ordered_set import FrozenOrderedSet
from metricflow_semantics.toolkit.mf_logging.lazy_formattable import LazyFormat

from metricflow_semantic_interfaces.enum_extension import assert_values_exhausted
from metricflow_semantic_interfaces.type_enums import MetricType

logger = logging.getLogger(__name__)


class MetricEvaluationLevelResolver:
    """Resolve metric evaluation levels within the metric dependency graph.

    The level describes the order in which metrics should be evaluated:

    * Simple, cumulative, and conversion metrics are evaluated at level `0`.
    * Ratio and derived metrics are evaluated at `max(input_levels) + 1`.

    For example, `bookings_per_listing` depends on `bookings` and `listings`. If those input metrics are level `0`,
    then `bookings_per_listing` is level `1`.
    """

    def __init__(self, manifest_object_lookup: ManifestObjectLookup) -> None:  # noqa: D107
        self._metric_evaluation_level_cache: ResultCache[str, int] = ResultCache()
        self._manifest_object_lookup = manifest_object_lookup

    def resolve_evaluation_level(self, metric_name: str) -> int:
        """Return the evaluation level for a metric name."""
        cache_entry = self._metric_evaluation_level_cache.get(metric_name)
        if cache_entry is not None:
            return cache_entry.value

        return self._metric_evaluation_level_cache.set_and_get(
            metric_name, self._compute_evaluation_level(metric_name=metric_name)
        )

    def _compute_evaluation_level(self, metric_name: str) -> int:
        """Compute the evaluation level for a metric without using the cache."""
        metric_definition = self._manifest_object_lookup.get_metric(metric_name)
        metric_type = metric_definition.type

        if (
            metric_type is MetricType.SIMPLE
            or metric_type is MetricType.CUMULATIVE
            or metric_type is MetricType.CONVERSION
        ):
            return 0
        if metric_type is MetricType.RATIO or metric_type is MetricType.DERIVED:
            # It's possible for a derived metric to have the same input metric multiple times via aliases,
            # so deduplicate.
            input_metric_names = FrozenOrderedSet(input_metric.name for input_metric in metric_definition.input_metrics)
            if not input_metric_names:
                raise MetricFlowInternalError(
                    LazyFormat(
                        "Expected ratio or derived metrics to define input metrics",
                        metric_name=metric_name,
                        metric_type=metric_type,
                    )
                )

            return max(self.resolve_evaluation_level(input_metric_name) for input_metric_name in input_metric_names) + 1

        assert_values_exhausted(metric_type)
