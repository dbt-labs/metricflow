from __future__ import annotations

import logging
import time
from typing import Dict, Optional, Sequence, Set, Tuple

from dbt_semantic_interfaces.enum_extension import assert_values_exhausted
from dbt_semantic_interfaces.protocols.metric import Metric, MetricInputMeasure, MetricType
from dbt_semantic_interfaces.protocols.semantic_manifest import SemanticManifest
from dbt_semantic_interfaces.references import MeasureReference, MetricReference
from dbt_semantic_interfaces.type_enums.time_granularity import TimeGranularity

from metricflow_semantics.collection_helpers.lru_cache import LruCache
from metricflow_semantics.errors.error_classes import DuplicateMetricError, MetricNotFoundError, NonExistentMeasureError
from metricflow_semantics.mf_logging.lazy_formattable import LazyFormat
from metricflow_semantics.model.linkable_element_property import LinkableElementProperty
from metricflow_semantics.model.semantics.element_filter import LinkableElementFilter
from metricflow_semantics.model.semantics.linkable_element_set import LinkableElementSet
from metricflow_semantics.model.semantics.linkable_spec_resolver import (
    ValidLinkableSpecResolver,
)
from metricflow_semantics.model.semantics.semantic_model_join_evaluator import MAX_JOIN_HOPS
from metricflow_semantics.model.semantics.semantic_model_lookup import SemanticModelLookup
from metricflow_semantics.specs.instance_spec import LinkableInstanceSpec
from metricflow_semantics.specs.time_dimension_spec import TimeDimensionSpec
from metricflow_semantics.time.granularity import ExpandedTimeGranularity

logger = logging.getLogger(__name__)


class MetricLookup:
    """Tracks semantic information for metrics by linking them to semantic models."""

    def __init__(
        self,
        semantic_manifest: SemanticManifest,
        semantic_model_lookup: SemanticModelLookup,
        custom_granularities: Dict[str, ExpandedTimeGranularity],
    ) -> None:
        """Initializer.

        Args:
            semantic_manifest: used to fetch and load the metrics and initialize the linkable spec resolver
            semantic_model_lookup: provides access to semantic model metadata for various lookup operations
        """
        self._metrics: Dict[MetricReference, Metric] = {}
        self._semantic_model_lookup = semantic_model_lookup
        self._custom_granularities = custom_granularities

        for metric in semantic_manifest.metrics:
            self._add_metric(metric)

        self._linkable_spec_resolver = ValidLinkableSpecResolver(
            semantic_manifest=semantic_manifest,
            semantic_model_lookup=semantic_model_lookup,
            max_entity_links=MAX_JOIN_HOPS,
        )

        # Cache for `get_min_queryable_time_granularity()`
        self._metric_reference_to_min_metric_time_grain: Dict[MetricReference, TimeGranularity] = {}

        # Cache for `get_valid_agg_time_dimensions_for_metric()`.
        self._metric_reference_to_valid_agg_time_dimension_specs: Dict[
            MetricReference, Sequence[TimeDimensionSpec]
        ] = {}

        # Cache for `linkable_elements_for_measure()`.
        self._linkable_element_set_for_measure_cache: Dict[
            Tuple[MeasureReference, LinkableElementFilter], LinkableElementSet
        ] = {}

        self._linkable_elements_including_group_by_metrics_cache = LruCache[
            Tuple[MeasureReference, LinkableElementFilter], LinkableElementSet
        ](128)
        self._linkable_elements_for_no_metrics_query_cache = LruCache[LinkableElementFilter, LinkableElementSet](128)
        self._linkable_elements_for_metrics_cache = LruCache[
            Tuple[Sequence[MetricReference], LinkableElementFilter], LinkableElementSet
        ](128)

    def linkable_elements_for_measure(
        self,
        measure_reference: MeasureReference,
        element_filter: LinkableElementFilter = LinkableElementFilter(),
    ) -> LinkableElementSet:
        """Return the set of linkable elements reachable from a given measure."""
        start_time = time.time()

        # Cache the result when group-by-metrics are selected in an LRU cache as there can be many of them and may
        # significantly increase memory usage.
        if (
            LinkableElementProperty.METRIC in element_filter.with_any_of
            and LinkableElementProperty.METRIC not in element_filter.without_any_of
        ):
            cache_key = (measure_reference, element_filter)
            result = self._linkable_elements_including_group_by_metrics_cache.get(cache_key)
            if result is not None:
                return result

            result = self._linkable_spec_resolver.get_linkable_element_set_for_measure(
                measure_reference, element_filter
            )
            self._linkable_elements_including_group_by_metrics_cache.set(cache_key, result)
            return result

        # Cache the result without element names in the filter for better hit rates.
        element_filter_without_element_names = element_filter.without_element_names()
        cache_key = (measure_reference, element_filter_without_element_names)

        result = self._linkable_element_set_for_measure_cache.get(cache_key)
        if result is not None:
            return result.filter(element_filter)

        result = self._linkable_spec_resolver.get_linkable_element_set_for_measure(
            measure_reference, element_filter_without_element_names
        )
        self._linkable_element_set_for_measure_cache[cache_key] = result

        logger.debug(
            LazyFormat(
                "Finished getting linkable elements",
                measure_reference=measure_reference,
                element_filter=element_filter,
                runtime=time.time() - start_time,
            )
        )
        return result.filter(element_filter)

    def linkable_elements_for_no_metrics_query(
        self, element_set_filter: LinkableElementFilter = LinkableElementFilter()
    ) -> LinkableElementSet:
        """Return the reachable linkable elements for a dimension values query with no metrics."""
        cache_key = element_set_filter
        result = self._linkable_elements_for_no_metrics_query_cache.get(cache_key)
        if result is not None:
            return result

        result = self._linkable_spec_resolver.get_linkable_elements_for_distinct_values_query(element_set_filter)
        self._linkable_elements_for_no_metrics_query_cache.set(cache_key, result)
        return result

    def linkable_elements_for_metrics(
        self, metric_references: Sequence[MetricReference], element_set_filter: LinkableElementFilter
    ) -> LinkableElementSet:
        """Retrieve the matching set of linkable elements common to all metrics requested (intersection)."""
        cache_key = (metric_references, element_set_filter)
        result = self._linkable_elements_for_metrics_cache.get(cache_key)
        if result is not None:
            return result

        result = self._linkable_spec_resolver.get_linkable_elements_for_metrics(
            metric_references=metric_references, element_filter=element_set_filter
        )
        self._linkable_elements_for_metrics_cache.set(cache_key, result)
        return result

    def get_metrics(self, metric_references: Sequence[MetricReference]) -> Sequence[Metric]:  # noqa: D102
        res = []
        for metric_reference in metric_references:
            if metric_reference not in self._metrics:
                raise MetricNotFoundError(
                    f"Unable to find metric `{metric_reference}`. Perhaps it has not been registered"
                )
            res.append(self._metrics[metric_reference])

        return res

    @property
    def metric_references(self) -> Sequence[MetricReference]:  # noqa: D102
        return list(self._metrics.keys())

    def get_metric(self, metric_reference: MetricReference) -> Metric:  # noqa: D102
        if metric_reference not in self._metrics:
            raise MetricNotFoundError(f"Unable to find metric `{metric_reference}`. Perhaps it has not been registered")
        return self._metrics[metric_reference]

    def _add_metric(self, metric: Metric) -> None:
        """Add metric, validating presence of required measures."""
        metric_reference = MetricReference(element_name=metric.name)
        if metric_reference in self._metrics:
            raise DuplicateMetricError(f"Metric `{metric.name}` has already been registered")
        for measure_reference in metric.measure_references:
            if measure_reference not in self._semantic_model_lookup.measure_references:
                raise NonExistentMeasureError(
                    f"Metric `{metric.name}` references measure `{measure_reference}` which has not been registered"
                )
        self._metrics[metric_reference] = metric

    def configured_input_measure_for_metric(  # noqa: D102
        self, metric_reference: MetricReference
    ) -> Optional[MetricInputMeasure]:  # noqa: D102
        metric = self.get_metric(metric_reference=metric_reference)
        if metric.type is MetricType.CUMULATIVE or metric.type is MetricType.SIMPLE:
            assert len(metric.input_measures) == 1, "Simple and cumulative metrics should have one input measure."
            return metric.input_measures[0]
        elif (
            metric.type is MetricType.RATIO or metric.type is MetricType.DERIVED or metric.type is MetricType.CONVERSION
        ):
            return None
        else:
            assert_values_exhausted(metric.type)

    def contains_cumulative_or_time_offset_metric(self, metric_references: Sequence[MetricReference]) -> bool:
        """Returns true if any of the specs correspond to a cumulative metric or a derived metric with time offset."""
        for metric_reference in metric_references:
            metric = self.get_metric(metric_reference)
            if metric.type == MetricType.CUMULATIVE:
                return True
            elif metric.type == MetricType.DERIVED:
                for input_metric in metric.type_params.metrics or []:
                    if input_metric.offset_window or input_metric.offset_to_grain:
                        return True
        return False

    def _get_agg_time_dimension_specs_for_metric(
        self, metric_reference: MetricReference
    ) -> Sequence[TimeDimensionSpec]:
        """Retrieves the aggregate time dimensions associated with the metric's measures."""
        metric = self.get_metric(metric_reference)
        specs: Set[TimeDimensionSpec] = set()
        for input_measure in metric.input_measures:
            measure_properties = self._semantic_model_lookup.measure_lookup.get_properties(
                measure_reference=input_measure.measure_reference
            )
            specs.update(measure_properties.agg_time_dimension_specs)
        return list(specs)

    def get_valid_agg_time_dimensions_for_metric(
        self, metric_reference: MetricReference
    ) -> Sequence[TimeDimensionSpec]:
        """Get the agg time dimension specs that can be used in place of metric time for this metric, if applicable."""
        result = self._metric_reference_to_valid_agg_time_dimension_specs.get(metric_reference)
        if result is not None:
            return result

        result = self._get_valid_agg_time_dimensions_for_metric(metric_reference)
        self._metric_reference_to_valid_agg_time_dimension_specs[metric_reference] = result

        return result

    def _get_valid_agg_time_dimensions_for_metric(
        self, metric_reference: MetricReference
    ) -> Sequence[TimeDimensionSpec]:
        agg_time_dimension_specs = self._get_agg_time_dimension_specs_for_metric(metric_reference)
        distinct_agg_time_dimension_identifiers = set(
            [(spec.reference, spec.entity_links) for spec in agg_time_dimension_specs]
        )
        if len(distinct_agg_time_dimension_identifiers) != 1:
            # If the metric's input measures have different agg_time_dimensions, user must use metric_time.
            return []

        agg_time_dimension_reference, agg_time_dimension_entity_links = distinct_agg_time_dimension_identifiers.pop()
        valid_agg_time_dimension_specs = TimeDimensionSpec.generate_possible_specs_for_time_dimension(
            time_dimension_reference=agg_time_dimension_reference,
            entity_links=agg_time_dimension_entity_links,
            custom_granularities=self._custom_granularities,
        )
        return valid_agg_time_dimension_specs

    def get_min_queryable_time_granularity(self, metric_reference: MetricReference) -> TimeGranularity:
        """The minimum grain that can be queried with this metric.

        Maps to the largest granularity defined for any of the metric's agg_time_dimensions.
        """
        result = self._metric_reference_to_min_metric_time_grain.get(metric_reference)
        if result is not None:
            return result

        result = self._get_min_queryable_time_granularity(metric_reference)
        self._metric_reference_to_min_metric_time_grain[metric_reference] = result
        return result

    def _get_min_queryable_time_granularity(self, metric_reference: MetricReference) -> TimeGranularity:
        metric = self.get_metric(metric_reference)
        agg_time_dimension_grains = set()
        for input_measure in metric.input_measures:
            measure_properties = self._semantic_model_lookup.measure_lookup.get_properties(
                input_measure.measure_reference
            )
            agg_time_dimension_grains.add(measure_properties.agg_time_granularity)

        return max(agg_time_dimension_grains, key=lambda time_granularity: time_granularity.to_int())

    def get_joinable_scd_specs_for_metric(self, metric_reference: MetricReference) -> Sequence[LinkableInstanceSpec]:
        """Get the SCDs that can be joined to a metric."""
        filter = LinkableElementFilter(
            with_any_of=frozenset([LinkableElementProperty.SCD_HOP]),
        )
        scd_elems = self.linkable_elements_for_metrics(
            metric_references=(metric_reference,),
            element_set_filter=filter,
        )

        return scd_elems.specs
