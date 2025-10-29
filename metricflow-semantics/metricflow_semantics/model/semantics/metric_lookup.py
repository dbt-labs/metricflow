from __future__ import annotations

import logging
from functools import cached_property
from typing import Dict, Final, Iterable, Sequence

from dbt_semantic_interfaces.enum_extension import assert_values_exhausted
from dbt_semantic_interfaces.naming.keywords import METRIC_TIME_ELEMENT_NAME
from dbt_semantic_interfaces.protocols import MetricInput
from dbt_semantic_interfaces.protocols.metric import Metric, MetricType
from dbt_semantic_interfaces.protocols.semantic_manifest import SemanticManifest
from dbt_semantic_interfaces.references import MetricReference, SemanticModelReference
from dbt_semantic_interfaces.type_enums.time_granularity import TimeGranularity

from metricflow_semantics.errors.error_classes import (
    DuplicateMetricError,
    InvalidManifestException,
    MetricFlowInternalError,
    MetricNotFoundError,
    UnknownMetricError,
)
from metricflow_semantics.model.linkable_element_property import GroupByItemProperty
from metricflow_semantics.model.semantics.element_filter import GroupByItemSetFilter
from metricflow_semantics.model.semantics.linkable_element_set_base import BaseGroupByItemSet
from metricflow_semantics.model.semantics.linkable_spec_resolver import (
    GroupByItemSetResolver,
)
from metricflow_semantics.semantic_graph.attribute_resolution.group_by_item_set import (
    GroupByItemSet,
)
from metricflow_semantics.semantic_graph.lookups.manifest_object_lookup import ManifestObjectLookup
from metricflow_semantics.specs.spec_set import group_specs_by_type
from metricflow_semantics.specs.time_dimension_spec import TimeDimensionSpec
from metricflow_semantics.toolkit.cache.result_cache import ResultCache
from metricflow_semantics.toolkit.collections.ordered_set import FrozenOrderedSet, MutableOrderedSet, OrderedSet
from metricflow_semantics.toolkit.mf_logging.lazy_formattable import LazyFormat

logger = logging.getLogger(__name__)


DEFAULT_COMMON_SET_FILTER: Final[GroupByItemSetFilter] = GroupByItemSetFilter.create(
    any_properties_denylist=(GroupByItemProperty.METRIC,)
)


class MetricLookup:
    """Tracks semantic information for metrics by linking them to semantic models."""

    def __init__(
        self,
        semantic_manifest: SemanticManifest,
        group_by_item_set_resolver: GroupByItemSetResolver,
        manifest_object_lookup: ManifestObjectLookup,
    ) -> None:
        """Initializer.

        Args:
            semantic_manifest: used to fetch and load the metrics and initialize the linkable spec resolver
        """
        self._metrics: Dict[MetricReference, Metric] = {}
        self._manifest_object_lookup = manifest_object_lookup

        for metric in semantic_manifest.metrics:
            metric_reference = MetricReference(element_name=metric.name)
            if metric_reference in self._metrics:
                raise DuplicateMetricError(
                    LazyFormat(
                        "A duplicate metric was found in the manifest",
                        conflicting_metrics=[self._metrics[metric_reference], metric],
                    )
                )
            self._metrics[metric_reference] = metric

        self._group_by_item_set_resolver = group_by_item_set_resolver

        self._result_cache_for_get_min_queryable_time_granularity: ResultCache[
            MetricReference, TimeGranularity
        ] = ResultCache()
        self._result_cache_for_aggregation_time_dimension_specs: ResultCache[
            str, FrozenOrderedSet[TimeDimensionSpec]
        ] = ResultCache()
        self._result_cache_for_derived_from_semantic_models: ResultCache[
            MetricReference, FrozenOrderedSet[SemanticModelReference]
        ] = ResultCache()

    def get_group_by_items_for_distinct_values_query(
        self, set_filter: GroupByItemSetFilter = GroupByItemSetFilter.create()
    ) -> BaseGroupByItemSet:
        """Return the reachable linkable elements for a dimension values query with no metrics."""
        return self._group_by_item_set_resolver.get_set_for_distinct_values_query(set_filter)

    def get_derived_from_semantic_models(self, metric_reference: MetricReference) -> OrderedSet[SemanticModelReference]:
        """Return the semantic models that the given metric is derived from.

        i.e. the set of semantic models where constituent simple metrics are located.
        """
        result_cache = self._result_cache_for_derived_from_semantic_models
        result = result_cache.get(metric_reference)
        if result:
            return result.value

        metric = self.get_metric(metric_reference)
        metric_inputs = MetricLookup.metric_inputs(metric, include_conversion_metric_input=True)

        model_references: MutableOrderedSet[SemanticModelReference] = MutableOrderedSet()
        if len(metric_inputs) == 0:
            metric_aggregation_params = metric.type_params.metric_aggregation_params
            if metric_aggregation_params is None:
                raise MetricFlowInternalError(
                    LazyFormat("Expected `metric_aggregation_params` to be set", metric=metric)
                )
            model_references.add(SemanticModelReference(metric_aggregation_params.semantic_model))
        else:
            model_references.update(
                *(
                    self.get_derived_from_semantic_models(MetricReference(metric_input.name))
                    for metric_input in metric_inputs
                )
            )

        return self._result_cache_for_derived_from_semantic_models.set_and_get(
            metric_reference, model_references.as_frozen()
        )

    def get_common_group_by_items(
        self,
        metric_references: Iterable[MetricReference] = (),
        set_filter: GroupByItemSetFilter = DEFAULT_COMMON_SET_FILTER,
        joins_disallowed: bool = False,
    ) -> BaseGroupByItemSet:
        """Gets the set of the valid group-by items common to all inputs."""
        if set_filter.element_name_allowlist is None:
            return self._group_by_item_set_resolver.get_common_set(
                metric_references=metric_references,
                set_filter=set_filter,
                joins_disallowed=joins_disallowed,
            )

        # If the filter specifies element names, make the call to the resolver without element names to get better
        # cache hit rates.
        filter_without_element_name_condition = set_filter.without_element_name_allowlist()

        result_superset = self._group_by_item_set_resolver.get_common_set(
            metric_references=metric_references,
            set_filter=filter_without_element_name_condition,
            joins_disallowed=joins_disallowed,
        )

        return GroupByItemSet(
            annotated_specs=tuple(
                annotated_spec
                for annotated_spec in result_superset.annotated_specs
                if annotated_spec.element_name in set_filter.element_name_allowlist
            )
        )

    def get_metrics(self, metric_references: Iterable[MetricReference]) -> Sequence[Metric]:  # noqa: D102
        return tuple(self.get_metric(metric_reference) for metric_reference in metric_references)

    @cached_property
    def metric_references(self) -> OrderedSet[MetricReference]:  # noqa: D102
        return FrozenOrderedSet(sorted(self._metrics.keys()))

    def get_metric(self, metric_reference: MetricReference) -> Metric:  # noqa: D102
        metric = self._metrics.get(metric_reference)
        if metric is None:
            raise MetricNotFoundError(LazyFormat("The given metric is not known", metric_reference=metric_reference))
        return metric

    @staticmethod
    def metric_inputs(metric: Metric, include_conversion_metric_input: bool) -> Sequence[MetricInput]:
        """Returns the metric inputs for the given metric."""
        metric_type = metric.type
        metric_inputs: list[MetricInput] = []

        if metric_type is MetricType.SIMPLE:
            pass
        elif metric_type is MetricType.CUMULATIVE:
            cumulative_type_params = metric.type_params.cumulative_type_params
            if cumulative_type_params is None:
                raise MetricFlowInternalError(
                    LazyFormat(
                        "Expected `cumulative_type_params` to be set for a cumulative metric",
                        complex_metric=metric,
                    )
                )

            input_metric_for_cumulative_metric = cumulative_type_params.metric
            if input_metric_for_cumulative_metric is None:
                raise MetricFlowInternalError(
                    LazyFormat(
                        "Expected `metric` to be set for a cumulative metric",
                        complex_metric=metric,
                    )
                )

            metric_inputs.append(input_metric_for_cumulative_metric)
        elif metric_type is MetricType.RATIO:
            numerator = metric.type_params.numerator
            if numerator is None:
                raise MetricFlowInternalError(
                    LazyFormat(
                        "Expected `numerator` to be set for a ratio metric",
                        complex_metric=metric,
                    )
                )
            metric_inputs.append(numerator)

            denominator = metric.type_params.denominator
            if denominator is None:
                raise MetricFlowInternalError(
                    LazyFormat(
                        "Expected `denominator` to be set for a ratio metric",
                        complex_metric=metric,
                    )
                )
            metric_inputs.append(denominator)
        elif metric_type is MetricType.CONVERSION:
            conversion_type_params = metric.type_params.conversion_type_params
            if conversion_type_params is None:
                raise MetricFlowInternalError(
                    LazyFormat(
                        "Expected `conversion_type_params` to be set for a conversion metric",
                        complex_metric=metric,
                    )
                )
            base_metric = conversion_type_params.base_metric
            if base_metric is None:
                raise MetricFlowInternalError(
                    LazyFormat("Expected `base_metric` to be set for a conversion metric", complex_metric=metric)
                )
            metric_inputs.append(base_metric)

            if include_conversion_metric_input:
                conversion_metric = conversion_type_params.conversion_metric
                if conversion_metric is None:
                    raise MetricFlowInternalError(
                        LazyFormat(
                            "Expected `conversion_metric` to be set for a conversion metric", complex_metric=metric
                        )
                    )
                metric_inputs.append(conversion_metric)

        elif metric_type is MetricType.DERIVED:
            metrics = metric.type_params.metrics
            if not metrics:
                raise MetricFlowInternalError(
                    LazyFormat("Expected `metrics` to be set for a derived metric", derived_metric=metric)
                )
            metric_inputs.extend(metrics)
        else:
            assert_values_exhausted(metric_type)

        return metric_inputs

    def get_aggregation_time_dimension_specs(self, metric_reference: MetricReference) -> OrderedSet[TimeDimensionSpec]:
        """Return the time dimension specs that are used for aggregating the simple metric."""
        metric_name = metric_reference.element_name
        cache_key = metric_name
        result = self._result_cache_for_aggregation_time_dimension_specs.get(cache_key)
        if result:
            return result.value

        metric_inputs = MetricLookup.metric_inputs(
            self.get_metric(metric_reference), include_conversion_metric_input=False
        )
        if len(metric_inputs) > 0:
            intersection_result = MutableOrderedSet(
                self.get_aggregation_time_dimension_specs(MetricReference(metric_inputs[0].name))
            )
            for metric_input in metric_inputs[1:]:
                intersection_result.intersection_update(
                    self.get_aggregation_time_dimension_specs(MetricReference(metric_input.name))
                )
        else:
            simple_metric_input = self._manifest_object_lookup.simple_metric_name_to_input.get(metric_name)
            if simple_metric_input is None:
                raise ValueError(
                    LazyFormat(
                        "Unable to find a simple metric with the given name",
                        metric_name=metric_name,
                    )
                )

            group_by_item_set = self.get_common_group_by_items(
                metric_references=(metric_reference,),
                set_filter=GroupByItemSetFilter.create(
                    element_name_allowlist=(simple_metric_input.agg_time_dimension_name, METRIC_TIME_ELEMENT_NAME)
                ),
                joins_disallowed=True,
            )
            spec_set = group_specs_by_type(group_by_item_set.specs)
            intersection_result = MutableOrderedSet(spec_set.time_dimension_specs)

        return self._result_cache_for_aggregation_time_dimension_specs.set_and_get(
            cache_key, intersection_result.as_frozen()
        )

    def get_min_queryable_time_granularity(self, metric_reference: MetricReference) -> TimeGranularity:
        """The minimum grain that can be queried with this metric.

        Maps to the largest granularity defined for any of the metric's agg_time_dimensions.
        """
        cache = self._result_cache_for_get_min_queryable_time_granularity
        cache_key = metric_reference
        result = cache.get(cache_key)
        if result:
            return result.value

        metric = self.get_metric(metric_reference)
        metric_type = metric.type

        agg_time_dimension_grains: set[TimeGranularity] = set()

        if metric_type is MetricType.SIMPLE:
            metric_name = metric_reference.element_name
            simple_metric_input = self._manifest_object_lookup.simple_metric_name_to_input.get(metric_name)
            if simple_metric_input is None:
                raise UnknownMetricError((metric_name,))
            agg_time_dimension_grains.add(simple_metric_input.agg_time_dimension_grain)
        elif (
            metric_type is MetricType.CONVERSION
            or metric_type is MetricType.DERIVED
            or metric_type is MetricType.RATIO
            or metric_type is MetricType.CUMULATIVE
        ):
            metric_inputs = MetricLookup.metric_inputs(metric, include_conversion_metric_input=True)
            if not metric_inputs:
                raise InvalidManifestException(
                    LazyFormat("Expected `metrics` to be non-empty for a non-simple metric", metric=metric)
                )
            agg_time_dimension_grains.update(
                (
                    self.get_min_queryable_time_granularity(MetricReference(metric_input.name))
                    for metric_input in metric_inputs
                )
            )
        else:
            assert_values_exhausted(metric_type)

        if len(agg_time_dimension_grains) == 0:
            raise RuntimeError(
                LazyFormat("Unable to resolve an aggregation-time-dimension grain for the given metric", metric=metric)
            )

        return cache.set_and_get(
            key=cache_key, value=max(agg_time_dimension_grains, key=lambda time_grain: time_grain.to_int())
        )
