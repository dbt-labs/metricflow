from __future__ import annotations

from collections import defaultdict
from typing import Dict, List, Sequence, Set

from dbt_semantic_interfaces.references import MetricReference
from dbt_semantic_interfaces.type_enums import TimeGranularity
from typing_extensions import override

from metricflow_semantics.model.semantics.metric_lookup import MetricLookup
from metricflow_semantics.specs.patterns.metric_time_pattern import MetricTimePattern
from metricflow_semantics.specs.patterns.spec_pattern import SpecPattern
from metricflow_semantics.specs.spec_classes import (
    InstanceSpec,
    LinkableInstanceSpec,
    TimeDimensionSpec,
    TimeDimensionSpecComparisonKey,
    TimeDimensionSpecField,
)
from metricflow_semantics.specs.spec_set import group_specs_by_type


class DefaultTimeGranularityPattern(SpecPattern):
    """A pattern that matches linkable specs, but for time dimension specs, only the one with the default granularity.

    # TODO: this might need to change - the below is only relevant for metric_time, right? Update this for all dims.
    When queried with metrics, default_granularity is specified in the YAML spec for the metrics. If this field is not
    set for a metric, it will default to DAY unless the minimum granularity is larger than DAY, in which case it will
    default to the minimum granularity. When multiple metrics are queried, the default granularity for the query will
    be the largest of the default granularities for the metrics queried.

    Same defaults apply if no metrics are queried: default to DAY if available for the time dimension queried, else the
    smallest available granularity. Always defaults to DAY for metric_time if not metrics are queried.

    e.g.

    inputs:
        [
            TimeDimensionSpec('metric_time', 'day'),
            TimeDimensionSpec('metric_time', 'month.'),
            DimensionSpec('listing__country'),
        ]

    matches:
        [
            TimeDimensionSpec('metric_time', 'day'),
            DimensionSpec('listing__country'),
        ]

    The finest grain represents the defined grain of the time dimension in the semantic model when evaluating specs
    of the source.

    This pattern helps to implement matching of group-by-items. An ambiguously specified group-by-item can only match to
    time dimension spec with the base grain.
    """

    def __init__(
        self,
        metric_lookup: MetricLookup,
        only_apply_for_metric_time: bool = False,
        queried_metrics: Sequence[MetricReference] = (),
    ) -> None:
        """Args:
            only_apply_for_metric_time: If set, only remove time dimension specs with a non-base grain if it's for
                metric_time. This is useful for resolving the default_granularity that metric_time should default to for
                a given set of metrics. This is typically set to True when resolving query parameters, and False when
                showing suggested group by items for a query (to avoid duplicates of the same time dimension in the list
                of suggestions).
            queried_metrics: The metrics in cluded in the query. This is used to resolve the default_granularity, which
                is set in the metric YAML spec.

        TODO: This is a little odd. This can be replaced once composite patterns are supported.
        """
        self._metric_lookup = metric_lookup
        self._only_apply_for_metric_time = only_apply_for_metric_time
        self._queried_metrics = queried_metrics

    @override
    def match(self, candidate_specs: Sequence[InstanceSpec]) -> Sequence[InstanceSpec]:
        if self._only_apply_for_metric_time:
            metric_time_specs = MetricTimePattern().match(candidate_specs)
            other_specs = tuple(spec for spec in candidate_specs if spec not in metric_time_specs)

            return other_specs + tuple(
                DefaultTimeGranularityPattern(
                    metric_lookup=self._metric_lookup,
                    only_apply_for_metric_time=False,
                    queried_metrics=self._queried_metrics,
                ).match(metric_time_specs)
            )

        spec_set = group_specs_by_type(candidate_specs)

        spec_key_to_grains: Dict[TimeDimensionSpecComparisonKey, Set[TimeGranularity]] = defaultdict(set)
        spec_key_to_specs: Dict[TimeDimensionSpecComparisonKey, List[TimeDimensionSpec]] = defaultdict(list)
        for time_dimension_spec in spec_set.time_dimension_specs:
            spec_key = time_dimension_spec.comparison_key(exclude_fields=(TimeDimensionSpecField.TIME_GRANULARITY,))
            spec_key_to_grains[spec_key].add(time_dimension_spec.time_granularity)
            spec_key_to_specs[spec_key].append(time_dimension_spec)

        default_granularity_for_metrics = self._metric_lookup.get_default_granularity_for_metrics(self._queried_metrics)
        matched_time_dimension_specs: List[TimeDimensionSpec] = []
        for spec_key, time_grains in spec_key_to_grains.items():
            grain_to_use = (
                default_granularity_for_metrics
                if (default_granularity_for_metrics and spec_key.source_spec.is_metric_time)
                else min(time_grains)
            )
            matched_time_dimension_specs.append(spec_key_to_specs[spec_key][0].with_grain(grain_to_use))

        matching_specs: Sequence[LinkableInstanceSpec] = (
            spec_set.dimension_specs
            + tuple(matched_time_dimension_specs)
            + spec_set.entity_specs
            + spec_set.group_by_metric_specs
        )

        return matching_specs
