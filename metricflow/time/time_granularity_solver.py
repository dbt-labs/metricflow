from __future__ import annotations

import logging
from collections import OrderedDict, defaultdict
from dataclasses import dataclass
from typing import Dict, List, Optional, Sequence, Set, Tuple

import pandas as pd
from dbt_semantic_interfaces.protocols.metric import MetricType
from dbt_semantic_interfaces.protocols.semantic_manifest import SemanticManifest
from dbt_semantic_interfaces.references import (
    EntityReference,
    MeasureReference,
    MetricModelReference,
    MetricReference,
    TimeDimensionReference,
)
from dbt_semantic_interfaces.type_enums.time_granularity import TimeGranularity

from metricflow.dataflow.builder.node_data_set import DataflowPlanNodeOutputDataSetResolver
from metricflow.dataflow.dataflow_plan import BaseOutput
from metricflow.dataset.semantic_model_adapter import SemanticModelDataSet
from metricflow.filters.time_constraint import TimeRangeConstraint
from metricflow.model.semantic_manifest_lookup import SemanticManifestLookup
from metricflow.query.query_exceptions import InvalidQueryException
from metricflow.specs.specs import (
    DEFAULT_TIME_GRANULARITY,
    TimeDimensionSpec,
)
from metricflow.time.time_granularity import (
    adjust_to_end_of_period,
    adjust_to_start_of_period,
    is_period_end,
    is_period_start,
)

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class PartialTimeDimensionSpec:
    """Similar to TimeDimensionSpec, but with an unknown time granularity.

    This is used to represent a time dimension spec from the user before the granularity is figured out.
    """

    element_name: str
    entity_links: Tuple[EntityReference, ...]


@dataclass(frozen=True)
class LocalTimeDimensionGranularityKey:
    """Used to associate a measure and a local time dimension without being specific about a granularity."""

    measure_reference: MeasureReference
    local_time_dimension_reference: TimeDimensionReference


class TimeGranularitySolver:
    """Figures out time granularities for metrics.

    1. Given the list of metrics, what's the smallest time granularity that's common across those metrics?
    2. Given the list of metrics and the dimensions that the user specified, does the primary time dimension need to be
    converted into another granularity? e.g. if the metrics have different time granularities and smallest time
    granularity for the given metrics is MONTH and the user specified ds, then we need to convert ds to ds__month.
    3. Also checks to see if the time granularity expressed in dimensions is valid. e.g. if a metric has a time
    granularity of MONTH, throw an exception if ds__day is in the requested dimensions.
    """

    def __init__(  # noqa: D
        self,
        semantic_manifest_lookup: SemanticManifestLookup,
        source_nodes: Sequence[BaseOutput[SemanticModelDataSet]],
        node_output_resolver: DataflowPlanNodeOutputDataSetResolver[SemanticModelDataSet],
    ) -> None:
        self._semantic_manifest_lookup = semantic_manifest_lookup
        self._metric_reference_to_measure_reference = TimeGranularitySolver._measures_for_metric(
            self._semantic_manifest_lookup.semantic_manifest
        )

        self._local_time_dimension_granularities: Dict[
            LocalTimeDimensionGranularityKey, Set[TimeGranularity]
        ] = defaultdict(set)

        for source_node in source_nodes:
            output_data_set = node_output_resolver.get_output_data_set(source_node)
            for time_dimension_instance in output_data_set.instance_set.time_dimension_instances:
                time_dimension_spec = time_dimension_instance.spec
                if len(time_dimension_spec.entity_links) == 0:
                    for measure_instance in output_data_set.instance_set.measure_instances:
                        self._local_time_dimension_granularities[
                            LocalTimeDimensionGranularityKey(
                                measure_reference=measure_instance.spec.as_reference,
                                local_time_dimension_reference=time_dimension_spec.reference,
                            )
                        ].add(time_dimension_spec.time_granularity)

    @staticmethod
    def _measures_for_metric(model: SemanticManifest) -> Dict[MetricModelReference, List[MeasureReference]]:
        """Given a model, return a dict that maps the name of the metric to the names of the measures used."""
        metric_reference_to_measure_references: Dict[MetricModelReference, List[MeasureReference]] = {}
        for metric in model.metrics:
            metric_reference_to_measure_references[MetricModelReference(metric_name=metric.name)] = [
                MeasureReference(element_name=measure.element_name) for measure in metric.measure_references
            ]

        return metric_reference_to_measure_references

    def local_dimension_granularity_range(
        self, metric_references: Sequence[MetricReference], local_time_dimension_reference: TimeDimensionReference
    ) -> Tuple[TimeGranularity, TimeGranularity]:
        """Return the ranges of time granularities for a local dimension associated with the measures in metrics.

        For example, let's say we're querying for two metrics that are based on 'bookings' and 'bookings_monthly'
        respectively.

        The 'bookings' measure defined is in the 'fct_bookings' semantic model. 'fct_bookings' has a local time dimension
        named 'ds' with granularity DAY.

        The 'monthly_bookings' measure is in defined in the 'fct_bookings_monthly' semantic model. 'fct_bookings_monthly'
        has a local time dimension named 'ds' with granularity MONTH.

        Then this would return [DAY, MONTH].
        """
        all_time_granularities = set()

        for metric_reference in metric_references:
            for measure_reference in self._metric_reference_to_measure_reference[
                MetricModelReference(metric_name=metric_reference.element_name)
            ]:
                key = LocalTimeDimensionGranularityKey(
                    measure_reference=measure_reference,
                    local_time_dimension_reference=local_time_dimension_reference,
                )
                valid_time_granularities_for_measure = self._local_time_dimension_granularities[key]

                if len(valid_time_granularities_for_measure) == 0:
                    raise InvalidQueryException(
                        f"Local dimension {local_time_dimension_reference} does not exist for measure "
                        f"'{measure_reference}' of metric '{metric_reference}"
                    )

                all_time_granularities.add(min(valid_time_granularities_for_measure))

        return min(all_time_granularities), max(all_time_granularities)

    def validate_time_granularity(
        self, metric_references: Sequence[MetricReference], time_dimension_specs: Sequence[TimeDimensionSpec]
    ) -> None:
        """Check that the granularity specified for time dimensions is valid with respect to the metrics.

        e.g. throw an error if "ds__week" is specified for a metric with a time granularity of MONTH.
        """
        for time_dimension_spec in time_dimension_specs:
            # Validate local time dimensions.
            if time_dimension_spec.entity_links == ():
                _, min_granularity_for_querying = self.local_dimension_granularity_range(
                    metric_references=metric_references,
                    local_time_dimension_reference=time_dimension_spec.reference,
                )
                if time_dimension_spec.time_granularity < min_granularity_for_querying:
                    raise RequestTimeGranularityException(
                        f"The minimum time granularity for querying metrics {metric_references} is "
                        f"{min_granularity_for_querying}. Got {time_dimension_spec}"
                    )

                # If there is a cumulative metric, granularity changes aren't supported. We need to check the granularity
                # specified in the configs for the cumulative metric alone, since `min_granularity_for_querying` may not be supported.
                for metric_reference in metric_references:
                    metric = self._semantic_manifest_lookup.metric_lookup.get_metric(metric_reference)
                    if metric.type == MetricType.CUMULATIVE:
                        _, only_queryable_granularity = self.local_dimension_granularity_range(
                            metric_references=[metric_reference],
                            local_time_dimension_reference=time_dimension_spec.reference,
                        )
                        if time_dimension_spec.time_granularity != only_queryable_granularity:
                            raise RequestTimeGranularityException(
                                f"For querying cumulative metric '{metric_reference.element_name}', the granularity of "
                                f"'{time_dimension_spec.qualified_name}' must be {only_queryable_granularity.name}"
                            )

        # TODO: Validate non-local time dimension granularities here instead of during plan building.

    def resolve_granularity_for_partial_time_dimension_specs(
        self,
        metric_references: Sequence[MetricReference],
        partial_time_dimension_specs: Sequence[PartialTimeDimensionSpec],
        metric_time_dimension_reference: TimeDimensionReference,
        time_granularity: Optional[TimeGranularity] = None,
    ) -> Dict[PartialTimeDimensionSpec, TimeDimensionSpec]:
        """Figure out the lowest granularity possible for the partially specified time dimension specs.

        Returns a dictionary that maps how the partial time dimension spec should be turned into a time dimension spec.
        """
        replacement_dict: OrderedDict[PartialTimeDimensionSpec, TimeDimensionSpec] = OrderedDict()
        for partial_time_dimension_spec in partial_time_dimension_specs:
            # Handle local time dimensions
            if partial_time_dimension_spec.entity_links == ():
                _, min_time_granularity_for_querying = self.local_dimension_granularity_range(
                    metric_references=metric_references,
                    local_time_dimension_reference=TimeDimensionReference(
                        element_name=partial_time_dimension_spec.element_name
                    ),
                )

                if (
                    partial_time_dimension_spec.element_name == metric_time_dimension_reference.element_name
                    and time_granularity
                ):
                    if time_granularity < min_time_granularity_for_querying:
                        raise RequestTimeGranularityException(
                            f"Can't use time granularity for time dimension '{metric_time_dimension_reference} since "
                            f"the minimum granularity is {min_time_granularity_for_querying}"
                        )
                    replacement_dict[partial_time_dimension_spec] = TimeDimensionSpec(
                        element_name=partial_time_dimension_spec.element_name,
                        entity_links=(),
                        time_granularity=time_granularity,
                    )
                else:
                    replacement_dict[partial_time_dimension_spec] = TimeDimensionSpec(
                        element_name=partial_time_dimension_spec.element_name,
                        entity_links=(),
                        time_granularity=min_time_granularity_for_querying,
                    )
            # Handle joined time dimensions.
            else:
                # TODO: For joined time dimensions, also compute the minimum granularity for querying.
                replacement_dict[partial_time_dimension_spec] = TimeDimensionSpec(
                    element_name=partial_time_dimension_spec.element_name,
                    entity_links=partial_time_dimension_spec.entity_links,
                    time_granularity=DEFAULT_TIME_GRANULARITY,
                )
        return replacement_dict

    def adjust_time_range_to_granularity(
        self, time_range_constraint: TimeRangeConstraint, time_granularity: TimeGranularity
    ) -> TimeRangeConstraint:
        """Change the time range so that the ends are at the ends of the appropriate time granularity windows.

        e.g. [2020-01-15, 2020-2-15] with MONTH granularity -> [2020-01-01, 2020-02-29]
        """
        constraint_start = time_range_constraint.start_time
        constraint_end = time_range_constraint.end_time

        start_ts = pd.Timestamp(time_range_constraint.start_time)
        if not is_period_start(time_granularity, start_ts):
            constraint_start = adjust_to_start_of_period(time_granularity, start_ts).to_pydatetime()

        end_ts = pd.Timestamp(time_range_constraint.end_time)
        if not is_period_end(time_granularity, end_ts):
            constraint_end = adjust_to_end_of_period(time_granularity, end_ts).to_pydatetime()

        if constraint_start < TimeRangeConstraint.ALL_TIME_BEGIN():
            constraint_start = TimeRangeConstraint.ALL_TIME_BEGIN()
        if constraint_end > TimeRangeConstraint.ALL_TIME_END():
            constraint_end = TimeRangeConstraint.ALL_TIME_END()

        return TimeRangeConstraint(start_time=constraint_start, end_time=constraint_end)


class RequestTimeGranularityException(Exception):
    """Raised when a query is requesting a time granularity that's not possible for the given metrics."""

    pass
