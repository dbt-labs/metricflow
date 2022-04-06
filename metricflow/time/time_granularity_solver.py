from __future__ import annotations

import logging
import pandas as pd
from collections import defaultdict, OrderedDict
from dataclasses import dataclass
from typing import Tuple, List, Dict, Sequence, Set, Optional

from metricflow.constraints.time_constraint import TimeRangeConstraint
from metricflow.instances import MetricModelReference
from metricflow.model.semantic_model import SemanticModel
from metricflow.query.query_exceptions import InvalidQueryException
from metricflow.specs import (
    MetricSpec,
    TimeDimensionSpec,
    LinklessIdentifierSpec,
    DEFAULT_TIME_GRANULARITY,
    TimeDimensionReference,
)
from metricflow.model.objects.metric import MetricType
from metricflow.model.objects.user_configured_model import UserConfiguredModel
from metricflow.model.objects.elements.dimension import DimensionType
from metricflow.time.time_granularity import TimeGranularity

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class PartialTimeDimensionSpec:
    """Similar to TimeDimensionSpec, but with an unknown time granularity.

    This is used to represent a time dimension spec from the user before the granularity is figured out.
    """

    element_name: str
    identifier_links: Tuple[LinklessIdentifierSpec, ...]


@dataclass(frozen=True)
class MeasureReference:
    """Refers to a measure where we don't know other attributes."""

    element_name: str


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

    def __init__(self, semantic_model: SemanticModel) -> None:  # noqa: D
        self._semantic_model = semantic_model
        self._metric_reference_to_measure_reference = TimeGranularitySolver._measures_for_metric(
            self._semantic_model.user_configured_model
        )

        self._local_time_dimension_granularities: Dict[
            LocalTimeDimensionGranularityKey, Set[TimeGranularity]
        ] = defaultdict(set)

        for data_source in self._semantic_model.user_configured_model.data_sources:
            for dimension in data_source.dimensions:
                if dimension.type == DimensionType.TIME:
                    config_granularity = dimension.type_params.time_granularity if dimension.type_params else None
                    time_granularity = config_granularity if config_granularity else DEFAULT_TIME_GRANULARITY

                    for measure in data_source.measures:
                        self._local_time_dimension_granularities[
                            LocalTimeDimensionGranularityKey(
                                measure_reference=MeasureReference(element_name=measure.name),
                                local_time_dimension_reference=TimeDimensionReference(
                                    element_name=dimension.name.element_name
                                ),
                            )
                        ].add(time_granularity)

    @staticmethod
    def _measures_for_metric(model: UserConfiguredModel) -> Dict[MetricModelReference, List[MeasureReference]]:
        """Given a model, return a dict that maps the name of the metric to the names of the measures used"""
        metric_reference_to_measure_references: Dict[MetricModelReference, List[MeasureReference]] = {}
        for metric in model.metrics:
            metric_reference_to_measure_references[MetricModelReference(metric_name=metric.name)] = [
                MeasureReference(element_name=measure) for measure in metric.measure_names
            ]

        return metric_reference_to_measure_references

    def local_dimension_granularity_range(
        self, metric_specs: Sequence[MetricSpec], local_time_dimension_reference: TimeDimensionReference
    ) -> Tuple[TimeGranularity, TimeGranularity]:
        """Return the ranges of time granularities for a local dimension associated with the measures in metrics.

        For example, let's say we're querying for two metrics that are based on 'bookings' and 'bookings_monthly'
        respectively.

        The 'bookings' measure defined is in the 'fct_bookings' data source. 'fct_bookings' has a local time dimension
        named 'ds' with granularity DAY.

        The 'monthly_bookings' measure is in defined in the 'fct_bookings_monthly' data source. 'fct_bookings_monthly'
        has a local time dimension named 'ds' with granularity DAY.

        Then this would return [DAY, MONTH].
        """
        all_time_granularities = set()

        for metric_spec in metric_specs:
            for measure_reference in self._metric_reference_to_measure_reference[
                MetricModelReference(metric_name=metric_spec.element_name)
            ]:
                key = LocalTimeDimensionGranularityKey(
                    measure_reference=measure_reference,
                    local_time_dimension_reference=local_time_dimension_reference,
                )
                time_granularities = self._local_time_dimension_granularities[key]

                if len(time_granularities) == 0:
                    raise InvalidQueryException(
                        f"Local dimension {local_time_dimension_reference} does not exist for measure "
                        f"'{measure_reference}' of metric '{metric_spec}"
                    )

                if len(time_granularities) > 1:
                    raise NotImplementedError(
                        "Time granularities not supported for local time dimensions for measures defined in "
                        "multiple data sources. metric: '{metric_name} measure: '{measure_name} "
                    )
                all_time_granularities.add(next(iter(time_granularities)))

        return min(all_time_granularities), max(all_time_granularities)

    def validate_time_granularity(
        self, metric_specs: Sequence[MetricSpec], time_dimension_specs: Sequence[TimeDimensionSpec]
    ) -> None:
        """Check that the granularity specified for time dimensions is valid with respect to the metrics.

        e.g. throw an error if "ds__week" is specified for a metric with a time granularity of MONTH.
        """
        for time_dimension_spec in time_dimension_specs:
            # Validate local time dimensions.
            if time_dimension_spec.identifier_links == ():
                _, min_granularity_for_querying = self.local_dimension_granularity_range(
                    metric_specs=metric_specs,
                    local_time_dimension_reference=time_dimension_spec.reference,
                )
                if time_dimension_spec.time_granularity < min_granularity_for_querying:
                    raise RequestTimeGranularityException(
                        f"The minimum time granularity for querying metrics {metric_specs} is "
                        f"{min_granularity_for_querying}. Got {time_dimension_spec}"
                    )

                # If there is a cumulative metric, granularity changes aren't supported.
                for metric_spec in metric_specs:
                    metric = self._semantic_model.metric_semantics.get_metric(metric_spec)
                    if (
                        metric.type == MetricType.CUMULATIVE
                        and time_dimension_spec.time_granularity != min_granularity_for_querying
                    ):
                        raise RequestTimeGranularityException(
                            f"For querying cumulative metric '{metric_spec.qualified_name}', the granularity of "
                            f"'{time_dimension_spec.qualified_name}' must be {min_granularity_for_querying.name}"
                        )

        # TODO: Validate non-local time dimension granularities here instead of during plan building.

    def resolve_granularity_for_partial_time_dimension_specs(
        self,
        metric_specs: Sequence[MetricSpec],
        partial_time_dimension_specs: Sequence[PartialTimeDimensionSpec],
        primary_time_dimension_reference: TimeDimensionReference,
        time_granularity: Optional[TimeGranularity] = None,
    ) -> Dict[PartialTimeDimensionSpec, TimeDimensionSpec]:
        """Figure out the lowest granularity possible for the partially specified time dimension specs.

        Returns a dictionary that maps how the partial time dimension spec should be turned into a time dimension spec.
        """
        replacement_dict: OrderedDict[PartialTimeDimensionSpec, TimeDimensionSpec] = OrderedDict()
        for partial_time_dimension_spec in partial_time_dimension_specs:
            # Handle local time dimensions
            if partial_time_dimension_spec.identifier_links == ():
                _, min_time_granularity_for_querying = self.local_dimension_granularity_range(
                    metric_specs=metric_specs,
                    local_time_dimension_reference=TimeDimensionReference(
                        element_name=partial_time_dimension_spec.element_name
                    ),
                )

                if (
                    partial_time_dimension_spec.element_name == primary_time_dimension_reference.element_name
                    and time_granularity
                ):
                    if time_granularity < min_time_granularity_for_querying:
                        raise RequestTimeGranularityException(
                            f"Can't use time granularity for time dimension '{primary_time_dimension_reference} since "
                            f"the minimum granularity is {min_time_granularity_for_querying}"
                        )
                    replacement_dict[partial_time_dimension_spec] = TimeDimensionSpec(
                        element_name=partial_time_dimension_spec.element_name,
                        identifier_links=(),
                        time_granularity=time_granularity,
                    )
                else:
                    replacement_dict[partial_time_dimension_spec] = TimeDimensionSpec(
                        element_name=partial_time_dimension_spec.element_name,
                        identifier_links=(),
                        time_granularity=min_time_granularity_for_querying,
                    )
            # Handle joined time dimensions.
            else:
                # TODO: For joined time dimensions, also compute the minimum granularity for querying.
                replacement_dict[partial_time_dimension_spec] = TimeDimensionSpec(
                    element_name=partial_time_dimension_spec.element_name,
                    identifier_links=partial_time_dimension_spec.identifier_links,
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
        if not time_granularity.is_period_start(start_ts):
            constraint_start = time_granularity.adjust_to_start_of_period(start_ts).to_pydatetime()

        end_ts = pd.Timestamp(time_range_constraint.end_time)
        if not time_granularity.is_period_end(end_ts):
            constraint_end = time_granularity.adjust_to_end_of_period(end_ts).to_pydatetime()

        if constraint_start < TimeRangeConstraint.ALL_TIME_BEGIN():
            constraint_start = TimeRangeConstraint.ALL_TIME_BEGIN()
        if constraint_end > TimeRangeConstraint.ALL_TIME_END():
            constraint_end = TimeRangeConstraint.ALL_TIME_END()

        return TimeRangeConstraint(start_time=constraint_start, end_time=constraint_end)


class RequestTimeGranularityException(Exception):
    """Raised when a query is requesting a time granularity that's not possible for the given metrics."""

    pass
