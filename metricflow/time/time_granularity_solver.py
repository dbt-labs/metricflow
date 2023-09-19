from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import Dict, Optional, Sequence, Tuple

import pandas as pd
from dbt_semantic_interfaces.pretty_print import pformat_big_objects
from dbt_semantic_interfaces.references import (
    EntityReference,
    MeasureReference,
    MetricReference,
    TimeDimensionReference,
)
from dbt_semantic_interfaces.type_enums.time_granularity import TimeGranularity

from metricflow.filters.time_constraint import TimeRangeConstraint
from metricflow.model.semantic_manifest_lookup import SemanticManifestLookup
from metricflow.specs.specs import (
    TimeDimensionSpec,
)
from metricflow.time.date_part import DatePart
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
    date_part: Optional[DatePart] = None


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
    ) -> None:
        self._semantic_manifest_lookup = semantic_manifest_lookup

    def validate_time_granularity(
        self, metric_references: Sequence[MetricReference], time_dimension_specs: Sequence[TimeDimensionSpec]
    ) -> None:
        """Check that the granularity specified for time dimensions is valid with respect to the metrics.

        e.g. throw an error if "ds__week" is specified for a metric with a time granularity of MONTH.
        """
        valid_group_by_elements = self._semantic_manifest_lookup.metric_lookup.linkable_set_for_metrics(
            metric_references=metric_references,
        )

        for time_dimension_spec in time_dimension_specs:
            match_found = False
            for path_key in valid_group_by_elements.path_key_to_linkable_dimensions:
                if (
                    path_key.element_name == time_dimension_spec.element_name
                    and (path_key.entity_links == time_dimension_spec.entity_links)
                    and path_key.time_granularity == time_dimension_spec.time_granularity
                ):
                    match_found = True
                    break
            if not match_found:
                raise RequestTimeGranularityException(
                    f"{time_dimension_spec} is not valid for querying {metric_references}"
                )

    def resolve_granularity_for_partial_time_dimension_specs(
        self,
        metric_references: Sequence[MetricReference],
        partial_time_dimension_specs: Sequence[PartialTimeDimensionSpec],
    ) -> Dict[PartialTimeDimensionSpec, TimeDimensionSpec]:
        """Figure out the lowest granularity possible for the partially specified time dimension specs.

        Returns a dictionary that maps how the partial time dimension spec should be turned into a time dimension spec.
        """
        result: Dict[PartialTimeDimensionSpec, TimeDimensionSpec] = {}
        for partial_time_dimension_spec in partial_time_dimension_specs:
            minimum_time_granularity = self.find_minimum_granularity_for_partial_time_dimension_spec(
                partial_time_dimension_spec=partial_time_dimension_spec, metric_references=metric_references
            )
            result[partial_time_dimension_spec] = TimeDimensionSpec(
                element_name=partial_time_dimension_spec.element_name,
                entity_links=partial_time_dimension_spec.entity_links,
                time_granularity=minimum_time_granularity,
                date_part=partial_time_dimension_spec.date_part,
            )
        return result

    def find_minimum_granularity_for_partial_time_dimension_spec(
        self, partial_time_dimension_spec: PartialTimeDimensionSpec, metric_references: Sequence[MetricReference]
    ) -> TimeGranularity:
        """Find minimum granularity allowed for time dimension when queried with given metrics."""
        valid_group_by_elements = self._semantic_manifest_lookup.metric_lookup.linkable_set_for_metrics(
            metric_references=metric_references,
        )

        minimum_time_granularity: Optional[TimeGranularity] = None
        for path_key in valid_group_by_elements.path_key_to_linkable_dimensions:
            if (
                path_key.element_name == partial_time_dimension_spec.element_name
                and path_key.entity_links == partial_time_dimension_spec.entity_links
                and path_key.time_granularity is not None
            ):
                minimum_time_granularity = (
                    path_key.time_granularity
                    if minimum_time_granularity is None
                    else min(minimum_time_granularity, path_key.time_granularity)
                )

        if not minimum_time_granularity:
            raise RequestTimeGranularityException(
                f"Unable to resolve the time dimension spec for {partial_time_dimension_spec}. "
                f"Valid group by elements are:\n"
                f"{pformat_big_objects([spec.qualified_name for spec in valid_group_by_elements.as_spec_set.as_tuple])}"
            )

        return minimum_time_granularity

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
