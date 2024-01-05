from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import Optional, Sequence, Tuple, Union

from dbt_semantic_interfaces.call_parameter_sets import (
    DimensionCallParameterSet,
    EntityCallParameterSet,
    TimeDimensionCallParameterSet,
)
from dbt_semantic_interfaces.protocols import WhereFilterIntersection
from dbt_semantic_interfaces.references import MetricReference
from typing_extensions import override

from metricflow.collection_helpers.merger import Mergeable
from metricflow.mf_logging.formatting import indent
from metricflow.mf_logging.pretty_print import mf_pformat
from metricflow.query.group_by_item.filter_spec_resolution.filter_location import WhereFilterLocation
from metricflow.query.group_by_item.path_prefixable import PathPrefixable
from metricflow.query.group_by_item.resolution_path import MetricFlowQueryResolutionPath
from metricflow.query.issues.issues_base import MetricFlowQueryResolutionIssueSet
from metricflow.specs.patterns.spec_pattern import SpecPattern
from metricflow.specs.specs import LinkableInstanceSpec

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class FilterSpecResolutionLookUp(Mergeable):
    """Allows you to look up the spec for a group-by-item in a where filter.

    If there are issues parsing the filter, or if the filter had specified an invalid group-by-item, the appropriate
    issue is contained in the issue_set.
    """

    spec_resolutions: Tuple[FilterSpecResolution, ...]
    non_parsable_resolutions: Tuple[NonParsableFilterResolution, ...]

    @property
    def has_errors(self) -> bool:  # noqa: D
        return any(
            non_parsable_resolution.issue_set.has_errors for non_parsable_resolution in self.non_parsable_resolutions
        ) or any(spec_resolution.issue_set.has_errors for spec_resolution in self.spec_resolutions)

    @property
    def has_issues(self) -> bool:  # noqa: D
        return any(
            non_parsable_resolution.issue_set.has_issues for non_parsable_resolution in self.non_parsable_resolutions
        ) or any(spec_resolution.issue_set.has_issues for spec_resolution in self.spec_resolutions)

    def get_spec_resolutions(self, resolved_spec_lookup_key: ResolvedSpecLookUpKey) -> Sequence[FilterSpecResolution]:
        """Return the specs resolutions associated with the given key."""
        return tuple(
            resolution for resolution in self.spec_resolutions if resolution.lookup_key == resolved_spec_lookup_key
        )

    def spec_resolution_exists(self, resolved_spec_lookup_key: ResolvedSpecLookUpKey) -> bool:
        """Returns true if a resolution exists for the given key."""
        return len(self.get_spec_resolutions(resolved_spec_lookup_key)) > 0

    def checked_resolved_spec(self, resolved_spec_lookup_key: ResolvedSpecLookUpKey) -> LinkableInstanceSpec:
        """Returns the resolved spec for the given key.

        If a resolution does not exist, or there is no spec associated with the resolution, this raises a RuntimeError.
        """
        resolutions = self.get_spec_resolutions(resolved_spec_lookup_key)
        if len(resolutions) == 0:
            raise RuntimeError(
                f"Unable to find a resolved spec.\n\n"
                f"Expected 1 resolution for:\n\n"
                f"{indent(mf_pformat(resolved_spec_lookup_key))}\n\n"
                f"but did not find any. All resolutions are:\n\n"
                f"{indent(mf_pformat(self.spec_resolutions))}"
            )

        # There may be multiple resolutions that match a given key because it's possible the same metric / filter is
        # used multiple times in a query (e.g. as a part of a derived metric). However, for a given metric and
        # a CallParameterSet, it should resolve to the same thing. Multiple resolutions are kept in this object for
        # error-reporting purposes.
        resolution = resolutions[0]
        if resolution.resolved_spec is None:
            raise RuntimeError(
                f"Expected a resolution with a resolved spec, but got:\n"
                f"{mf_pformat(resolution)}.\n"
                f"All resolutions are:\n"
                f"{mf_pformat(self.spec_resolutions)}"
            )

        return resolution.resolved_spec

    @override
    def merge(self, other: FilterSpecResolutionLookUp) -> FilterSpecResolutionLookUp:
        return FilterSpecResolutionLookUp(
            spec_resolutions=self.spec_resolutions + other.spec_resolutions,
            non_parsable_resolutions=self.non_parsable_resolutions + other.non_parsable_resolutions,
        )

    @override
    @classmethod
    def empty_instance(cls) -> FilterSpecResolutionLookUp:
        return FilterSpecResolutionLookUp(
            spec_resolutions=(),
            non_parsable_resolutions=(),
        )


@dataclass(frozen=True)
class NonParsableFilterResolution(PathPrefixable):
    """A where filter intersection that couldn't be parsed e.g. Jinja error."""

    filter_location_path: MetricFlowQueryResolutionPath
    where_filter_intersection: WhereFilterIntersection
    issue_set: MetricFlowQueryResolutionIssueSet

    @override
    def with_path_prefix(self, path_prefix: MetricFlowQueryResolutionPath) -> NonParsableFilterResolution:
        return NonParsableFilterResolution(
            filter_location_path=self.filter_location_path.with_path_prefix(path_prefix),
            where_filter_intersection=self.where_filter_intersection,
            issue_set=self.issue_set.with_path_prefix(path_prefix),
        )


@dataclass(frozen=True)
class ResolvedSpecLookUpKey:
    """A key that associates a call parameter set and the filter where it is located."""

    filter_location: WhereFilterLocation
    call_parameter_set: CallParameterSet

    @staticmethod
    def from_parameters(  # noqa: D
        filter_location: WhereFilterLocation, call_parameter_set: CallParameterSet
    ) -> ResolvedSpecLookUpKey:
        return ResolvedSpecLookUpKey(
            filter_location=filter_location,
            call_parameter_set=call_parameter_set,
        )

    @staticmethod
    def for_metric_filter(  # noqa: D
        metric_reference: MetricReference, call_parameter_set: CallParameterSet
    ) -> ResolvedSpecLookUpKey:
        """Create a key related to a filter in a metric definition."""
        return ResolvedSpecLookUpKey(
            filter_location=WhereFilterLocation.for_metric(
                metric_reference,
            ),
            call_parameter_set=call_parameter_set,
        )

    @staticmethod
    def for_query_filter(  # noqa: D
        metrics_in_query: Sequence[MetricReference], call_parameter_set: CallParameterSet
    ) -> ResolvedSpecLookUpKey:
        """Create a key related to a filter for a query."""
        return ResolvedSpecLookUpKey(
            filter_location=WhereFilterLocation.for_query(metrics_in_query),
            call_parameter_set=call_parameter_set,
        )


@dataclass(frozen=True)
class FilterSpecResolution:
    """Associates a lookup key and the resolved spec."""

    lookup_key: ResolvedSpecLookUpKey
    where_filter_intersection: WhereFilterIntersection
    resolved_spec: Optional[LinkableInstanceSpec]
    spec_pattern: SpecPattern
    issue_set: MetricFlowQueryResolutionIssueSet
    # Used for error messages.
    filter_location_path: MetricFlowQueryResolutionPath
    object_builder_str: str


CallParameterSet = Union[DimensionCallParameterSet, TimeDimensionCallParameterSet, EntityCallParameterSet]


@dataclass(frozen=True)
class PatternAssociationForWhereFilterGroupByItem:
    """Describes the pattern associated with a group-by-item in a where filter.

    e.g. "{{ TimeDimension('metric_time', 'day') }} = '2020-01-01'" ->
        GroupByItemInWhereFilter(
            call_parameter_set=TimeDimensionCallParameterSet('metric_time', DAY),
            input_str="TimeDimension('metric_time', 'day')",
            ...
        )
    """

    call_parameter_set: CallParameterSet
    object_builder_str: str
    spec_pattern: SpecPattern
