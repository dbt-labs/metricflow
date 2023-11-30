from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import List, Optional, Sequence, Set, Tuple, Union

from dbt_semantic_interfaces.call_parameter_sets import (
    DimensionCallParameterSet,
    EntityCallParameterSet,
    TimeDimensionCallParameterSet,
)
from dbt_semantic_interfaces.references import MetricReference
from typing_extensions import override

from metricflow.collection_helpers.merger import Mergeable
from metricflow.collection_helpers.pretty_print import mf_pformat
from metricflow.formatting import indent_log_line
from metricflow.query.group_by_item.filter_spec_resolution.filter_location import WhereFilterLocation
from metricflow.query.group_by_item.path_prefixable import PathPrefixable
from metricflow.query.group_by_item.resolution_dag.resolution_nodes.base_node import GroupByItemResolutionNode
from metricflow.query.group_by_item.resolution_path import MetricFlowQueryResolutionPath
from metricflow.query.issues.issues_base import MetricFlowQueryResolutionIssueSet
from metricflow.specs.specs import LinkableInstanceSpec

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class FilterSpecResolutionLookUp(Mergeable, PathPrefixable):
    """Allows you to look up the spec for a group-by-item in a where filter.

    If there are issues parsing the filter, or if the filter had specified an invalid group-by-item, the appropriate
    issue is contained in the issue_set.
    """

    spec_resolutions: Tuple[FilterSpecResolution, ...]
    issue_set: MetricFlowQueryResolutionIssueSet

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
                f"{indent_log_line(mf_pformat(resolved_spec_lookup_key))}\n\n"
                f"but did not find any. All resolutions are:\n\n"
                f"{indent_log_line(mf_pformat(self.spec_resolutions))}"
            )

        if len(resolutions) > 1:
            raise RuntimeError(
                f"Got multiple resolved specs.\n\n"
                f"Expected 1 resolution for:\n\n"
                f"{indent_log_line(mf_pformat(resolved_spec_lookup_key))}\n\n"
                f"but got:\n\n"
                f"{indent_log_line(mf_pformat(resolutions))}.\n\n"
                f"All resolutions are:\n\n"
                f"{indent_log_line(mf_pformat(self.spec_resolutions))}"
            )

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
            issue_set=self.issue_set.merge(other.issue_set),
        )

    @override
    @classmethod
    def empty_instance(cls) -> FilterSpecResolutionLookUp:
        return FilterSpecResolutionLookUp(
            spec_resolutions=(),
            issue_set=MetricFlowQueryResolutionIssueSet.empty_instance(),
        )

    @override
    def with_path_prefix(self, path_prefix_node: GroupByItemResolutionNode) -> FilterSpecResolutionLookUp:
        return FilterSpecResolutionLookUp(
            spec_resolutions=tuple(
                resolution.with_path_prefix(path_prefix_node) for resolution in self.spec_resolutions
            ),
            issue_set=self.issue_set.with_path_prefix(path_prefix_node),
        )

    def dedupe(self) -> FilterSpecResolutionLookUp:  # noqa: D
        deduped_spec_resolutions: List[FilterSpecResolution] = []
        deduped_lookup_keys: Set[ResolvedSpecLookUpKey] = set()
        for spec_resolution in self.spec_resolutions:
            if spec_resolution.lookup_key in deduped_lookup_keys:
                continue

            deduped_spec_resolutions.append(spec_resolution)
            deduped_lookup_keys.add(spec_resolution.lookup_key)

        return FilterSpecResolutionLookUp(spec_resolutions=tuple(deduped_spec_resolutions), issue_set=self.issue_set)


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
class FilterSpecResolution(PathPrefixable):
    """Associates a lookup key and the resolved spec."""

    lookup_key: ResolvedSpecLookUpKey
    resolution_path: MetricFlowQueryResolutionPath
    resolved_spec: Optional[LinkableInstanceSpec]

    @override
    def with_path_prefix(self, path_prefix_node: GroupByItemResolutionNode) -> FilterSpecResolution:
        return FilterSpecResolution(
            lookup_key=self.lookup_key,
            resolution_path=self.resolution_path.with_path_prefix(path_prefix_node),
            resolved_spec=self.resolved_spec,
        )


CallParameterSet = Union[DimensionCallParameterSet, TimeDimensionCallParameterSet, EntityCallParameterSet]
