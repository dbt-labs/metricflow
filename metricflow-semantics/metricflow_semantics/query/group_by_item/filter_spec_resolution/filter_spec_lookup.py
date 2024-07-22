from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import TYPE_CHECKING, Optional, Sequence, Tuple, Union

from dbt_semantic_interfaces.call_parameter_sets import (
    DimensionCallParameterSet,
    EntityCallParameterSet,
    MetricCallParameterSet,
    TimeDimensionCallParameterSet,
)
from dbt_semantic_interfaces.protocols import WhereFilterIntersection
from typing_extensions import override

from metricflow_semantics.collection_helpers.merger import Mergeable
from metricflow_semantics.mf_logging.formatting import indent
from metricflow_semantics.mf_logging.pretty_print import mf_pformat
from metricflow_semantics.model.semantics.linkable_element import LinkableElement
from metricflow_semantics.query.group_by_item.filter_spec_resolution.filter_location import WhereFilterLocation
from metricflow_semantics.query.group_by_item.path_prefixable import PathPrefixable
from metricflow_semantics.query.group_by_item.resolution_path import MetricFlowQueryResolutionPath
from metricflow_semantics.query.issues.issues_base import MetricFlowQueryResolutionIssueSet
from metricflow_semantics.specs.patterns.spec_pattern import SpecPattern

if TYPE_CHECKING:
    from metricflow_semantics.model.semantics.linkable_element_set import LinkableElementSet
    from metricflow_semantics.specs.instance_spec import LinkableInstanceSpec

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
    def has_errors(self) -> bool:  # noqa: D102
        return any(
            non_parsable_resolution.issue_set.has_errors for non_parsable_resolution in self.non_parsable_resolutions
        ) or any(spec_resolution.issue_set.has_errors for spec_resolution in self.spec_resolutions)

    @property
    def has_issues(self) -> bool:  # noqa: D102
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

    def _checked_resolution(self, resolved_spec_lookup_key: ResolvedSpecLookUpKey) -> FilterSpecResolution:
        """Helper to get just the resolution so we can access different properties on it."""
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

        return resolution

    def checked_resolved_spec(self, resolved_spec_lookup_key: ResolvedSpecLookUpKey) -> LinkableInstanceSpec:
        """Returns the resolved spec for the given key.

        If a resolution does not exist, or there is no spec associated with the resolution, this raises a RuntimeError.
        """
        resolution = self._checked_resolution(resolved_spec_lookup_key=resolved_spec_lookup_key)
        resolved_spec = resolution.resolved_spec
        assert (
            resolved_spec is not None
        ), f"Typechecker hint. Expected a resolution with a resolved spec, but got:\n{mf_pformat(resolution)}"
        return resolved_spec

    def checked_resolved_linkable_elements(
        self, resolved_spec_lookup_key: ResolvedSpecLookUpKey
    ) -> Sequence[LinkableElement]:
        """Returns the sequence of LinkableElements for the given spec lookup key.

        These are the LinkableElements bound to the singular spec/path_key for a given resolved filter item. They are
        useful for propagating metadata about the origin semantic model across the boundary between the filter resolver
        and the DataflowPlanBuilder.
        """
        return self._checked_resolution(resolved_spec_lookup_key=resolved_spec_lookup_key).resolved_linkable_elements

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
    def from_parameters(  # noqa: D102
        filter_location: WhereFilterLocation, call_parameter_set: CallParameterSet
    ) -> ResolvedSpecLookUpKey:
        return ResolvedSpecLookUpKey(
            filter_location=filter_location,
            call_parameter_set=call_parameter_set,
        )


@dataclass(frozen=True)
class FilterSpecResolution:
    """Associates a lookup key with the resolved spec and its associated LinkableElements.

    We store the LinkableElementSet as a convenience container for managing the relationship between the
    singular resolved LinkableInstanceSpec and the collection of LinkableElements. This allows us to
    do things like fetch the full set of semantic models where the inputs for the given spec are defined.
    In most cases, there would be exactly one such semantic model, but in cases where, for example, an entity
    is used as a join key there might be multiple semantic model inputs for the given element, and we would
    need to be able to track that information.
    """

    lookup_key: ResolvedSpecLookUpKey
    where_filter_intersection: WhereFilterIntersection
    resolved_linkable_element_set: LinkableElementSet
    spec_pattern: SpecPattern
    issue_set: MetricFlowQueryResolutionIssueSet
    # Used for error messages.
    filter_location_path: MetricFlowQueryResolutionPath
    object_builder_str: str

    def __post_init__(self) -> None:
        """Validation to ensure there is exactly one resolved spec for a FilterSpecResolution.

        Due to the way the FilterSpecResolution is structured, the final output should contain a single spec.
        """
        num_specs = len(self.resolved_linkable_element_set.specs)
        assert num_specs <= 1, (
            f"Found {num_specs} in {self.resolved_linkable_element_set}, but a valid FilterSpecResolution should "
            "contain either 0 or 1 resolved specs."
        )

    @property
    def resolved_spec(self) -> Optional[LinkableInstanceSpec]:
        """Returns the lone resolved spec, if one was found.

        The final ValueError should not be reachable due to the post-init validation, but is in place in case someone
        updates or removes the latter without accounting for the possibility of runtime divergence.
        """
        specs = self.resolved_linkable_element_set.specs
        if len(specs) == 0:
            return None
        elif len(specs) == 1:
            return specs[0]
        else:
            raise ValueError(
                f"Found {len(specs)} in {self.resolved_linkable_element_set}, this should not be possible!"
            )

    @property
    def resolved_linkable_elements(self) -> Sequence[LinkableElement]:
        """Returns the resolved linkable elements, if any, for this resolution result."""
        resolved_spec = self.resolved_spec
        if resolved_spec is None:
            return tuple()

        return self.resolved_linkable_element_set.linkable_elements_for_path_key(resolved_spec.element_path_key)


CallParameterSet = Union[
    DimensionCallParameterSet, TimeDimensionCallParameterSet, EntityCallParameterSet, MetricCallParameterSet
]


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
