from __future__ import annotations

from dataclasses import dataclass
from typing import Optional, Sized, Tuple

from typing_extensions import override

from metricflow.collection_helpers.merger import Mergeable
from metricflow.mf_logging.pretty_print import mf_pformat
from metricflow.query.group_by_item.filter_spec_resolution.filter_spec_lookup import FilterSpecResolutionLookUp
from metricflow.query.group_by_item.resolution_dag.dag import GroupByItemResolutionDag
from metricflow.query.issues.issues_base import MetricFlowQueryResolutionIssueSet
from metricflow.query.resolver_inputs.base_resolver_inputs import MetricFlowQueryResolverInput
from metricflow.specs.specs import MetricFlowQuerySpec


@dataclass(frozen=True)
class InputToIssueSetMappingItem:
    """A mapping item that associates an input to issues generated when resolving the input."""

    resolver_input: MetricFlowQueryResolverInput
    issue_set: MetricFlowQueryResolutionIssueSet


@dataclass(frozen=True)
class InputToIssueSetMapping(Mergeable, Sized):
    """Collection of InputToIssueSetMappingItems.

    This implements Sized to aid mf_pformat(). Sized is used in mf_pformat() when excluding display of empty items.
    """

    items: Tuple[InputToIssueSetMappingItem, ...]

    @property
    def has_issues(self) -> bool:  # noqa: D
        return any(item.issue_set.has_issues for item in self.items)

    @property
    def merged_issue_set(self) -> MetricFlowQueryResolutionIssueSet:
        """Return an issue set that contains all issues in the included mapping items."""
        return MetricFlowQueryResolutionIssueSet.merge_iterable(item.issue_set for item in self.items)

    @override
    def merge(self, other: InputToIssueSetMapping) -> InputToIssueSetMapping:
        return InputToIssueSetMapping(items=self.items + other.items)

    @classmethod
    @override
    def empty_instance(cls) -> InputToIssueSetMapping:
        return InputToIssueSetMapping(
            items=(),
        )

    @staticmethod
    def from_one_item(  # noqa: D
        resolver_input: MetricFlowQueryResolverInput, issue_set: MetricFlowQueryResolutionIssueSet
    ) -> InputToIssueSetMapping:
        return InputToIssueSetMapping(
            items=(InputToIssueSetMappingItem(resolver_input=resolver_input, issue_set=issue_set),)
        )

    @override
    def __len__(self) -> int:
        return len(self.items)


@dataclass(frozen=True)
class MetricFlowQueryResolution:
    """The result of resolving query inputs to specs."""

    # Can be None if there were errors.
    query_spec: Optional[MetricFlowQuerySpec]

    # The resolution DAG generated for the query.
    resolution_dag: Optional[GroupByItemResolutionDag]
    # The lookup that is used later in the DataflowPlanBuilder to figure out which specs are required by the filters
    # in the queries / metrics.
    filter_spec_lookup: FilterSpecResolutionLookUp
    # Mapping of issues with the inputs.
    input_to_issue_set: InputToIssueSetMapping

    @property
    def checked_query_spec(self) -> MetricFlowQuerySpec:
        """Returns the query_spec, but if MetricFlowQueryResolution.has_errors was True, raise a RuntimeError."""
        if self.input_to_issue_set.has_issues:
            raise RuntimeError(
                f"Can't get the query spec because errors were present in the resolution:\n"
                f"{mf_pformat(self.input_to_issue_set.has_issues)}"
            )
        if self.query_spec is None:
            raise RuntimeError("If there were no errors, query_spec should have been populated.")
        return self.query_spec

    @property
    def has_errors(self) -> bool:  # noqa: D
        return self.input_to_issue_set.has_issues or self.filter_spec_lookup.has_errors
