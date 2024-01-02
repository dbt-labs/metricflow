from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass
from enum import Enum
from typing import Sequence, Sized, Tuple

from typing_extensions import override

from metricflow.collection_helpers.merger import Mergeable
from metricflow.query.group_by_item.path_prefixable import PathPrefixable
from metricflow.query.group_by_item.resolution_path import MetricFlowQueryResolutionPath
from metricflow.query.resolver_inputs.base_resolver_inputs import MetricFlowQueryResolverInput


class MetricFlowQueryIssueType(Enum):
    """Errors prevent the query from running. Later, we'll add warnings."""

    ERROR = "ERROR"


@dataclass(frozen=True)
class MetricFlowQueryIssue:
    """An issue in the query that needs attention from the user."""

    issue_type: MetricFlowQueryIssueType
    # Allows for hierarchical issues that have more detailed context and are more easily generated in a recursive call.
    parent_issues: Tuple[MetricFlowQueryIssue, ...]


@dataclass(frozen=True)
class MetricFlowQueryResolutionIssue(PathPrefixable, ABC):
    """Represents an issue that has come up while resolving user inputs to a query."""

    issue_type: MetricFlowQueryIssueType
    parent_issues: Tuple[MetricFlowQueryResolutionIssue, ...]
    query_resolution_path: MetricFlowQueryResolutionPath

    @abstractmethod
    def ui_description(self, associated_input: MetricFlowQueryResolverInput) -> str:
        """Return a string that describes this issue and is suitable for displaying to the user.

        TBD: Make the associated_input a field of this class.
        """
        raise NotImplementedError


@dataclass(frozen=True)
class MetricFlowQueryResolutionIssueSet(Mergeable, PathPrefixable, Sized):
    """The result of resolving query inputs to specs.

    This implements Sized so that empty issue sets don't have to be printed by mf_pformat().
    """

    issues: Tuple[MetricFlowQueryResolutionIssue, ...] = ()

    @override
    def merge(self, other: MetricFlowQueryResolutionIssueSet) -> MetricFlowQueryResolutionIssueSet:  # noqa: D
        return MetricFlowQueryResolutionIssueSet(issues=tuple(self.issues) + tuple(other.issues))

    @override
    @classmethod
    def empty_instance(cls) -> MetricFlowQueryResolutionIssueSet:
        return MetricFlowQueryResolutionIssueSet()

    @property
    def errors(self) -> Sequence[MetricFlowQueryResolutionIssue]:  # noqa: D
        return tuple(issue for issue in self.issues if issue.issue_type is MetricFlowQueryIssueType.ERROR)

    @property
    def has_errors(self) -> bool:  # noqa: D
        return len(self.errors) > 0

    def add_issue(self, issue: MetricFlowQueryResolutionIssue) -> MetricFlowQueryResolutionIssueSet:
        """Return a new issue set that is this issue set with the given issue added."""
        return MetricFlowQueryResolutionIssueSet(issues=tuple(self.issues) + (issue,))

    @staticmethod
    def from_issue(issue: MetricFlowQueryResolutionIssue) -> MetricFlowQueryResolutionIssueSet:
        """Return an issue set containing only the given issue."""
        return MetricFlowQueryResolutionIssueSet(issues=(issue,))

    @staticmethod
    def from_issues(issues: Sequence[MetricFlowQueryResolutionIssue]) -> MetricFlowQueryResolutionIssueSet:  # noqa: D
        return MetricFlowQueryResolutionIssueSet(issues=tuple(issues))

    @override
    def with_path_prefix(self, path_prefix: MetricFlowQueryResolutionPath) -> MetricFlowQueryResolutionIssueSet:
        return MetricFlowQueryResolutionIssueSet(
            issues=tuple(issue.with_path_prefix(path_prefix) for issue in self.issues),
        )

    @property
    def has_issues(self) -> bool:  # noqa: D
        return len(self.issues) > 0

    @override
    def __len__(self) -> int:
        return len(self.issues)
