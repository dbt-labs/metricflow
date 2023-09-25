from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
from typing import Sequence

from dbt_semantic_interfaces.references import MetricReference
from typing_extensions import override


class MetricFlowQueryIssueType(Enum):
    """Errors prevent the query from running, where warnings do not."""

    WARNING = "WARNING"
    ERROR = "ERROR"


@dataclass(frozen=True)
class MetricFlowQueryResolutionIssue:
    """An issue in the query that should be resolved."""

    issue_type: MetricFlowQueryIssueType
    message: str
    metric_path: Sequence[MetricReference] = ()

    @override
    def __str__(self) -> str:
        if not self.metric_path:
            return f"{self.issue_type.value} - {self.message}"
        readable_metric_path = str([metric_reference.element_name for metric_reference in self.metric_path])
        return f"{self.issue_type.value} - {readable_metric_path} - {self.message}"

    def with_additional_metric_path_prefix(  # noqa: D
        self, metric_reference: MetricReference
    ) -> MetricFlowQueryResolutionIssue:
        return MetricFlowQueryResolutionIssue(
            issue_type=self.issue_type, message=self.message, metric_path=(metric_reference,) + tuple(self.metric_path)
        )


@dataclass(frozen=True)
class MetricFlowQueryIssueSet:
    """The result of resolving query inputs to specs."""

    issues: Sequence[MetricFlowQueryResolutionIssue] = ()

    def with_additional_metric_path_prefix(self, metric_reference: MetricReference) -> MetricFlowQueryIssueSet:
        """Return a new issue set where the existing metric paths are prefixed with the given metric."""
        return MetricFlowQueryIssueSet(
            issues=tuple(issue.with_additional_metric_path_prefix(metric_reference) for issue in self.issues)
        )

    def merge(self, other: MetricFlowQueryIssueSet) -> MetricFlowQueryIssueSet:  # noqa: D
        return MetricFlowQueryIssueSet(issues=tuple(self.issues) + tuple(other.issues))

    @property
    def errors(self) -> Sequence[MetricFlowQueryResolutionIssue]:  # noqa: D
        return tuple(issue for issue in self.issues if issue.issue_type is MetricFlowQueryIssueType.ERROR)
