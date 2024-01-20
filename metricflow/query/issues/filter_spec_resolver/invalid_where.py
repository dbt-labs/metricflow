from __future__ import annotations

import traceback
from dataclasses import dataclass

from dbt_semantic_interfaces.protocols import WhereFilter
from typing_extensions import override

from metricflow.mf_logging.formatting import indent
from metricflow.query.group_by_item.resolution_path import MetricFlowQueryResolutionPath
from metricflow.query.issues.issues_base import (
    MetricFlowQueryIssueType,
    MetricFlowQueryResolutionIssue,
)
from metricflow.query.resolver_inputs.base_resolver_inputs import MetricFlowQueryResolverInput


@dataclass(frozen=True)
class WhereFilterParsingIssue(MetricFlowQueryResolutionIssue):
    """Describes an issue with a query where one of the where filters can't be parsed."""

    where_filter: WhereFilter
    parse_exception: Exception

    @staticmethod
    def from_parameters(  # noqa: D
        where_filter: WhereFilter,
        parse_exception: Exception,
        query_resolution_path: MetricFlowQueryResolutionPath,
    ) -> WhereFilterParsingIssue:
        return WhereFilterParsingIssue(
            issue_type=MetricFlowQueryIssueType.ERROR,
            parent_issues=(),
            query_resolution_path=query_resolution_path,
            where_filter=where_filter,
            parse_exception=parse_exception,
        )

    @override
    def ui_description(self, associated_input: MetricFlowQueryResolverInput) -> str:
        return (
            f"Error parsing where filter:\n\n"
            f"{indent(repr(self.where_filter.where_sql_template))}\n\n"
            f"Got exception:\n\n"
            f"{indent(''.join(traceback.TracebackException.from_exception(self.parse_exception).format()))}"
        )

    @override
    def with_path_prefix(self, path_prefix: MetricFlowQueryResolutionPath) -> WhereFilterParsingIssue:
        return WhereFilterParsingIssue(
            issue_type=self.issue_type,
            parent_issues=tuple(issue.with_path_prefix(path_prefix) for issue in self.parent_issues),
            query_resolution_path=self.query_resolution_path.with_path_prefix(path_prefix),
            where_filter=self.where_filter,
            parse_exception=self.parse_exception,
        )
