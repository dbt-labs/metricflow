from __future__ import annotations

import logging
from typing import List, Sequence, Set

from dbt_semantic_interfaces.protocols import WhereFilterIntersection
from dbt_semantic_interfaces.references import MetricReference
from typing_extensions import override

from metricflow_semantics.query.group_by_item.resolution_path import MetricFlowQueryResolutionPath
from metricflow_semantics.query.issues.issues_base import MetricFlowQueryResolutionIssueSet
from metricflow_semantics.query.issues.parsing.duplicate_column_names import DuplicateOutputColumnIssue
from metricflow_semantics.query.validation_rules.base_validation_rule import PostResolutionQueryValidationRule
from metricflow_semantics.specs.dunder_column_association_resolver import DunderColumnAssociationResolver

logger = logging.getLogger(__name__)


class UniqueOutputColumnValidationRule(PostResolutionQueryValidationRule):
    """Validates that a query does not include the same output column multiple times."""

    @override
    def validate_complex_metric_in_resolution_dag(
        self,
        metric_reference: MetricReference,
        resolution_path: MetricFlowQueryResolutionPath,
    ) -> MetricFlowQueryResolutionIssueSet:
        return MetricFlowQueryResolutionIssueSet.empty_instance()

    @override
    def validate_query_in_resolution_dag(
        self,
        metrics_in_query: Sequence[MetricReference],
        where_filter_intersection: WhereFilterIntersection,
        resolution_path: MetricFlowQueryResolutionPath,
    ) -> MetricFlowQueryResolutionIssueSet:
        column_association_resolver = DunderColumnAssociationResolver()

        output_column_names: Set[str] = set()
        duplicate_column_names: List[str] = []
        for spec in self._resolve_metric_result.metric_specs + self._resolve_group_by_item_result.group_by_item_specs:
            output_column_name = column_association_resolver.resolve_spec(spec).column_name
            if output_column_name in output_column_names:
                duplicate_column_names.append(output_column_name)
            output_column_names.add(output_column_name)

        if duplicate_column_names:
            return MetricFlowQueryResolutionIssueSet.from_issue(
                DuplicateOutputColumnIssue.from_parameters(
                    duplicate_column_names=duplicate_column_names, query_resolution_path=resolution_path
                ),
            )

        return MetricFlowQueryResolutionIssueSet.empty_instance()

    @override
    def validate_simple_metric_in_resolution_dag(
        self,
        metric_reference: MetricReference,
        resolution_path: MetricFlowQueryResolutionPath,
    ) -> MetricFlowQueryResolutionIssueSet:
        return MetricFlowQueryResolutionIssueSet.empty_instance()
