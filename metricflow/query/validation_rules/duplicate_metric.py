from __future__ import annotations

import logging
from typing import Sequence

from dbt_semantic_interfaces.protocols import WhereFilterIntersection
from dbt_semantic_interfaces.references import MetricReference
from typing_extensions import override

from metricflow.model.semantic_manifest_lookup import SemanticManifestLookup
from metricflow.query.group_by_item.resolution_path import MetricFlowQueryResolutionPath
from metricflow.query.issues.issues_base import MetricFlowQueryResolutionIssueSet
from metricflow.query.issues.parsing.duplicate_metric import DuplicateMetricIssue
from metricflow.query.resolver_inputs.query_resolver_inputs import ResolverInputForQuery
from metricflow.query.validation_rules.base_validation_rule import PostResolutionQueryValidationRule

logger = logging.getLogger(__name__)


class DuplicateMetricValidationRule(PostResolutionQueryValidationRule):
    """Validates that a query does not include the same metric multiple times."""

    def __init__(self, manifest_lookup: SemanticManifestLookup) -> None:  # noqa: D
        super().__init__(manifest_lookup=manifest_lookup)

    @override
    def validate_metric_in_resolution_dag(
        self,
        metric_reference: MetricReference,
        resolver_input_for_query: ResolverInputForQuery,
        resolution_path: MetricFlowQueryResolutionPath,
    ) -> MetricFlowQueryResolutionIssueSet:
        return MetricFlowQueryResolutionIssueSet.empty_instance()

    @override
    def validate_query_in_resolution_dag(
        self,
        metrics_in_query: Sequence[MetricReference],
        where_filter_intersection: WhereFilterIntersection,
        resolver_input_for_query: ResolverInputForQuery,
        resolution_path: MetricFlowQueryResolutionPath,
    ) -> MetricFlowQueryResolutionIssueSet:
        duplicate_metric_references = tuple(
            sorted(
                metric_reference
                for metric_reference in set(metrics_in_query)
                if metrics_in_query.count(metric_reference) > 1
            )
        )

        if len(duplicate_metric_references) > 0:
            return MetricFlowQueryResolutionIssueSet.from_issue(
                DuplicateMetricIssue.from_parameters(
                    query_resolution_path=resolution_path, duplicate_metric_references=duplicate_metric_references
                )
            )

        return MetricFlowQueryResolutionIssueSet.empty_instance()
