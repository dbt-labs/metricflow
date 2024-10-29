from __future__ import annotations

from typing import Sequence

from typing_extensions import override

from metricflow_semantics.query.group_by_item.candidate_push_down.push_down_visitor import DagTraversalPathTracker
from metricflow_semantics.query.group_by_item.resolution_dag.dag import GroupByItemResolutionDag
from metricflow_semantics.query.group_by_item.resolution_dag.resolution_nodes.base_node import (
    GroupByItemResolutionNode,
    GroupByItemResolutionNodeVisitor,
)
from metricflow_semantics.query.group_by_item.resolution_dag.resolution_nodes.measure_source_node import (
    MeasureGroupByItemSourceNode,
)
from metricflow_semantics.query.group_by_item.resolution_dag.resolution_nodes.metric_resolution_node import (
    MetricGroupByItemResolutionNode,
)
from metricflow_semantics.query.group_by_item.resolution_dag.resolution_nodes.no_metrics_query_source_node import (
    NoMetricsGroupByItemSourceNode,
)
from metricflow_semantics.query.group_by_item.resolution_dag.resolution_nodes.query_resolution_node import (
    QueryGroupByItemResolutionNode,
)
from metricflow_semantics.query.issues.issues_base import MetricFlowQueryResolutionIssueSet
from metricflow_semantics.query.resolver_inputs.query_resolver_inputs import ResolverInputForQuery
from metricflow_semantics.query.validation_rules.base_validation_rule import PostResolutionQueryValidationRule


class PostResolutionQueryValidator:
    """Runs query validation rules after query resolution is complete."""

    def validate_query(
        self,
        resolution_dag: GroupByItemResolutionDag,
        resolver_input_for_query: ResolverInputForQuery,
        validation_rules: Sequence[PostResolutionQueryValidationRule],
    ) -> MetricFlowQueryResolutionIssueSet:
        """Validate according to the list of configured validation rules and return a set containing issues found."""
        validation_visitor = _PostResolutionQueryValidationVisitor(
            resolver_input_for_query=resolver_input_for_query,
            validation_rules=validation_rules,
        )

        return resolution_dag.sink_node.accept(validation_visitor)


class _PostResolutionQueryValidationVisitor(GroupByItemResolutionNodeVisitor[MetricFlowQueryResolutionIssueSet]):
    """Visitor that runs the validation rule when it visits a metric."""

    def __init__(
        self,
        resolver_input_for_query: ResolverInputForQuery,
        validation_rules: Sequence[PostResolutionQueryValidationRule],
    ) -> None:
        self._validation_rules = validation_rules
        self._path_from_start_node_tracker = DagTraversalPathTracker()
        self._resolver_input_for_query = resolver_input_for_query

    def _default_handler(self, node: GroupByItemResolutionNode) -> MetricFlowQueryResolutionIssueSet:
        with self._path_from_start_node_tracker.track_node_visit(node):
            return MetricFlowQueryResolutionIssueSet.merge_iterable(
                parent_node.accept(self) for parent_node in node.parent_nodes
            )

    @override
    def visit_measure_node(self, node: MeasureGroupByItemSourceNode) -> MetricFlowQueryResolutionIssueSet:
        with self._path_from_start_node_tracker.track_node_visit(node) as current_traversal_path:
            issue_sets_to_merge = [parent_node.accept(self) for parent_node in node.parent_nodes]

            for validation_rule in self._validation_rules:
                issue_sets_to_merge.append(
                    validation_rule.validate_measure_in_resolution_dag(
                        measure_reference=node.measure_reference,
                        resolution_path=current_traversal_path,
                    )
                )

            return MetricFlowQueryResolutionIssueSet.merge_iterable(issue_sets_to_merge)

    @override
    def visit_metric_node(self, node: MetricGroupByItemResolutionNode) -> MetricFlowQueryResolutionIssueSet:
        with self._path_from_start_node_tracker.track_node_visit(node) as current_traversal_path:
            issue_sets_to_merge = [parent_node.accept(self) for parent_node in node.parent_nodes]

            for validation_rule in self._validation_rules:
                issue_sets_to_merge.append(
                    validation_rule.validate_metric_in_resolution_dag(
                        metric_reference=node.metric_reference,
                        resolution_path=current_traversal_path,
                    )
                )

            return MetricFlowQueryResolutionIssueSet.merge_iterable(issue_sets_to_merge)

    @override
    def visit_query_node(self, node: QueryGroupByItemResolutionNode) -> MetricFlowQueryResolutionIssueSet:
        with self._path_from_start_node_tracker.track_node_visit(node) as current_traversal_path:
            issue_sets_to_merge = [parent_node.accept(self) for parent_node in node.parent_nodes]

            for validation_rule in self._validation_rules:
                issue_sets_to_merge.append(
                    validation_rule.validate_query_in_resolution_dag(
                        metrics_in_query=node.metrics_in_query,
                        where_filter_intersection=node.where_filter_intersection,
                        resolution_path=current_traversal_path,
                    )
                )

            return MetricFlowQueryResolutionIssueSet.merge_iterable(issue_sets_to_merge)

    @override
    def visit_no_metrics_query_node(self, node: NoMetricsGroupByItemSourceNode) -> MetricFlowQueryResolutionIssueSet:
        return self._default_handler(node)
