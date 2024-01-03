from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Sequence

from dbt_semantic_interfaces.protocols import Metric, WhereFilterIntersection
from dbt_semantic_interfaces.references import MetricReference

from metricflow.model.semantic_manifest_lookup import SemanticManifestLookup
from metricflow.query.group_by_item.resolution_path import MetricFlowQueryResolutionPath
from metricflow.query.issues.issues_base import MetricFlowQueryResolutionIssueSet
from metricflow.query.resolver_inputs.query_resolver_inputs import ResolverInputForQuery


class PostResolutionQueryValidationRule(ABC):
    """A validation rule that runs after all query inputs have been resolved to specs."""

    def __init__(self, manifest_lookup: SemanticManifestLookup) -> None:  # noqa: D
        self._manifest_lookup = manifest_lookup

    def _get_metric(self, metric_reference: MetricReference) -> Metric:
        return self._manifest_lookup.metric_lookup.get_metric(metric_reference)

    @abstractmethod
    def validate_metric_in_resolution_dag(
        self,
        metric_reference: MetricReference,
        resolver_input_for_query: ResolverInputForQuery,
        resolution_path: MetricFlowQueryResolutionPath,
    ) -> MetricFlowQueryResolutionIssueSet:
        """Given a metric that exists in a resolution DAG, check that the query is valid.

        This is called for each metric as the resolution DAG is traversed.
        """
        raise NotImplementedError

    @abstractmethod
    def validate_query_in_resolution_dag(
        self,
        metrics_in_query: Sequence[MetricReference],
        where_filter_intersection: WhereFilterIntersection,
        resolver_input_for_query: ResolverInputForQuery,
        resolution_path: MetricFlowQueryResolutionPath,
    ) -> MetricFlowQueryResolutionIssueSet:
        """Validate the parameters to the query are valid.

        This will be call once at the query node in the resolution DAG.
        """
        raise NotImplementedError
