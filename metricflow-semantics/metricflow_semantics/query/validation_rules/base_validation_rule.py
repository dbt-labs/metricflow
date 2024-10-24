from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Sequence

from dbt_semantic_interfaces.protocols import WhereFilterIntersection
from dbt_semantic_interfaces.references import MetricReference

from metricflow_semantics.model.semantic_manifest_lookup import SemanticManifestLookup
from metricflow_semantics.query.group_by_item.resolution_path import MetricFlowQueryResolutionPath
from metricflow_semantics.query.issues.issues_base import MetricFlowQueryResolutionIssueSet
from metricflow_semantics.query.resolver_inputs.query_resolver_inputs import ResolverInputForQuery


class PostResolutionQueryValidationRule(ABC):
    """A validation rule that runs after all query inputs have been resolved to specs."""

    def __init__(  # noqa: D107
        self, manifest_lookup: SemanticManifestLookup, resolver_input_for_query: ResolverInputForQuery
    ) -> None:
        self._manifest_lookup = manifest_lookup
        self._resolver_input_for_query = resolver_input_for_query

    @abstractmethod
    def validate_metric_in_resolution_dag(
        self,
        metric_reference: MetricReference,
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
        resolution_path: MetricFlowQueryResolutionPath,
    ) -> MetricFlowQueryResolutionIssueSet:
        """Validate the parameters to the query are valid.

        This will be call once at the query node in the resolution DAG.
        """
        raise NotImplementedError
