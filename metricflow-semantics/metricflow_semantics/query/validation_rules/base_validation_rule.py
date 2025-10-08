from __future__ import annotations

import typing
from abc import ABC, abstractmethod
from typing import Sequence

from dbt_semantic_interfaces.protocols import WhereFilterIntersection
from dbt_semantic_interfaces.references import MetricReference

from metricflow_semantics.model.semantic_manifest_lookup import SemanticManifestLookup
from metricflow_semantics.query.group_by_item.resolution_path import MetricFlowQueryResolutionPath
from metricflow_semantics.query.issues.issues_base import MetricFlowQueryResolutionIssueSet
from metricflow_semantics.query.resolver_inputs.query_resolver_inputs import ResolverInputForQuery

if typing.TYPE_CHECKING:
    from metricflow_semantics.query.query_resolver import ResolveGroupByItemsResult, ResolveMetricsResult


class PostResolutionQueryValidationRule(ABC):
    """A validation rule that runs after all query inputs have been resolved to specs."""

    def __init__(  # noqa: D107
        self,
        manifest_lookup: SemanticManifestLookup,
        resolver_input_for_query: ResolverInputForQuery,
        resolve_group_by_item_result: ResolveGroupByItemsResult,
        resolve_metric_result: ResolveMetricsResult,
    ) -> None:
        self._manifest_lookup = manifest_lookup
        self._resolver_input_for_query = resolver_input_for_query
        self._resolve_group_by_item_result = resolve_group_by_item_result
        self._resolve_metric_result = resolve_metric_result

    @abstractmethod
    def validate_simple_metric_in_resolution_dag(
        self,
        metric_reference: MetricReference,
        resolution_path: MetricFlowQueryResolutionPath,
    ) -> MetricFlowQueryResolutionIssueSet:
        """Given a simple metric that exists in a resolution DAG, check that the query is valid.

        This is called for each simple metric of a complex metric as the resolution DAG is traversed.
        """
        raise NotImplementedError

    @abstractmethod
    def validate_complex_metric_in_resolution_dag(
        self,
        metric_reference: MetricReference,
        resolution_path: MetricFlowQueryResolutionPath,
    ) -> MetricFlowQueryResolutionIssueSet:
        """Given a complex metric that exists in a resolution DAG, check that the query is valid.

        This is called for each complex metric as the resolution DAG is traversed.
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
