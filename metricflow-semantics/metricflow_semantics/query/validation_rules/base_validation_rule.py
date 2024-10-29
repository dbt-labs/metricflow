from __future__ import annotations

import typing
from abc import ABC, abstractmethod
from typing import Sequence

from dbt_semantic_interfaces.protocols import WhereFilterIntersection
from dbt_semantic_interfaces.references import MeasureReference, MetricReference

from metricflow_semantics.model.semantic_manifest_lookup import SemanticManifestLookup
from metricflow_semantics.query.group_by_item.resolution_path import MetricFlowQueryResolutionPath
from metricflow_semantics.query.issues.issues_base import MetricFlowQueryResolutionIssueSet
from metricflow_semantics.query.resolver_inputs.query_resolver_inputs import ResolverInputForQuery

if typing.TYPE_CHECKING:
    from metricflow_semantics.query.query_resolver import ResolveGroupByItemsResult


class PostResolutionQueryValidationRule(ABC):
    """A validation rule that runs after all query inputs have been resolved to specs."""

    def __init__(  # noqa: D107
        self,
        manifest_lookup: SemanticManifestLookup,
        resolver_input_for_query: ResolverInputForQuery,
        resolve_group_by_item_result: ResolveGroupByItemsResult,
    ) -> None:
        self._manifest_lookup = manifest_lookup
        self._resolver_input_for_query = resolver_input_for_query
        self._resolve_group_by_item_result = resolve_group_by_item_result

    @abstractmethod
    def validate_measure_in_resolution_dag(
        self,
        measure_reference: MeasureReference,
        resolution_path: MetricFlowQueryResolutionPath,
    ) -> MetricFlowQueryResolutionIssueSet:
        """Given a measure that exists in a resolution DAG, check that the query is valid.

        This is called for each measure of a base metric as the resolution DAG is traversed.
        """
        raise NotImplementedError

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
