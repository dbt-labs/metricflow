from __future__ import annotations

import itertools
import logging
import time
from collections import defaultdict
from collections.abc import Set
from dataclasses import dataclass
from typing import Dict, List, Optional, Sequence, Tuple

from dbt_semantic_interfaces.references import MetricReference, SemanticModelReference

from metricflow_semantics.errors.error_classes import InvalidManifestException
from metricflow_semantics.model.semantic_manifest_lookup import SemanticManifestLookup
from metricflow_semantics.model.semantic_model_derivation import SemanticModelDerivation
from metricflow_semantics.model.semantics.linkable_element_set_base import BaseGroupByItemSet
from metricflow_semantics.naming.metric_scheme import MetricNamingScheme
from metricflow_semantics.query.group_by_item.filter_spec_resolution.filter_pattern_factory import (
    WhereFilterPatternFactory,
)
from metricflow_semantics.query.group_by_item.filter_spec_resolution.filter_spec_lookup import (
    FilterSpecResolutionLookUp,
)
from metricflow_semantics.query.group_by_item.filter_spec_resolution.filter_spec_resolver import (
    WhereFilterSpecResolver,
)
from metricflow_semantics.query.group_by_item.group_by_item_resolver import GroupByItemResolution, GroupByItemResolver
from metricflow_semantics.query.group_by_item.resolution_dag.dag import GroupByItemResolutionDag
from metricflow_semantics.query.group_by_item.resolution_dag.dag_builder import GroupByItemResolutionDagBuilder
from metricflow_semantics.query.group_by_item.resolution_dag.resolution_nodes.query_resolution_node import (
    QueryGroupByItemResolutionNode,
)
from metricflow_semantics.query.group_by_item.resolution_path import MetricFlowQueryResolutionPath
from metricflow_semantics.query.issues.issues_base import (
    MetricFlowQueryResolutionIssueSet,
)
from metricflow_semantics.query.issues.parsing.invalid_apply_group_by import InvalidApplyGroupByIssue
from metricflow_semantics.query.issues.parsing.invalid_limit import InvalidLimitIssue
from metricflow_semantics.query.issues.parsing.invalid_metric import InvalidMetricIssue
from metricflow_semantics.query.issues.parsing.invalid_min_max_only import InvalidMinMaxOnlyIssue
from metricflow_semantics.query.issues.parsing.invalid_order import InvalidOrderByItemIssue
from metricflow_semantics.query.issues.parsing.no_metric_or_group_by import NoMetricOrGroupByIssue
from metricflow_semantics.query.query_resolution import (
    InputToIssueSetMapping,
    InputToIssueSetMappingItem,
    MetricFlowQueryResolution,
)
from metricflow_semantics.query.resolver_inputs.query_resolver_inputs import (
    ResolverInputForApplyGroupBy,
    ResolverInputForGroupByItem,
    ResolverInputForLimit,
    ResolverInputForMetric,
    ResolverInputForMinMaxOnly,
    ResolverInputForOrderByItem,
    ResolverInputForQuery,
    ResolverInputForQueryLevelWhereFilterIntersection,
    ResolverInputForWhereFilterIntersection,
)
from metricflow_semantics.query.suggestion_generator import QueryItemSuggestionGenerator
from metricflow_semantics.query.validation_rules.duplicate_metric import DuplicateMetricValidationRule
from metricflow_semantics.query.validation_rules.metric_time_requirements import MetricTimeQueryValidationRule
from metricflow_semantics.query.validation_rules.query_validator import PostResolutionQueryValidator
from metricflow_semantics.query.validation_rules.unique_column_names import UniqueOutputColumnValidationRule
from metricflow_semantics.semantic_graph.attribute_resolution.group_by_item_set import (
    GroupByItemSet,
)
from metricflow_semantics.specs.instance_spec import InstanceSpec, LinkableInstanceSpec
from metricflow_semantics.specs.metric_spec import MetricSpec
from metricflow_semantics.specs.order_by_spec import OrderBySpec
from metricflow_semantics.specs.patterns.spec_pattern import SpecPattern
from metricflow_semantics.specs.query_spec import MetricFlowQuerySpec
from metricflow_semantics.specs.spec_set import group_specs_by_type
from metricflow_semantics.toolkit.collections.ordered_set import MutableOrderedSet
from metricflow_semantics.toolkit.mf_logging.lazy_formattable import LazyFormat
from metricflow_semantics.toolkit.mf_logging.pretty_print import mf_pformat
from metricflow_semantics.toolkit.mf_logging.runtime import log_runtime

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class ResolveOrderByResult:
    """Result of resolving order-by inputs to specs."""

    order_by_specs: Tuple[OrderBySpec, ...]
    input_to_issue_set_mapping: InputToIssueSetMapping


@dataclass(frozen=True)
class ResolveMetricsResult:
    """Result of resolving a metric input to specs."""

    metric_specs: Tuple[MetricSpec, ...]
    input_to_issue_set_mapping: InputToIssueSetMapping


@dataclass(frozen=True)
class ResolveLimitResult:
    """Result of resolving a limit input."""

    limit: Optional[int]
    input_to_issue_set_mapping: InputToIssueSetMapping


@dataclass(frozen=True)
class ResolveMinMaxOnlyResult:
    """Result of resolving a limit input."""

    min_max_only: bool
    input_to_issue_set_mapping: InputToIssueSetMapping


@dataclass(frozen=True)
class ResolveApplyGroupByResult:
    """Result of resolving a apply group by input."""

    apply_group_by: bool
    input_to_issue_set_mapping: InputToIssueSetMapping


@dataclass(frozen=True)
class ResolveGroupByItemsResult:
    """Result of resolving group-by inputs to specs."""

    resolution_dag: GroupByItemResolutionDag
    group_by_item_specs: Tuple[LinkableInstanceSpec, ...]
    input_to_issue_set_mapping: InputToIssueSetMapping
    linkable_element_set: BaseGroupByItemSet


@dataclass(frozen=True)
class ResolveMetricOrGroupByItemsResult:
    """Result of checking that there are metrics or group by items in the query."""

    input_to_issue_set_mapping: InputToIssueSetMapping


class MetricFlowQueryResolver:
    """Resolves inputs to a query (e.g. metrics, group by items into concrete specs."""

    def __init__(  # noqa: D107
        self,
        manifest_lookup: SemanticManifestLookup,
        where_filter_pattern_factory: WhereFilterPatternFactory,
    ) -> None:
        self._manifest_lookup = manifest_lookup
        self._post_resolution_query_validator = PostResolutionQueryValidator()
        self._where_filter_pattern_factory = where_filter_pattern_factory

    @staticmethod
    def _resolve_has_metric_or_group_by_inputs(
        resolver_input_for_query: ResolverInputForQuery, query_resolution_path: MetricFlowQueryResolutionPath
    ) -> ResolveMetricOrGroupByItemsResult:
        if len(resolver_input_for_query.metric_inputs) == 0 and len(resolver_input_for_query.group_by_item_inputs) == 0:
            return ResolveMetricOrGroupByItemsResult(
                input_to_issue_set_mapping=InputToIssueSetMapping.from_one_item(
                    resolver_input=resolver_input_for_query,
                    issue_set=MetricFlowQueryResolutionIssueSet.from_issue(
                        NoMetricOrGroupByIssue.from_parameters(
                            resolver_input_for_query=resolver_input_for_query,
                            query_resolution_path=query_resolution_path,
                        ),
                    ),
                )
            )
        return ResolveMetricOrGroupByItemsResult(input_to_issue_set_mapping=InputToIssueSetMapping.empty_instance())

    @staticmethod
    def _resolve_group_by_item_input(
        group_by_item_input: ResolverInputForGroupByItem,
        group_by_item_resolver: GroupByItemResolver,
    ) -> GroupByItemResolution:
        suggestion_generator = QueryItemSuggestionGenerator(
            input_naming_scheme=group_by_item_input.input_obj_naming_scheme,
            input_str=str(group_by_item_input.input_obj),
            candidate_filters=QueryItemSuggestionGenerator.GROUP_BY_ITEM_CANDIDATE_FILTERS,
        )
        resolution = group_by_item_resolver.resolve_matching_item_for_querying(
            spec_pattern=group_by_item_input.spec_pattern,
            suggestion_generator=suggestion_generator,
        )
        if group_by_item_input.alias:
            resolution = resolution.with_alias(group_by_item_input.alias)
        return resolution

    def _resolve_metric_input(
        self, spec_pattern: SpecPattern, available_metric_specs: Sequence[MetricSpec]
    ) -> List[MetricSpec]:
        start_time = time.perf_counter()
        matching_specs = set(spec_pattern.match(available_metric_specs))

        filtered_metric_specs: List[MetricSpec] = []
        for metric_spec in available_metric_specs:
            if metric_spec in matching_specs:
                filtered_metric_specs.append(metric_spec)

        logger.debug(LazyFormat(lambda: f"Filtering valid metrics took: {time.perf_counter() - start_time:.2f}s"))
        return filtered_metric_specs

    def _resolve_metric_inputs(
        self,
        metric_inputs: Sequence[ResolverInputForMetric],
        query_resolution_path: MetricFlowQueryResolutionPath,
    ) -> ResolveMetricsResult:
        """Resolve the metric specs associated with the given inputs.

        The order of outputs should be the same as the order of inputs.
        """
        # Build a list of metrics that are available from the manifest.
        available_metric_specs = tuple(
            MetricSpec.from_reference(metric_reference)
            for metric_reference in self._manifest_lookup.metric_lookup.metric_references
        )
        metric_specs: List[MetricSpec] = []
        input_to_issue_set_mapping_items: List[InputToIssueSetMappingItem] = []
        alias_to_metrics: Dict[str, List[Tuple[ResolverInputForMetric, MetricReference]]] = defaultdict(list)

        # Find the metric that matches the metric pattern from the input.
        for metric_input in metric_inputs:
            matching_specs = self._resolve_metric_input(
                spec_pattern=metric_input.spec_pattern, available_metric_specs=available_metric_specs
            )
            if len(matching_specs) == 1:
                matching_spec = matching_specs[0]
                alias = metric_input.alias
                if alias:
                    matching_spec = matching_spec.with_alias(alias)
                metric_specs.append(matching_spec)
                resolved_name = matching_spec.alias or matching_spec.dunder_name
                alias_to_metrics[resolved_name].append((metric_input, matching_spec.reference))
            else:
                suggestion_generator = QueryItemSuggestionGenerator(
                    input_naming_scheme=MetricNamingScheme(),
                    input_str=str(metric_input.input_obj),
                    candidate_filters=(),
                )
                metric_suggestions = suggestion_generator.input_suggestions(candidate_specs=available_metric_specs)
                input_to_issue_set_mapping_items.append(
                    InputToIssueSetMappingItem(
                        resolver_input=metric_input,
                        issue_set=MetricFlowQueryResolutionIssueSet.from_issue(
                            InvalidMetricIssue.from_parameters(
                                metric_suggestions=metric_suggestions,
                                query_resolution_path=query_resolution_path,
                            )
                        ),
                    )
                )

        return ResolveMetricsResult(
            metric_specs=tuple(metric_specs),
            input_to_issue_set_mapping=InputToIssueSetMapping(items=tuple(input_to_issue_set_mapping_items)),
        )

    def _resolve_group_by_items_result(
        self,
        metric_references: Sequence[MetricReference],
        group_by_item_inputs: Sequence[ResolverInputForGroupByItem],
        filter_input: ResolverInputForQueryLevelWhereFilterIntersection,
    ) -> ResolveGroupByItemsResult:
        """Resolve the group-by-item specs associated with the given inputs.

        The order of items in the result should be the same as the order of inputs.
        """
        resolution_dag_builder = GroupByItemResolutionDagBuilder(
            manifest_lookup=self._manifest_lookup,
        )
        resolution_dag = resolution_dag_builder.build(
            metric_references=metric_references,
            where_filter_intersection=filter_input.where_filter_intersection,
        )
        logger.debug(LazyFormat(lambda: f"Resolution DAG is:\n{resolution_dag.structure_text()}"))

        group_by_item_resolver = GroupByItemResolver(
            manifest_lookup=self._manifest_lookup,
            resolution_dag=resolution_dag,
        )

        input_to_issue_set_mapping_items: List[InputToIssueSetMappingItem] = []
        group_by_item_specs: List[LinkableInstanceSpec] = []
        linkable_element_sets: List[BaseGroupByItemSet] = []

        for group_by_item_input in group_by_item_inputs:
            resolution = MetricFlowQueryResolver._resolve_group_by_item_input(
                group_by_item_resolver=group_by_item_resolver,
                group_by_item_input=group_by_item_input,
            )
            if resolution.issue_set.has_issues:
                input_to_issue_set_mapping_items.append(
                    InputToIssueSetMappingItem(resolver_input=group_by_item_input, issue_set=resolution.issue_set)
                )
            if resolution.spec is not None:
                group_by_item_specs.append(resolution.spec)
                linkable_element_sets.append(resolution.linkable_element_set)

        linkable_element_set: BaseGroupByItemSet
        if len(linkable_element_sets) == 0:
            linkable_element_set = GroupByItemSet()
        else:
            linkable_element_set = linkable_element_sets[0].union(*linkable_element_sets[1:])

        return ResolveGroupByItemsResult(
            resolution_dag=resolution_dag,
            group_by_item_specs=tuple(group_by_item_specs),
            input_to_issue_set_mapping=InputToIssueSetMapping(tuple(input_to_issue_set_mapping_items)),
            linkable_element_set=linkable_element_set,
        )

    @staticmethod
    def _resolve_order_by(
        resolver_inputs_for_order_by_items: Sequence[ResolverInputForOrderByItem],
        metric_specs: Sequence[MetricSpec],
        group_by_item_specs: Sequence[LinkableInstanceSpec],
        query_resolution_path: MetricFlowQueryResolutionPath,
    ) -> ResolveOrderByResult:
        mapping_items: List[InputToIssueSetMappingItem] = []
        order_by_specs: List[OrderBySpec] = []

        # Match the pattern from the order by input to one of the metric or group-by-item specs.
        # The pattern needs to be used because there are cases where the order-by-item is specified in a different way
        # from the group-by-item, so an equality comparison won't work.
        for resolver_input_for_order_by in resolver_inputs_for_order_by_items:
            matching_specs: set[InstanceSpec] = set()
            for possible_input in resolver_input_for_order_by.possible_inputs:
                spec_pattern = possible_input.spec_pattern
                matching_specs.update(spec_pattern.match(metric_specs))
                matching_specs.update(spec_pattern.match(group_by_item_specs))

            if len(matching_specs) != 1:
                mapping_items.append(
                    InputToIssueSetMappingItem(
                        resolver_input=resolver_input_for_order_by,
                        issue_set=MetricFlowQueryResolutionIssueSet.from_issue(
                            InvalidOrderByItemIssue.from_parameters(
                                order_by_item_input=resolver_input_for_order_by,
                                query_resolution_path=query_resolution_path,
                            )
                        ),
                    )
                )
            else:
                order_by_specs.append(
                    OrderBySpec(
                        # Ignore aliases in the order by since we'll render the expression instead of the alias.
                        instance_spec=matching_specs.pop().with_alias(None),
                        descending=resolver_input_for_order_by.descending,
                    )
                )

        return ResolveOrderByResult(
            input_to_issue_set_mapping=InputToIssueSetMapping(items=tuple(mapping_items)),
            order_by_specs=tuple(order_by_specs),
        )

    @staticmethod
    def _resolve_limit_input(
        limit_input: ResolverInputForLimit, query_resolution_path: MetricFlowQueryResolutionPath
    ) -> ResolveLimitResult:
        limit = limit_input.limit
        if limit is not None and limit < 0:
            return ResolveLimitResult(
                limit=limit,
                input_to_issue_set_mapping=InputToIssueSetMapping.from_one_item(
                    resolver_input=limit_input,
                    issue_set=MetricFlowQueryResolutionIssueSet.from_issue(
                        InvalidLimitIssue.from_parameters(limit=limit, query_resolution_path=query_resolution_path),
                    ),
                ),
            )
        return ResolveLimitResult(limit=limit, input_to_issue_set_mapping=InputToIssueSetMapping.empty_instance())

    @staticmethod
    def _resolve_min_max_only_input(
        min_max_only_input: ResolverInputForMinMaxOnly,
        query_resolution_path: MetricFlowQueryResolutionPath,
        metric_inputs: Tuple[ResolverInputForMetric, ...],
        group_by_item_inputs: Tuple[ResolverInputForGroupByItem, ...],
        order_by_item_inputs: Tuple[ResolverInputForOrderByItem, ...],
        limit_input: ResolverInputForLimit,
    ) -> ResolveMinMaxOnlyResult:
        min_max_only = min_max_only_input.min_max_only
        if min_max_only:
            if (
                metric_inputs
                or order_by_item_inputs
                or (limit_input.limit is not None)
                or len(group_by_item_inputs) != 1
            ):
                return ResolveMinMaxOnlyResult(
                    min_max_only=min_max_only,
                    input_to_issue_set_mapping=InputToIssueSetMapping.from_one_item(
                        resolver_input=min_max_only_input,
                        issue_set=MetricFlowQueryResolutionIssueSet.from_issue(
                            InvalidMinMaxOnlyIssue.from_parameters(
                                min_max_only=min_max_only, query_resolution_path=query_resolution_path
                            ),
                        ),
                    ),
                )
        return ResolveMinMaxOnlyResult(
            min_max_only=min_max_only, input_to_issue_set_mapping=InputToIssueSetMapping.empty_instance()
        )

    @staticmethod
    def _resolve_apply_group_by_input(
        apply_group_by_input: ResolverInputForApplyGroupBy,
        metric_inputs: Tuple[ResolverInputForMetric, ...],
        query_resolution_path: MetricFlowQueryResolutionPath,
    ) -> ResolveApplyGroupByResult:
        if metric_inputs and not apply_group_by_input.apply_group_by:
            return ResolveApplyGroupByResult(
                apply_group_by=apply_group_by_input.apply_group_by,
                input_to_issue_set_mapping=InputToIssueSetMapping.from_one_item(
                    resolver_input=apply_group_by_input,
                    issue_set=MetricFlowQueryResolutionIssueSet.from_issue(
                        InvalidApplyGroupByIssue.from_parameters(
                            apply_group_by=apply_group_by_input.apply_group_by,
                            metric_inputs=metric_inputs,
                            query_resolution_path=query_resolution_path,
                        ),
                    ),
                ),
            )
        return ResolveApplyGroupByResult(
            apply_group_by=apply_group_by_input.apply_group_by,
            input_to_issue_set_mapping=InputToIssueSetMapping.empty_instance(),
        )

    def _build_filter_spec_lookup(
        self,
        resolution_dag: GroupByItemResolutionDag,
    ) -> FilterSpecResolutionLookUp:
        where_filter_spec_resolver = WhereFilterSpecResolver(
            manifest_lookup=self._manifest_lookup,
            resolution_dag=resolution_dag,
            spec_pattern_factory=self._where_filter_pattern_factory,
        )

        return where_filter_spec_resolver.resolve_lookup()

    def resolve_query(self, resolver_input_for_query: ResolverInputForQuery) -> MetricFlowQueryResolution:
        """Resolve the query into specs that can be passed into the next stage in query processing."""
        # Workaround for a Pycharm type inspection issue with decorators.
        # noinspection PyArgumentList
        return self._resolve_query(resolver_input_for_query=resolver_input_for_query)

    @log_runtime()
    def _resolve_query(self, resolver_input_for_query: ResolverInputForQuery) -> MetricFlowQueryResolution:
        metric_inputs = resolver_input_for_query.metric_inputs
        group_by_item_inputs = resolver_input_for_query.group_by_item_inputs
        order_by_item_inputs = resolver_input_for_query.order_by_item_inputs
        limit_input = resolver_input_for_query.limit_input
        query_level_filter_input = resolver_input_for_query.filter_input
        min_max_only_input = resolver_input_for_query.min_max_only
        apply_group_by_input = resolver_input_for_query.apply_group_by

        mappings_to_merge: List[InputToIssueSetMapping] = []

        # Resolve metrics.
        resolve_metrics_result = self._resolve_metric_inputs(
            metric_inputs=metric_inputs,
            query_resolution_path=MetricFlowQueryResolutionPath.empty_instance(),
        )
        mappings_to_merge.append(resolve_metrics_result.input_to_issue_set_mapping)
        metric_specs = resolve_metrics_result.metric_specs

        # Define a resolution path for issues where the input is considered to be the whole query.
        query_resolution_path = MetricFlowQueryResolutionPath.from_path_item(
            QueryGroupByItemResolutionNode.create(
                parent_nodes=(),
                metrics_in_query=tuple(metric_spec.reference for metric_spec in metric_specs),
                where_filter_intersection=query_level_filter_input.where_filter_intersection,
            )
        )

        # Check that the query contains metrics or group by items.
        resolve_metrics_or_group_by_result = self._resolve_has_metric_or_group_by_inputs(
            resolver_input_for_query=resolver_input_for_query, query_resolution_path=query_resolution_path
        )
        mappings_to_merge.append(resolve_metrics_or_group_by_result.input_to_issue_set_mapping)

        # Resolve limit
        resolve_limit_result = self._resolve_limit_input(
            limit_input=limit_input,
            query_resolution_path=query_resolution_path,
        )
        mappings_to_merge.append(resolve_limit_result.input_to_issue_set_mapping)

        # Resolve min max only
        resolve_min_max_only_result = self._resolve_min_max_only_input(
            min_max_only_input=min_max_only_input,
            query_resolution_path=query_resolution_path,
            metric_inputs=metric_inputs,
            group_by_item_inputs=group_by_item_inputs,
            order_by_item_inputs=order_by_item_inputs,
            limit_input=limit_input,
        )
        mappings_to_merge.append(resolve_min_max_only_result.input_to_issue_set_mapping)

        # Resolve apply group by
        resolve_apply_group_by_result = self._resolve_apply_group_by_input(
            apply_group_by_input=apply_group_by_input,
            metric_inputs=metric_inputs,
            query_resolution_path=query_resolution_path,
        )
        mappings_to_merge.append(resolve_apply_group_by_result.input_to_issue_set_mapping)

        # Early stop before resolving further as with invalid metrics, the errors won't be as useful.
        issue_set_mapping_so_far = InputToIssueSetMapping.merge_iterable(mappings_to_merge)
        if issue_set_mapping_so_far.has_issues:
            return MetricFlowQueryResolution(
                query_spec=None,
                resolution_dag=None,
                filter_spec_lookup=FilterSpecResolutionLookUp.empty_instance(),
                input_to_issue_set=issue_set_mapping_so_far,
                queried_semantic_models=(),
            )

        # Resolve group by items.
        resolve_group_by_item_result = self._resolve_group_by_items_result(
            metric_references=tuple(metric_spec.reference for metric_spec in metric_specs),
            group_by_item_inputs=group_by_item_inputs,
            filter_input=query_level_filter_input,
        )
        resolution_dag = resolve_group_by_item_result.resolution_dag
        group_by_item_specs = resolve_group_by_item_result.group_by_item_specs
        mappings_to_merge.append(resolve_group_by_item_result.input_to_issue_set_mapping)

        # Resolve order by.
        resolve_order_by_result = MetricFlowQueryResolver._resolve_order_by(
            resolver_inputs_for_order_by_items=order_by_item_inputs,
            metric_specs=metric_specs,
            group_by_item_specs=group_by_item_specs,
            query_resolution_path=query_resolution_path,
        )
        order_by_specs = resolve_order_by_result.order_by_specs
        if resolve_order_by_result.input_to_issue_set_mapping.has_issues:
            mappings_to_merge.append(resolve_order_by_result.input_to_issue_set_mapping)

        # Resolve all where filters in the DAG and generate mappings if there are issues.
        filter_spec_lookup = self._build_filter_spec_lookup(resolution_dag)
        for filter_spec_resolution in filter_spec_lookup.spec_resolutions:
            if filter_spec_resolution.issue_set.has_issues:
                mappings_to_merge.append(
                    InputToIssueSetMapping.from_one_item(
                        resolver_input=ResolverInputForWhereFilterIntersection(
                            filter_resolution_path=filter_spec_resolution.filter_location_path,
                            where_filter_intersection=filter_spec_resolution.where_filter_intersection,
                            object_builder_str=filter_spec_resolution.object_builder_str,
                        ),
                        issue_set=filter_spec_resolution.issue_set,
                    )
                )

        for non_parseable_resolution in filter_spec_lookup.non_parsable_resolutions:
            filter_resolver_input = ResolverInputForWhereFilterIntersection(
                filter_resolution_path=non_parseable_resolution.filter_location_path,
                where_filter_intersection=non_parseable_resolution.where_filter_intersection,
                object_builder_str=None,
            )
            mappings_to_merge.append(
                InputToIssueSetMapping.from_one_item(
                    resolver_input=filter_resolver_input,
                    issue_set=non_parseable_resolution.issue_set,
                )
            )

        # Return if there are any errors as issues generated by later validations may not make sense otherwise.
        issue_set_mapping_so_far = InputToIssueSetMapping.merge_iterable(mappings_to_merge)
        if issue_set_mapping_so_far.has_issues:
            return MetricFlowQueryResolution(
                query_spec=None,
                resolution_dag=resolution_dag,
                filter_spec_lookup=filter_spec_lookup,
                input_to_issue_set=issue_set_mapping_so_far,
                queried_semantic_models=(),
            )

        # No errors.
        linkable_spec_set = group_specs_by_type(group_by_item_specs)
        logger.debug(
            LazyFormat(lambda: f"Group-by-items were resolved to:\n{mf_pformat(linkable_spec_set.linkable_specs)}")
        )

        # Run post-resolution validation rules to generate issues that are generated at the query-level.
        query_level_issue_set = self._post_resolution_query_validator.validate_query(
            resolution_dag=resolution_dag,
            resolver_input_for_query=resolver_input_for_query,
            validation_rules=(
                MetricTimeQueryValidationRule(
                    manifest_lookup=self._manifest_lookup,
                    resolver_input_for_query=resolver_input_for_query,
                    resolve_group_by_item_result=resolve_group_by_item_result,
                    resolve_metric_result=resolve_metrics_result,
                ),
                DuplicateMetricValidationRule(
                    manifest_lookup=self._manifest_lookup,
                    resolver_input_for_query=resolver_input_for_query,
                    resolve_group_by_item_result=resolve_group_by_item_result,
                    resolve_metric_result=resolve_metrics_result,
                ),
                UniqueOutputColumnValidationRule(
                    manifest_lookup=self._manifest_lookup,
                    resolver_input_for_query=resolver_input_for_query,
                    resolve_group_by_item_result=resolve_group_by_item_result,
                    resolve_metric_result=resolve_metrics_result,
                ),
            ),
        )

        if query_level_issue_set.has_issues:
            mappings_to_merge.append(
                InputToIssueSetMapping(
                    (
                        InputToIssueSetMappingItem(
                            resolver_input=resolver_input_for_query,
                            issue_set=query_level_issue_set,
                        ),
                    )
                )
            )

        issue_set_mapping = InputToIssueSetMapping.merge_iterable(mappings_to_merge)

        if issue_set_mapping.has_issues:
            return MetricFlowQueryResolution(
                query_spec=None,
                resolution_dag=resolution_dag,
                filter_spec_lookup=filter_spec_lookup,
                input_to_issue_set=issue_set_mapping,
                queried_semantic_models=(),
            )

        model_reference_set = set(resolve_group_by_item_result.linkable_element_set.derived_from_semantic_models)
        for filter_spec_resolution in filter_spec_lookup.spec_resolutions:
            model_reference_set.update(
                set(filter_spec_resolution.resolved_group_by_item_set.derived_from_semantic_models)
            )

        # Collect all semantic models referenced by the query.
        semantic_models_in_group_by_items = set(
            resolve_group_by_item_result.linkable_element_set.derived_from_semantic_models
        )
        semantic_models_in_filters = set(
            itertools.chain.from_iterable(
                filter_spec_resolution.resolved_group_by_item_set.derived_from_semantic_models
                for filter_spec_resolution in filter_spec_lookup.spec_resolutions
            )
        )
        simple_metric_semantic_models = self._get_models_for_simple_metrics(resolution_dag)

        queried_semantic_models = set.union(
            semantic_models_in_group_by_items, semantic_models_in_filters, simple_metric_semantic_models
        )
        queried_semantic_models -= {SemanticModelDerivation.VIRTUAL_SEMANTIC_MODEL_REFERENCE}

        # Sanity check to make sure that all queried semantic models are in the model.
        models_not_in_manifest = queried_semantic_models - {
            semantic_model.reference for semantic_model in self._manifest_lookup.semantic_manifest.semantic_models
        }

        # There are no known cases where this should happen, but adding this check just in case there's a bug where
        # a metric alias is used incorrectly.
        if len(models_not_in_manifest) > 0:
            logger.error(
                LazyFormat(
                    "Semantic references that aren't in the manifest were found in the set used in"
                    " a query. This is a bug, and to avoid potential issues, they will be filtered out.",
                    models_not_in_manifest=models_not_in_manifest,
                )
            )
        queried_semantic_models -= models_not_in_manifest

        return MetricFlowQueryResolution(
            query_spec=MetricFlowQuerySpec(
                metric_specs=metric_specs,
                dimension_specs=linkable_spec_set.dimension_specs,
                entity_specs=linkable_spec_set.entity_specs,
                time_dimension_specs=linkable_spec_set.time_dimension_specs,
                order_by_specs=tuple(order_by_specs),
                limit=limit_input.limit,
                filter_intersection=query_level_filter_input.where_filter_intersection,
                filter_spec_resolution_lookup=filter_spec_lookup,
                min_max_only=min_max_only_input.min_max_only,
                apply_group_by=apply_group_by_input.apply_group_by,
                spec_output_order=group_by_item_specs + metric_specs,
            ),
            resolution_dag=resolution_dag,
            filter_spec_lookup=filter_spec_lookup,
            input_to_issue_set=issue_set_mapping,
            queried_semantic_models=tuple(sorted(queried_semantic_models)),
        )

    def _get_models_for_simple_metrics(self, resolution_dag: GroupByItemResolutionDag) -> Set[SemanticModelReference]:
        """Return the semantic model references for all simple metrics used in the query."""
        resolution_dag_node_set = resolution_dag.sink_node.inclusive_ancestors()

        simple_metric_references: MutableOrderedSet[MetricReference] = MutableOrderedSet()

        # Collect simple metrics.
        for simple_metric_node in resolution_dag_node_set.simple_metric_nodes:
            simple_metric_references.add(simple_metric_node.metric_reference)

        # For conversion metrics, get the input conversion metric through the metric since those
        # aren't in the DAG.
        for metric_node in resolution_dag_node_set.complex_metric_nodes:
            metric = self._manifest_lookup.metric_lookup.get_metric(metric_node.metric_reference)
            conversion_type_params = metric.type_params.conversion_type_params
            if conversion_type_params is None:
                continue

            assert conversion_type_params.base_metric is not None, "A conversion metric must have a base metric."
            assert (
                conversion_type_params.conversion_metric is not None
            ), "A conversion metric must have an input conversion metric."
            # The input base metric should be in the DAG, but just in case.
            simple_metric_references.add(MetricReference(conversion_type_params.base_metric.name))
            simple_metric_references.add(MetricReference(conversion_type_params.conversion_metric.name))

        model_references: MutableOrderedSet[SemanticModelReference] = MutableOrderedSet()
        for simple_metric_reference in simple_metric_references:
            metric = self._manifest_lookup.metric_lookup.get_metric(simple_metric_reference)
            metric_aggregation_params = metric.type_params.metric_aggregation_params
            if metric_aggregation_params is None:
                raise InvalidManifestException(
                    LazyFormat("Metric does not have `metric_aggregation_params` set", metric=metric)
                )
            model_references.add(SemanticModelReference(metric_aggregation_params.semantic_model))

        return model_references
