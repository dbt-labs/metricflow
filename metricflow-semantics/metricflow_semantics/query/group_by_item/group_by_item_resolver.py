from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import Optional, Sequence, Tuple

from dbt_semantic_interfaces.call_parameter_sets import TimeDimensionCallParameterSet
from dbt_semantic_interfaces.naming.keywords import METRIC_TIME_ELEMENT_NAME
from dbt_semantic_interfaces.references import SemanticModelReference, TimeDimensionReference
from dbt_semantic_interfaces.type_enums import TimeGranularity
from typing_extensions import override

from metricflow_semantics.mf_logging.formatting import indent
from metricflow_semantics.mf_logging.lazy_formattable import LazyFormat
from metricflow_semantics.mf_logging.pretty_print import mf_pformat
from metricflow_semantics.model.semantic_manifest_lookup import SemanticManifestLookup
from metricflow_semantics.model.semantic_model_derivation import SemanticModelDerivation
from metricflow_semantics.model.semantics.linkable_element_set import LinkableElementSet
from metricflow_semantics.naming.object_builder_scheme import ObjectBuilderNamingScheme
from metricflow_semantics.query.group_by_item.candidate_push_down.push_down_visitor import (
    PushDownResult,
    _PushDownGroupByItemCandidatesVisitor,
)
from metricflow_semantics.query.group_by_item.filter_spec_resolution.filter_location import WhereFilterLocation
from metricflow_semantics.query.group_by_item.resolution_dag.dag import GroupByItemResolutionDag, ResolutionDagSinkNode
from metricflow_semantics.query.group_by_item.resolution_path import MetricFlowQueryResolutionPath
from metricflow_semantics.query.issues.group_by_item_resolver.ambiguous_group_by_item import AmbiguousGroupByItemIssue
from metricflow_semantics.query.issues.issues_base import (
    MetricFlowQueryResolutionIssueSet,
)
from metricflow_semantics.query.suggestion_generator import QueryItemSuggestionGenerator
from metricflow_semantics.specs.instance_spec import LinkableInstanceSpec
from metricflow_semantics.specs.patterns.metric_time_default_granularity import MetricTimeDefaultGranularityPattern
from metricflow_semantics.specs.patterns.minimum_time_grain import MinimumTimeGrainPattern
from metricflow_semantics.specs.patterns.no_group_by_metric import NoGroupByMetricPattern
from metricflow_semantics.specs.patterns.spec_pattern import SpecPattern
from metricflow_semantics.specs.patterns.typed_patterns import TimeDimensionPattern
from metricflow_semantics.specs.spec_set import InstanceSpecSet, group_specs_by_type

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class GroupByItemResolution(SemanticModelDerivation):
    """Result object that contains matching spec for a potentially ambiguous input to a query.

    e.g. "TimeDimension('metric_time')" -> TimeDimensionSpec('metric_time', DAY)

    Issues found while resolving the input pattern are returned in issue_set.
    """

    # If the spec is None, then the pattern couldn't be resolved
    spec: Optional[LinkableInstanceSpec]
    linkable_element_set: LinkableElementSet
    issue_set: MetricFlowQueryResolutionIssueSet

    @property
    @override
    def derived_from_semantic_models(self) -> Sequence[SemanticModelReference]:
        return self.linkable_element_set.derived_from_semantic_models


@dataclass(frozen=True)
class AvailableGroupByItemsResolution:
    """Result object that contains the available group-by-items for a query."""

    specs: Tuple[LinkableInstanceSpec, ...]
    issue_set: MetricFlowQueryResolutionIssueSet


class GroupByItemResolver:
    """Resolves group-by items for potentially ambiguous inputs that are specified in queries / filters."""

    def __init__(  # noqa: D107
        self,
        manifest_lookup: SemanticManifestLookup,
        resolution_dag: GroupByItemResolutionDag,
    ) -> None:
        self._manifest_lookup = manifest_lookup
        self._resolution_dag = resolution_dag

    def resolve_matching_item_for_querying(
        self,
        spec_pattern: SpecPattern,
        suggestion_generator: Optional[QueryItemSuggestionGenerator],
    ) -> GroupByItemResolution:
        """Returns the spec that corresponds to the one described by spec_pattern and is valid for the query.

        For queries, if the pattern matches to a spec for the same element at different grains, the spec with the finest
        common grain is returned, unless the spec is metric_time, in which case the default grain is returned.
        """
        push_down_visitor = _PushDownGroupByItemCandidatesVisitor(
            manifest_lookup=self._manifest_lookup,
            source_spec_patterns=(spec_pattern, NoGroupByMetricPattern()),
            group_by_item_resolver=self,
            suggestion_generator=suggestion_generator,
        )

        push_down_result: PushDownResult = self._resolution_dag.sink_node.accept(push_down_visitor)

        if push_down_result.candidate_set.num_candidates == 0:
            return GroupByItemResolution(
                spec=None,
                linkable_element_set=LinkableElementSet(),
                issue_set=push_down_result.issue_set,
            )

        for candidate_filter in (
            # Default pattern must come first to avoid removing default grain options prematurely.
            MetricTimeDefaultGranularityPattern(push_down_result.max_metric_default_time_granularity),
            MinimumTimeGrainPattern(),
        ):
            push_down_result = push_down_result.filter_candidates_by_pattern(candidate_filter)

        logger.debug(
            LazyFormat(
                lambda: f"Spec pattern:\n"
                f"{indent(mf_pformat(spec_pattern))}\n"
                f"was resolved to:\n"
                f"{indent(mf_pformat(push_down_result.candidate_set.specs))}"
            )
        )
        if push_down_result.candidate_set.num_candidates > 1:
            return GroupByItemResolution(
                spec=None,
                linkable_element_set=LinkableElementSet(),
                issue_set=push_down_result.issue_set.add_issue(
                    AmbiguousGroupByItemIssue.from_parameters(
                        candidate_set=push_down_result.candidate_set,
                        query_resolution_path=MetricFlowQueryResolutionPath.from_path_item(
                            self._resolution_dag.sink_node
                        ),
                    )
                ),
            )

        return GroupByItemResolution(
            spec=push_down_result.candidate_set.specs[0],
            linkable_element_set=push_down_result.candidate_set.linkable_element_set,
            issue_set=push_down_result.issue_set,
        )

    def resolve_matching_item_for_filters(
        self,
        input_str: str,
        spec_pattern: SpecPattern,
        resolution_node: ResolutionDagSinkNode,
        filter_location: WhereFilterLocation,
    ) -> GroupByItemResolution:
        """Returns the spec that matches the spec_pattern associated with the filter in the given node.

        The input_str is a string representation of the input for use in generating suggestions for errors.

        e.g. if the query has a where filter like "{{ TimeDimension('metric_time') }} = '2020-01-01'", then to resolve
        the spec for TimeDimension('metric_time'), the resolution node would be a QueryGroupByItemResolutionNode() from
        the DAG used in the initialization of this resolver.
        """
        suggestion_generator = QueryItemSuggestionGenerator(
            input_naming_scheme=ObjectBuilderNamingScheme(),
            input_str=input_str,
            candidate_filters=QueryItemSuggestionGenerator.FILTER_ITEM_CANDIDATE_FILTERS,
        )

        push_down_visitor = _PushDownGroupByItemCandidatesVisitor(
            manifest_lookup=self._manifest_lookup,
            source_spec_patterns=(spec_pattern,),
            group_by_item_resolver=self,
            suggestion_generator=suggestion_generator,
            filter_location=filter_location,
        )

        push_down_result: PushDownResult = resolution_node.accept(push_down_visitor)

        for candidate_filter in (
            # Default pattern must come first to avoid removing default grain options prematurely.
            MetricTimeDefaultGranularityPattern(push_down_result.max_metric_default_time_granularity),
            MinimumTimeGrainPattern(),
        ):
            push_down_result = push_down_result.filter_candidates_by_pattern(candidate_filter)

        if push_down_result.candidate_set.num_candidates == 0:
            return GroupByItemResolution(
                spec=None,
                linkable_element_set=LinkableElementSet(),
                issue_set=push_down_result.issue_set,
            )

        if push_down_result.candidate_set.num_candidates > 1:
            return GroupByItemResolution(
                spec=None,
                linkable_element_set=LinkableElementSet(),
                issue_set=push_down_result.issue_set.add_issue(
                    AmbiguousGroupByItemIssue.from_parameters(
                        candidate_set=push_down_result.candidate_set,
                        query_resolution_path=MetricFlowQueryResolutionPath.from_path_item(
                            self._resolution_dag.sink_node
                        ),
                    )
                ),
            )

        return GroupByItemResolution(
            spec=push_down_result.candidate_set.specs[0],
            linkable_element_set=push_down_result.candidate_set.linkable_element_set,
            issue_set=push_down_result.issue_set,
        )

    def resolve_available_items(
        self,
        resolution_node: Optional[ResolutionDagSinkNode] = None,
        source_spec_patterns: Sequence[SpecPattern] = (),
    ) -> AvailableGroupByItemsResolution:
        """Return all available group-by-items at a given node.

        By default, the query node is used, so this will return the available group-by-items that can be used in a
        query.
        """
        if resolution_node is None:
            resolution_node = self._resolution_dag.sink_node

        push_down_visitor = _PushDownGroupByItemCandidatesVisitor(
            manifest_lookup=self._manifest_lookup,
            source_spec_patterns=source_spec_patterns,
            group_by_item_resolver=self,
            suggestion_generator=None,
        )

        push_down_result: PushDownResult = resolution_node.accept(push_down_visitor)

        return AvailableGroupByItemsResolution(
            specs=tuple(push_down_result.candidate_set.specs),
            issue_set=push_down_result.issue_set,
        )

    def resolve_min_metric_time_grain(self) -> TimeGranularity:
        """Returns the finest base time grain of metric_time for querying."""
        metric_time_grain_resolution = self.resolve_matching_item_for_querying(
            spec_pattern=TimeDimensionPattern.from_call_parameter_set(
                TimeDimensionCallParameterSet(
                    entity_path=(),
                    time_dimension_reference=TimeDimensionReference(element_name=METRIC_TIME_ELEMENT_NAME),
                )
            ),
            suggestion_generator=None,
        )
        metric_time_spec_set = (
            group_specs_by_type((metric_time_grain_resolution.spec,))
            if metric_time_grain_resolution.spec is not None
            else InstanceSpecSet.empty_instance()
        )
        if len(metric_time_spec_set.time_dimension_specs) != 1:
            raise RuntimeError(
                f"The grain for {repr(METRIC_TIME_ELEMENT_NAME)} could not be resolved. Got spec "
                f"{metric_time_grain_resolution.spec} and issues:\n\n"
                f"{indent(mf_pformat(metric_time_grain_resolution.issue_set))}"
            )
        return metric_time_spec_set.time_dimension_specs[0].time_granularity.base_granularity
