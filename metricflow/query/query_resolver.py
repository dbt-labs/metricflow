from __future__ import annotations

import textwrap
from dataclasses import dataclass
from typing import Optional, Sequence, Dict

import rapidfuzz
from dbt_semantic_interfaces.enum_extension import assert_values_exhausted
from dbt_semantic_interfaces.pretty_print import pformat_big_objects
from dbt_semantic_interfaces.protocols import WhereFilter
from dbt_semantic_interfaces.references import MetricReference
from dbt_semantic_interfaces.type_enums import MetricType
from typing_extensions import override

from metricflow.model.semantic_manifest_lookup import SemanticManifestLookup
from metricflow.query.query_issues import (
    MetricFlowQueryIssueSet,
    MetricFlowQueryIssueType,
    MetricFlowQueryResolutionIssue,
)
from metricflow.specs.column_assoc import ColumnAssociationResolver
from metricflow.specs.merge_builder import Mergeable, MergeBuilder
from metricflow.specs.patterns.metric_pattern import MetricNamePattern, MetricNamingScheme
from metricflow.specs.patterns.spec_pattern import ScoredSpec, ScoringResults, SpecPattern
from metricflow.specs.specs import MetricFlowQuerySpec, MetricSpec, OrderBySpec, LinkableInstanceSpec, WhereFilterSpec


@dataclass(frozen=True)
class MetricFlowQueryResolution(Mergeable):
    """The result of resolving query inputs to specs."""

    # Can be None if there were errors.
    query_spec: Optional[MetricFlowQuerySpec] = None

    metric_matches: Dict[SpecPattern[MetricSpec], Sequence[MetricSpec]]
    group_by_item_matches: Dict[SpecPattern[LinkableInstanceSpec], Sequence[LinkableInstanceSpec]]
    where_filter_spec: Optional[WhereFilterSpec]
    
    issue_set: MetricFlowQueryIssueSet = MetricFlowQueryIssueSet()

    def checked_query_spec(self) -> MetricFlowQuerySpec:
        """Returns the query_spec, but if MetricFlowQueryResolution.has_errors was True, raise a RuntimeError."""
        if self.has_errors:
            raise RuntimeError(
                f"Can't get the query spec because errors were present in the resolution:"
                f"\n{pformat_big_objects(self.issue_set.errors)}"
            )
        if self.query_spec is None:
            raise RuntimeError("If there were no errors, query_spec should have been populated.")
        return self.query_spec

    def with_additional_issues(
        self,
        issue_set: MetricFlowQueryIssueSet,
    ) -> MetricFlowQueryResolution:
        """Return a new resolution with those issues added."""
        return MetricFlowQueryResolution(query_spec=self.query_spec, issue_set=self.issue_set.merge(issue_set))

    @property
    def has_errors(self) -> bool:  # noqa: D
        return len(self.issue_set.errors) > 0

    @override
    def merge(self, other: MetricFlowQueryResolution) -> MetricFlowQueryResolution:
        merge_builder = MergeBuilder(MetricFlowQuerySpec())
        if self.query_spec is not None:
            merge_builder.add(self.query_spec)
        if other.query_spec is not None:
            merge_builder.add(other.query_spec)

        return MetricFlowQueryResolution(
            query_spec=merge_builder.build_result, issue_set=self.issue_set.merge(other.issue_set)
        )

    def with_additional_metric_path_prefix(self, metric_reference: MetricReference) -> MetricFlowQueryResolution:
        """Return a resolution where the metric is added as the first element in the issues' metric path."""
        return MetricFlowQueryResolution(
            query_spec=self.query_spec, issue_set=self.issue_set.with_additional_metric_path_prefix(metric_reference)
        )


@dataclass(frozen=True)
class MetricFlowQueryOrderByItem:
    """Describes the order direction for one of the metrics or group by items."""

    spec_pattern: SpecPattern
    descending: bool


class MetricFlowQueryResolver:
    """Given spec patterns that define the query, resolve them into specs.

    TODO: WhereSpecFactory should not need to depend on the column association resolver.
    """

    _INDENT = "    "

    def __init__(  # noqa: D
        self,
        manifest_lookup: SemanticManifestLookup,
        column_association_resolver: ColumnAssociationResolver,
    ) -> None:
        self._manifest_lookup = manifest_lookup
        self._column_association_resolver = column_association_resolver
        self._known_metric_specs = [
            MetricSpec.from_reference(metric_reference)
            for metric_reference in self._manifest_lookup.metric_lookup.metric_references
        ]

    @staticmethod
    def _create_error_resolution_for_unmatched_pattern(
        plural_item_name: str,
        spec_pattern: SpecPattern,
        scoring_results: ScoringResults,
        top_n_suggestions: int = 6,
    ) -> MetricFlowQueryResolution:
        ranked_specs: Sequence[ScoredSpec] = sorted(
            scoring_results.scored_specs, key=lambda scored_spec: scored_spec.score
        )

        suggestions = pformat_big_objects(
            [spec_pattern.naming_scheme.input_str(scored_spec.spec) for scored_spec in ranked_specs[:top_n_suggestions]]
        )
        return MetricFlowQueryResolution(
            issue_set=MetricFlowQueryIssueSet(
                issues=(
                    MetricFlowQueryResolutionIssue(
                        issue_type=MetricFlowQueryIssueType.ERROR,
                        message=(
                            f"`{spec_pattern}` does not match exactly to one of the available "
                            f"{plural_item_name}. Suggestions:\n"
                            f"{textwrap.indent(suggestions, prefix=MetricFlowQueryResolver._INDENT)}"
                        ),
                    ),
                )
            )
        )

    def resolve_query(  # noqa: D
        self,
        metric_patterns: Sequence[SpecPattern],
        group_by_item_patterns: Sequence[SpecPattern],
        order_by_items: Sequence[MetricFlowQueryOrderByItem],
        limit: Optional[int],
        where_filter: Optional[WhereFilter],
    ) -> MetricFlowQueryResolution:
        query_resolution_builder = MergeBuilder(MetricFlowQueryResolution())

        for metric_pattern in metric_patterns:
            query_resolution_builder.add(
                self._resolve_query_for_one_metric(
                    metric_pattern=metric_pattern,
                    group_by_item_patterns=group_by_item_patterns,
                    where_spec=where_filter,
                )
            )

        # Check the order by if there is a query spec with metrics and group by items.
        query_spec = query_resolution_builder.build_result.query_spec
        if query_spec is not None:
            query_item_specs = query_spec.metric_specs + query_spec.linkable_specs.as_tuple

            # Check that the patterns in the order by match with one of the specs specified in the query.
            order_by_specs = []
            for order_by_item in order_by_items:
                scoring_results = order_by_item.spec_pattern.score(query_item_specs)
                if not scoring_results.has_exactly_one_match:
                    query_resolution_builder.add(
                        self._create_error_resolution_for_unmatched_pattern(
                            plural_item_name="query items",
                            spec_pattern=order_by_item.spec_pattern,
                            scoring_results=scoring_results,
                        )
                    )
                else:
                    order_by_specs.append(
                        OrderBySpec(
                            instance_spec=scoring_results.matching_spec,
                            descending=order_by_item.descending,
                        )
                    )

            query_resolution_builder.add(
                MetricFlowQueryResolution(query_spec=MetricFlowQuerySpec(order_by_specs=tuple(order_by_specs)))
            )

        # Add an issue if the limit is negative.
        if limit is not None and limit < 0:
            query_resolution_builder.add(
                MetricFlowQueryResolution(
                    issue_set=MetricFlowQueryIssueSet(
                        issues=(
                            MetricFlowQueryResolutionIssue(
                                issue_type=MetricFlowQueryIssueType.ERROR,
                                message=f"The limit was specified as {limit}, but it must be >= 0.",
                            ),
                        )
                    )
                )
            )

        query_resolution: MetricFlowQueryResolution = query_resolution_builder.build_result

        # Return the resolution without the query spec if there are errors.
        if query_resolution.has_errors:
            return MetricFlowQueryResolution(issue_set=query_resolution.issue_set)

        # If there are no errors, there should be a spec and the resolution should be ready to return.
        if query_resolution.query_spec is None:
            raise RuntimeError("The query spec is missing even though there are no errors in the query.")

        return query_resolution

    def _resolve_query_for_one_metric(  # noqa: D
        self,
        metric_pattern: SpecPattern,
        group_by_item_patterns: Sequence[SpecPattern],
        where_spec: Optional[WhereFilter],
    ) -> MetricFlowQueryResolution:
        query_resolution_builder = MergeBuilder(MetricFlowQueryResolution())

        # Check if the metric patterns match with known metrics.
        metric_scoring_results = metric_pattern.score(self._known_metric_specs)

        if not metric_scoring_results.has_exactly_one_match:
            query_resolution_builder.add(
                MetricFlowQueryResolver._create_error_resolution_for_unmatched_pattern(
                    plural_item_name="metrics",
                    spec_pattern=metric_pattern,
                    scoring_results=metric_scoring_results,
                )
            )
            return query_resolution_builder.build_result

        metric_spec_set = metric_scoring_results.matching_spec.as_spec_set
        assert (
            len(metric_spec_set.metric_specs) == 1
        ), f"Did not get exactly 1 metric spec: {metric_spec_set.metric_specs}"

        metric_spec = metric_spec_set.metric_specs[0]

        metric = self._manifest_lookup.metric_lookup.get_metric(metric_spec.reference)

        if metric.type is MetricType.SIMPLE or metric.type is MetricType.CUMULATIVE:
            raise NotImplementedError
        elif metric.type is MetricType.RATIO or metric.type is MetricType.DERIVED:
            parent_metrics = [input_metric for input_metric in metric.input_metrics]

            for parent_metric in parent_metrics:
                metric_reference = parent_metric.as_reference
                parent_metric_query_resolution = self._resolve_query_for_one_metric(
                    metric_pattern=MetricNamePattern(
                        naming_scheme=MetricNamingScheme(),
                        target_spec=MetricSpec.from_reference(metric_reference),
                        input_str=metric_reference.element_name,
                    ),
                    group_by_item_patterns=group_by_item_patterns,
                    where_spec=where_spec,
                )
                query_resolution_builder.add(
                    parent_metric_query_resolution.with_additional_metric_path_prefix(metric_reference),
                )
        else:
            assert_values_exhausted(metric.type)

        # Return early as it's difficult to resolve group by items without correct metrics.
        if query_resolution_builder.build_result.has_errors > 0:
            return query_resolution_builder.build_result

        # Check that the group by items match to one that's available.
        possible_group_by_specs = self._manifest_lookup.metric_lookup.element_specs_for_metrics(
            metric_references=tuple(metric_spec.reference for metric_spec in metric_spec_set.metric_specs)
        )

        # Build a spec set by matching the patterns for the group by items to the available group by item specs
        # for the queried metrics.
        for group_by_item_pattern in group_by_item_patterns:
            group_by_item_scoring_results = group_by_item_pattern.score(possible_group_by_specs)
            if not group_by_item_scoring_results.has_exactly_one_match:
                query_resolution_builder.add(
                    MetricFlowQueryResolver._create_error_resolution_for_unmatched_pattern(
                        plural_item_name="group by items",
                        spec_pattern=group_by_item_pattern,
                        scoring_results=group_by_item_scoring_results,
                    )
                )

            else:
                matching_spec = group_by_item_scoring_results.matching_spec
                query_resolution_builder.add(
                    MetricFlowQueryResolution(query_spec=MetricFlowQuerySpec.from_spec_set(matching_spec.as_spec_set))
                )

        # Check the filter for the metric
        if metric.filter is not None:
            raise NotImplementedError

        return query_resolution_builder.build_result
