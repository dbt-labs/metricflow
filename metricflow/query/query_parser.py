from __future__ import annotations

import datetime
import logging
from typing import List, Optional, Sequence, Tuple, Union

import pandas as pd
from dbt_semantic_interfaces.implementations.filters.where_filter import (
    PydanticWhereFilter,
    PydanticWhereFilterIntersection,
)
from dbt_semantic_interfaces.protocols import SavedQuery
from dbt_semantic_interfaces.protocols.where_filter import WhereFilter
from dbt_semantic_interfaces.type_enums import TimeGranularity

from metricflow.assert_one_arg import assert_at_most_one_arg_set
from metricflow.filters.merge_where import merge_to_single_where_filter
from metricflow.filters.time_constraint import TimeRangeConstraint
from metricflow.mf_logging.formatting import indent
from metricflow.mf_logging.pretty_print import mf_pformat
from metricflow.mf_logging.runtime import log_runtime
from metricflow.model.semantic_manifest_lookup import SemanticManifestLookup
from metricflow.naming.dunder_scheme import DunderNamingScheme
from metricflow.naming.metric_scheme import MetricNamingScheme
from metricflow.naming.object_builder_scheme import ObjectBuilderNamingScheme
from metricflow.protocols.query_parameter import (
    GroupByParameter,
    MetricQueryParameter,
    OrderByQueryParameter,
    SavedQueryParameter,
)
from metricflow.query.group_by_item.filter_spec_resolution.filter_pattern_factory import (
    DefaultWhereFilterPatternFactory,
    WhereFilterPatternFactory,
)
from metricflow.query.group_by_item.group_by_item_resolver import GroupByItemResolver
from metricflow.query.group_by_item.resolution_dag.dag import GroupByItemResolutionDag
from metricflow.query.issues.issues_base import MetricFlowQueryResolutionIssueSet
from metricflow.query.issues.parsing.string_input_parsing_issue import StringInputParsingIssue
from metricflow.query.query_exceptions import InvalidQueryException
from metricflow.query.query_resolution import InputToIssueSetMapping, InputToIssueSetMappingItem
from metricflow.query.query_resolver import MetricFlowQueryResolver
from metricflow.query.resolver_inputs.base_resolver_inputs import MetricFlowQueryResolverInput
from metricflow.query.resolver_inputs.query_resolver_inputs import (
    InvalidStringInput,
    ResolverInputForGroupByItem,
    ResolverInputForLimit,
    ResolverInputForMetric,
    ResolverInputForMinMaxOnly,
    ResolverInputForOrderByItem,
    ResolverInputForQuery,
    ResolverInputForQueryLevelWhereFilterIntersection,
)
from metricflow.specs.patterns.base_time_grain import BaseTimeGrainPattern
from metricflow.specs.patterns.metric_time_pattern import MetricTimePattern
from metricflow.specs.patterns.none_date_part import NoneDatePartPattern
from metricflow.specs.specs import (
    InstanceSpec,
    InstanceSpecSet,
    MetricFlowQuerySpec,
    TimeDimensionSpec,
)
from metricflow.time.time_granularity import (
    adjust_to_end_of_period,
    adjust_to_start_of_period,
    is_period_end,
    is_period_start,
)

logger = logging.getLogger(__name__)


class MetricFlowQueryParser:
    """Parse input objects from the user into a metric query specification.

    Input objects are converted into *ResolverInput objects and then passed to the query resolver. The query resolver
    returns concrete specs corresponding to the request input / issues with query resolution.

    TODO: Add fuzzy match results.
    """

    def __init__(  # noqa: D
        self,
        semantic_manifest_lookup: SemanticManifestLookup,
        where_filter_pattern_factory: WhereFilterPatternFactory = DefaultWhereFilterPatternFactory(),
    ) -> None:
        self._manifest_lookup = semantic_manifest_lookup
        self._metric_naming_schemes = (MetricNamingScheme(),)
        self._group_by_item_naming_schemes = (
            ObjectBuilderNamingScheme(),
            DunderNamingScheme(),
        )
        self._where_filter_pattern_factory = where_filter_pattern_factory

    def parse_and_validate_saved_query(
        self,
        saved_query_parameter: SavedQueryParameter,
        where_filter: Optional[WhereFilter],
        limit: Optional[int],
        time_constraint_start: Optional[datetime.datetime],
        time_constraint_end: Optional[datetime.datetime],
        order_by_names: Optional[Sequence[str]],
        order_by_parameters: Optional[Sequence[OrderByQueryParameter]],
    ) -> MetricFlowQuerySpec:
        """Parse and validate a query using parameters from a pre-defined / saved query.

        Additional parameters act in conjunction with the parameters in the saved query.
        """
        saved_query = self._get_saved_query(saved_query_parameter)

        # Merge interface could streamline this.
        where_filters: List[WhereFilter] = []
        if saved_query.query_params.where is not None:
            where_filters.extend(saved_query.query_params.where.where_filters)
        if where_filter is not None:
            where_filters.append(where_filter)

        return self.parse_and_validate_query(
            metric_names=saved_query.query_params.metrics,
            group_by_names=saved_query.query_params.group_by,
            where_constraint=merge_to_single_where_filter(PydanticWhereFilterIntersection(where_filters=where_filters)),
            time_constraint_start=time_constraint_start,
            time_constraint_end=time_constraint_end,
            limit=limit,
            order_by_names=order_by_names,
            order_by=order_by_parameters,
        )

    def _get_saved_query(self, saved_query_parameter: SavedQueryParameter) -> SavedQuery:
        matching_saved_queries = [
            saved_query
            for saved_query in self._manifest_lookup.semantic_manifest.saved_queries
            if saved_query.name == saved_query_parameter.name
        ]

        if len(matching_saved_queries) != 1:
            known_saved_query_names = sorted(
                saved_query.name for saved_query in self._manifest_lookup.semantic_manifest.saved_queries
            )
            raise InvalidQueryException(
                f"Did not find saved query `{saved_query_parameter.name}` in known saved queries:\n"
                f"{mf_pformat(known_saved_query_names)}"
            )

        return matching_saved_queries[0]

    @staticmethod
    def _metric_time_granularity(time_dimension_specs: Sequence[TimeDimensionSpec]) -> Optional[TimeGranularity]:
        matching_specs: Sequence[InstanceSpec] = time_dimension_specs

        for pattern_to_apply in (
            MetricTimePattern(),
            BaseTimeGrainPattern(),
            NoneDatePartPattern(),
        ):
            matching_specs = pattern_to_apply.match(matching_specs)
        # The conversion below is awkward and needs some more thought.
        time_dimension_specs = InstanceSpecSet.from_specs(matching_specs).time_dimension_specs
        if len(time_dimension_specs) == 0:
            return None

        assert (
            len(time_dimension_specs) == 1
        ), f"Bug with BaseTimeGrainPattern - should have returned exactly 1 spec but got {time_dimension_specs}"

        return time_dimension_specs[0].time_granularity

    def _adjust_time_constraint(
        self,
        resolution_dag: GroupByItemResolutionDag,
        time_dimension_specs_in_query: Sequence[TimeDimensionSpec],
        time_constraint: TimeRangeConstraint,
    ) -> TimeRangeConstraint:
        metric_time_granularity = MetricFlowQueryParser._metric_time_granularity(time_dimension_specs_in_query)
        if metric_time_granularity is None:
            group_by_item_resolver = GroupByItemResolver(
                manifest_lookup=self._manifest_lookup,
                resolution_dag=resolution_dag,
            )
            metric_time_granularity = group_by_item_resolver.resolve_min_metric_time_grain()

        """Change the time range so that the ends are at the ends of the appropriate time granularity windows.

        e.g. [2020-01-15, 2020-2-15] with MONTH granularity -> [2020-01-01, 2020-02-29]
        """
        constraint_start = time_constraint.start_time
        constraint_end = time_constraint.end_time

        start_ts = pd.Timestamp(time_constraint.start_time)
        if not is_period_start(metric_time_granularity, start_ts):
            constraint_start = adjust_to_start_of_period(metric_time_granularity, start_ts).to_pydatetime()

        end_ts = pd.Timestamp(time_constraint.end_time)
        if not is_period_end(metric_time_granularity, end_ts):
            constraint_end = adjust_to_end_of_period(metric_time_granularity, end_ts).to_pydatetime()

        if constraint_start < TimeRangeConstraint.ALL_TIME_BEGIN():
            constraint_start = TimeRangeConstraint.ALL_TIME_BEGIN()
        if constraint_end > TimeRangeConstraint.ALL_TIME_END():
            constraint_end = TimeRangeConstraint.ALL_TIME_END()

        return TimeRangeConstraint(start_time=constraint_start, end_time=constraint_end)

    def _parse_order_by_names(
        self,
        order_by_names: Sequence[str],
    ) -> Sequence[ResolverInputForOrderByItem]:
        resolver_inputs: List[ResolverInputForOrderByItem] = []

        for order_by_name in order_by_names:
            possible_inputs: List[Union[ResolverInputForMetric, ResolverInputForGroupByItem]] = []
            if order_by_name[0] == "-":
                descending = True
                order_by_name_without_prefix = order_by_name[1:]
            else:
                descending = False
                order_by_name_without_prefix = order_by_name

            for group_by_item_naming_scheme in self._group_by_item_naming_schemes:
                if group_by_item_naming_scheme.input_str_follows_scheme(order_by_name_without_prefix):
                    possible_inputs.append(
                        ResolverInputForGroupByItem(
                            input_obj=order_by_name,
                            input_obj_naming_scheme=group_by_item_naming_scheme,
                            spec_pattern=group_by_item_naming_scheme.spec_pattern(order_by_name_without_prefix),
                        )
                    )
                    break

            for metric_naming_scheme in self._metric_naming_schemes:
                if metric_naming_scheme.input_str_follows_scheme(order_by_name_without_prefix):
                    possible_inputs.append(
                        ResolverInputForMetric(
                            input_obj=order_by_name,
                            naming_scheme=metric_naming_scheme,
                            spec_pattern=metric_naming_scheme.spec_pattern(order_by_name_without_prefix),
                        )
                    )

            resolver_inputs.append(
                ResolverInputForOrderByItem(
                    input_obj=order_by_name,
                    possible_inputs=tuple(possible_inputs),
                    descending=descending,
                )
            )

        return resolver_inputs

    @staticmethod
    def _parse_order_by(
        order_by: Sequence[OrderByQueryParameter],
    ) -> Sequence[ResolverInputForOrderByItem]:
        return tuple(order_by_query_parameter.query_resolver_input for order_by_query_parameter in order_by)

    @staticmethod
    def generate_error_message(
        input_to_issue_set: InputToIssueSetMapping,
    ) -> Optional[str]:
        """Create an error message that formats the inputs / issues."""
        lines: List[str] = ["Got errors while resolving the query."]
        issue_counter = 0

        for item in input_to_issue_set.items:
            resolver_input = item.resolver_input
            issue_set = item.issue_set

            if not issue_set.has_errors:
                continue

            issue_counter += 1
            for error_issue in issue_set.errors:
                lines.append(f"\nError #{issue_counter}:")
                issue_set_lines: List[str] = [
                    "Message:\n",
                    indent(error_issue.ui_description(resolver_input)),
                    "\nQuery Input:\n",
                    indent(resolver_input.ui_description),
                ]

                if len(error_issue.query_resolution_path.resolution_path_nodes) > 0:
                    issue_set_lines.extend(
                        [
                            "\nIssue Location:\n",
                            indent(error_issue.query_resolution_path.ui_description),
                        ]
                    )

                lines.extend(indent(issue_set_line) for issue_set_line in issue_set_lines)

        return "\n".join(lines)

    def _raise_exception_if_there_are_errors(
        self,
        input_to_issue_set: InputToIssueSetMapping,
    ) -> None:
        if not input_to_issue_set.merged_issue_set.has_errors:
            return

        raise InvalidQueryException(self.generate_error_message(input_to_issue_set=input_to_issue_set))

    def parse_and_validate_query(
        self,
        metric_names: Optional[Sequence[str]] = None,
        metrics: Optional[Sequence[MetricQueryParameter]] = None,
        group_by_names: Optional[Sequence[str]] = None,
        group_by: Optional[Tuple[GroupByParameter, ...]] = None,
        limit: Optional[int] = None,
        time_constraint_start: Optional[datetime.datetime] = None,
        time_constraint_end: Optional[datetime.datetime] = None,
        where_constraint: Optional[WhereFilter] = None,
        where_constraint_str: Optional[str] = None,
        order_by_names: Optional[Sequence[str]] = None,
        order_by: Optional[Sequence[OrderByQueryParameter]] = None,
        min_max_only: bool = False,
    ) -> MetricFlowQuerySpec:
        """Parse the query into spec objects, validating them in the process.

        e.g. make sure that the given metric is a valid metric name.
        """
        # Workaround for a Pycharm type inspection issue with decorators.
        # noinspection PyArgumentList
        return self._parse_and_validate_query(
            metric_names=metric_names,
            metrics=metrics,
            group_by_names=group_by_names,
            group_by=group_by,
            limit=limit,
            time_constraint_start=time_constraint_start,
            time_constraint_end=time_constraint_end,
            where_constraint=where_constraint,
            where_constraint_str=where_constraint_str,
            order_by_names=order_by_names,
            order_by=order_by,
            min_max_only=min_max_only,
        )

    @log_runtime()
    def _parse_and_validate_query(
        self,
        metric_names: Optional[Sequence[str]],
        metrics: Optional[Sequence[MetricQueryParameter]],
        group_by_names: Optional[Sequence[str]],
        group_by: Optional[Tuple[GroupByParameter, ...]],
        limit: Optional[int],
        time_constraint_start: Optional[datetime.datetime],
        time_constraint_end: Optional[datetime.datetime],
        where_constraint: Optional[WhereFilter],
        where_constraint_str: Optional[str],
        order_by_names: Optional[Sequence[str]],
        order_by: Optional[Sequence[OrderByQueryParameter]],
        min_max_only: bool,
    ) -> MetricFlowQuerySpec:
        # TODO: validate min_max_only - can only be called for non-metric queries
        assert_at_most_one_arg_set(metric_names=metric_names, metrics=metrics)
        assert_at_most_one_arg_set(group_by_names=group_by_names, group_by=group_by)
        assert_at_most_one_arg_set(order_by_names=order_by_names, order_by=order_by)
        assert_at_most_one_arg_set(where_constraint=where_constraint, where_constraint_str=where_constraint_str)

        metric_names = metric_names or ()
        metrics = metrics or ()

        group_by_names = group_by_names or ()
        group_by = group_by or ()

        order_by_names = order_by_names or ()
        order_by = order_by or ()

        input_to_issue_set_mapping_item: List[InputToIssueSetMappingItem] = []

        resolver_inputs_for_metrics: List[ResolverInputForMetric] = []
        for metric_name in metric_names:
            resolver_input_for_metric: Optional[MetricFlowQueryResolverInput] = None
            for metric_naming_scheme in self._metric_naming_schemes:
                if metric_naming_scheme.input_str_follows_scheme(metric_name):
                    resolver_input_for_metric = ResolverInputForMetric(
                        input_obj=metric_name,
                        naming_scheme=metric_naming_scheme,
                        spec_pattern=metric_naming_scheme.spec_pattern(metric_name),
                    )
                    resolver_inputs_for_metrics.append(resolver_input_for_metric)
                    break
            if resolver_input_for_metric is None:
                resolver_input_for_metric = InvalidStringInput(metric_name)
                input_to_issue_set_mapping_item.append(
                    InputToIssueSetMappingItem(
                        resolver_input=resolver_input_for_metric,
                        issue_set=MetricFlowQueryResolutionIssueSet.from_issue(
                            StringInputParsingIssue.from_parameters(
                                input_str=metric_name,
                            )
                        ),
                    )
                )

        for metric_query_parameter in metrics:
            resolver_inputs_for_metrics.append(metric_query_parameter.query_resolver_input)

        resolver_inputs_for_group_by_items: List[ResolverInputForGroupByItem] = []
        for group_by_name in group_by_names:
            resolver_input_for_group_by_item: Optional[MetricFlowQueryResolverInput] = None
            for group_by_item_naming_scheme in self._group_by_item_naming_schemes:
                if group_by_item_naming_scheme.input_str_follows_scheme(group_by_name):
                    spec_pattern = group_by_item_naming_scheme.spec_pattern(group_by_name)
                    resolver_input_for_group_by_item = ResolverInputForGroupByItem(
                        input_obj=group_by_name,
                        input_obj_naming_scheme=group_by_item_naming_scheme,
                        spec_pattern=spec_pattern,
                    )
                    resolver_inputs_for_group_by_items.append(resolver_input_for_group_by_item)
                    break
            if resolver_input_for_group_by_item is None:
                resolver_input_for_group_by_item = InvalidStringInput(group_by_name)
                input_to_issue_set_mapping_item.append(
                    InputToIssueSetMappingItem(
                        resolver_input=resolver_input_for_group_by_item,
                        issue_set=MetricFlowQueryResolutionIssueSet.from_issue(
                            StringInputParsingIssue.from_parameters(
                                input_str=group_by_name,
                            )
                        ),
                    )
                )

            logger.info(
                "Converted group-by-item input:\n"
                + indent(f"Input: {repr(group_by_name)}")
                + "\n"
                + indent(f"Resolver Input: {mf_pformat(resolver_input_for_group_by_item)}")
            )

        for group_by_parameter in group_by:
            resolver_input_for_group_by_parameter = group_by_parameter.query_resolver_input
            resolver_inputs_for_group_by_items.append(resolver_input_for_group_by_parameter)
            logger.info(
                "Converted group-by-item input:\n"
                + indent(f"Input: {repr(group_by_parameter)}")
                + "\n"
                + indent(f"Resolver Input: {mf_pformat(resolver_input_for_group_by_parameter)}")
            )

        where_filters: List[PydanticWhereFilter] = []

        if where_constraint is not None:
            where_filters.append(PydanticWhereFilter(where_sql_template=where_constraint.where_sql_template))
        if where_constraint_str is not None:
            where_filters.append(PydanticWhereFilter(where_sql_template=where_constraint_str))

        resolver_input_for_filter = ResolverInputForQueryLevelWhereFilterIntersection(
            where_filter_intersection=PydanticWhereFilterIntersection(where_filters=where_filters)
        )

        query_resolver = MetricFlowQueryResolver(
            manifest_lookup=self._manifest_lookup, where_filter_pattern_factory=self._where_filter_pattern_factory
        )

        resolver_inputs_for_order_by: List[ResolverInputForOrderByItem] = []
        resolver_inputs_for_order_by.extend(
            self._parse_order_by_names(
                order_by_names=order_by_names,
            )
        )
        resolver_inputs_for_order_by.extend(MetricFlowQueryParser._parse_order_by(order_by=order_by))

        resolver_input_for_limit = ResolverInputForLimit(limit=limit)
        resolver_input_for_min_max_only = ResolverInputForMinMaxOnly(min_max_only=min_max_only)

        resolver_input_for_query = ResolverInputForQuery(
            metric_inputs=tuple(resolver_inputs_for_metrics),
            group_by_item_inputs=tuple(resolver_inputs_for_group_by_items),
            order_by_item_inputs=tuple(resolver_inputs_for_order_by),
            limit_input=resolver_input_for_limit,
            filter_input=resolver_input_for_filter,
            min_max_only=resolver_input_for_min_max_only,
        )

        logger.info("Resolver input for query is:\n" + indent(mf_pformat(resolver_input_for_query)))

        query_resolution = query_resolver.resolve_query(resolver_input_for_query)

        logger.info("Query resolution is:\n" + indent(mf_pformat(query_resolution)))

        self._raise_exception_if_there_are_errors(
            input_to_issue_set=query_resolution.input_to_issue_set.merge(
                InputToIssueSetMapping(tuple(input_to_issue_set_mapping_item))
            ),
        )

        query_spec = query_resolution.checked_query_spec
        assert query_resolution.resolution_dag is not None
        if time_constraint_start is not None or time_constraint_end is not None:
            if time_constraint_start is None:
                time_constraint_start = TimeRangeConstraint.ALL_TIME_BEGIN()
                logger.info(f"time_constraint_start was None, so it was set to {time_constraint_start}")
            if time_constraint_end is None:
                time_constraint_end = TimeRangeConstraint.ALL_TIME_END()
                logger.info(f"time_constraint_end was None, so it was set to {time_constraint_end}")

            time_constraint = TimeRangeConstraint(
                start_time=time_constraint_start,
                end_time=time_constraint_end,
            )

            time_constraint = self._adjust_time_constraint(
                resolution_dag=query_resolution.resolution_dag,
                time_dimension_specs_in_query=query_spec.time_dimension_specs,
                time_constraint=time_constraint,
            )
            logger.info(f"Time constraint after adjustment is: {time_constraint}")

            return query_spec.with_time_range_constraint(time_constraint)

        return query_spec
