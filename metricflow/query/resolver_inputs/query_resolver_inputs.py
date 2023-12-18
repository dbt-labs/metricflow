"""Contains input classes for the query resolver.

The naming of these classes is a little odd as they have the "For.." suffix. But using the "*ResolverInput" leads to
some confusing names like "ResolverInputForQuery" -> "QueryResolverInput". Improved naming for these classes is TBD.
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Optional, Tuple, Union

from dbt_semantic_interfaces.protocols import WhereFilterIntersection
from typing_extensions import override

from metricflow.collection_helpers.pretty_print import mf_pformat
from metricflow.naming.metric_scheme import MetricNamingScheme
from metricflow.naming.naming_scheme import QueryItemNamingScheme
from metricflow.protocols.query_parameter import GroupByParameter, MetricQueryParameter, OrderByQueryParameter
from metricflow.query.resolver_inputs.base_resolver_inputs import InputPatternDescription, MetricFlowQueryResolverInput
from metricflow.specs.patterns.metric_pattern import MetricSpecPattern
from metricflow.specs.patterns.spec_pattern import SpecPattern


@dataclass(frozen=True)
class InvalidStringInput(MetricFlowQueryResolverInput):
    """A string input that doesn't match any of the known naming schemes."""

    input_obj: str

    @property
    @override
    def ui_description(self) -> str:
        return self.input_obj


@dataclass(frozen=True)
class ResolverInputForMetric(MetricFlowQueryResolverInput):
    """An input that describes the metrics in the query."""

    input_obj: Union[MetricQueryParameter, str]
    naming_scheme: MetricNamingScheme
    spec_pattern: MetricSpecPattern

    @property
    @override
    def ui_description(self) -> str:
        return str(self.input_obj)

    @property
    @override
    def input_pattern_description(self) -> InputPatternDescription:
        return InputPatternDescription(
            naming_scheme=self.naming_scheme,
            spec_pattern=self.spec_pattern,
        )


@dataclass(frozen=True)
class ResolverInputForGroupByItem(MetricFlowQueryResolverInput):
    """An input that describes a group-by item in the query."""

    input_obj: Union[GroupByParameter, str]
    input_obj_naming_scheme: QueryItemNamingScheme
    spec_pattern: SpecPattern

    @property
    @override
    def ui_description(self) -> str:
        return str(self.input_obj)

    @property
    @override
    def input_pattern_description(self) -> InputPatternDescription:
        return InputPatternDescription(
            naming_scheme=self.input_obj_naming_scheme,
            spec_pattern=self.spec_pattern,
        )


@dataclass(frozen=True)
class ResolverInputForOrderByItem(MetricFlowQueryResolverInput):
    """An input that describes the ordered item.

    The challenge with order-by items is that it may not be obvious how to match an order-by item to a metric or a
    group-by item in the query. When the query inputs were always strings, this was easy because the order-by item
    could be resolved with an equality check. However, when the query inputs could be a string or a *QueryParameter
    object, the equality check is not possible. e.g. consider the case:

        group-by item: TimeDimension("creation_time"), order-by item: "creation_time".

    Instead, the approach is to resolve the metrics / group-by items into concrete spec objects, and then use the
    SpecPattern generated from the order-by item input to match to those.

    possible_inputs is necessary because at parse-time for string inputs, order-by inputs are resolved to spec patterns,
    and those patterns could match to either metrics or group-by-items.
    """

    input_obj: Union[str, OrderByQueryParameter]
    possible_inputs: Tuple[Union[ResolverInputForMetric, ResolverInputForGroupByItem], ...]
    descending: bool

    @property
    @override
    def ui_description(self) -> str:
        return str(self.input_obj)


@dataclass(frozen=True)
class ResolverInputForLimit(MetricFlowQueryResolverInput):
    """An input that describes the limit."""

    limit: Optional[int]

    @property
    @override
    def ui_description(self) -> str:
        return str(self.limit)


@dataclass(frozen=True)
class ResolverInputForWhereFilterIntersection(MetricFlowQueryResolverInput):
    """An input that describes the where filter."""

    where_filter_intersection: WhereFilterIntersection

    @property
    @override
    def ui_description(self) -> str:
        # TODO: Improve description.
        return (
            "WhereFilter("
            + mf_pformat(
                [where_filter.where_sql_template for where_filter in self.where_filter_intersection.where_filters]
            )
            + ")"
        )


@dataclass(frozen=True)
class ResolverInputForQuery(MetricFlowQueryResolverInput):
    """An input that describes the entire query."""

    metric_inputs: Tuple[ResolverInputForMetric, ...]
    group_by_item_inputs: Tuple[ResolverInputForGroupByItem, ...]
    filter_input: ResolverInputForWhereFilterIntersection
    order_by_item_inputs: Tuple[ResolverInputForOrderByItem, ...]
    limit_input: ResolverInputForLimit

    @property
    @override
    def ui_description(self) -> str:
        return (
            f"Query({repr([metric_input.ui_description for metric_input in self.metric_inputs])}, "
            f"{repr([group_by_item_input.input_obj for group_by_item_input in self.group_by_item_inputs])}"
        )
