from __future__ import annotations

from abc import ABC, abstractmethod
from collections.abc import Set
from functools import cached_property
from typing import Iterable, Mapping, Optional

from metricflow_semantics.specs.instance_spec import LinkableInstanceSpec
from metricflow_semantics.specs.linkable_spec_set import LinkableSpecSet
from metricflow_semantics.specs.metric_spec import MetricSpec
from metricflow_semantics.toolkit.collections.ordered_set import FrozenOrderedSet, OrderedSet
from metricflow_semantics.toolkit.dataclass_helpers import fast_frozen_dataclass
from metricflow_semantics.toolkit.mf_logging.lazy_formattable import LazyFormat
from metricflow_semantics.toolkit.mf_logging.pretty_formattable import MetricFlowPrettyFormattable
from metricflow_semantics.toolkit.mf_logging.pretty_formatter import PrettyFormatContext
from typing_extensions import override

from metricflow.plan_conversion.node_processor import PredicatePushdownState


class MetricQueryElementLookup(ABC):
    """A lookup object to get relevant query elements and associated inputs."""

    @property
    @abstractmethod
    def query_elements(self) -> Set[MetricQueryElement]:
        """Return all contained query elements."""
        raise NotImplementedError

    @abstractmethod
    def get_input_query_elements(self, query_element: MetricQueryElement) -> OrderedSet[MetricQueryElement]:
        """Return the input query elements for the given query element (e.g. for derived metrics)."""
        raise NotImplementedError

    @property
    @abstractmethod
    def query_element_to_input_elements(self) -> Mapping[MetricQueryElement, OrderedSet[MetricQueryElement]]:
        """Similar to `get_input_query_elements()` but as a complete mapping."""
        raise NotImplementedError


class MetricQueryElementCollector(MetricQueryElementLookup):
    """A mutable class to collect query elements during traversal of the metric dependency graph."""

    def __init__(self) -> None:  # noqa: D107
        self._query_element_to_input_elements: dict[MetricQueryElement, FrozenOrderedSet[MetricQueryElement]] = {}

    def add_query_element(  # noqa: D102
        self, query_element: MetricQueryElement, input_query_elements: Optional[Iterable[MetricQueryElement]]
    ) -> None:
        """Add a query element and the query elements that it directly depends on."""
        if query_element in self._query_element_to_input_elements:
            raise RuntimeError(LazyFormat("Query element already added", query_element=query_element))
        self._query_element_to_input_elements[query_element] = (
            FrozenOrderedSet(input_query_elements) if input_query_elements is not None else FrozenOrderedSet()
        )

    @property
    @override
    def query_elements(self) -> Set[MetricQueryElement]:  # noqa: D102
        """Return all query elements in insertion order."""
        return self._query_element_to_input_elements.keys()

    @override
    def get_input_query_elements(self, query_element: MetricQueryElement) -> OrderedSet[MetricQueryElement]:
        """Return direct dependencies for a query element."""
        input_query_elements = self._query_element_to_input_elements.get(query_element)
        if input_query_elements is None:
            raise ValueError(
                LazyFormat(
                    "Unknown query element",
                    query_element=query_element,
                    known_elements=self._query_element_to_input_elements.keys(),
                )
            )
        return input_query_elements

    @property
    @override
    def query_element_to_input_elements(self) -> Mapping[MetricQueryElement, OrderedSet[MetricQueryElement]]:
        """Return all query elements with their direct dependencies."""
        return self._query_element_to_input_elements


@fast_frozen_dataclass()
class MetricQueryPropertySet(MetricFlowPrettyFormattable):
    """Properties that are used to group query elements into a common query."""

    group_by_item_specs: FrozenOrderedSet[LinkableInstanceSpec]
    predicate_pushdown_state: PredicatePushdownState

    @staticmethod
    def create(
        group_by_item_specs: Iterable[LinkableInstanceSpec], predicate_pushdown_state: PredicatePushdownState
    ) -> MetricQueryPropertySet:
        """Create properties that define whether query elements can be composed together."""
        return MetricQueryPropertySet(
            group_by_item_specs=FrozenOrderedSet.from_iterable(group_by_item_specs),
            predicate_pushdown_state=predicate_pushdown_state,
        )

    @override
    def pretty_format(self, format_context: PrettyFormatContext) -> Optional[str]:
        return format_context.formatter.pretty_format_object_by_parts(
            class_name=self.__class__.__name__,
            field_mapping={
                "group_by_item_specs": [spec.dunder_name for spec in self.group_by_item_specs],
                "pushdown_enabled_types": self.predicate_pushdown_state.pushdown_enabled_types,
            },
        )

    @cached_property
    def group_by_item_spec_set(self) -> LinkableSpecSet:  # noqa: D102
        return LinkableSpecSet.create_from_specs(self.group_by_item_specs)


@fast_frozen_dataclass()
class MetricQueryElement:
    """A composable element that is used to build a query for metrics / group-by items.

    For example, `bookings, listings by metric_time` can be broken down into the query elements
    `bookings by metric_time` and `listings by metric_time`.

    Similarly, a query for `bookings_per_listing by metric_time` can be composed using query elements
    `bookings by metric_time` and `listings by metric_time`.

    For query elements to be composed into a query, the query elements must have the same query properties.
    """

    metric_spec: MetricSpec
    query_properties: MetricQueryPropertySet

    @staticmethod
    def create(
        metric_spec: MetricSpec,
        group_by_item_specs: Iterable[LinkableInstanceSpec],
        predicate_pushdown_state: PredicatePushdownState,
    ) -> MetricQueryElement:
        """Create a query element for a metric with the associated query properties."""
        return MetricQueryElement(
            metric_spec=metric_spec,
            query_properties=MetricQueryPropertySet.create(
                group_by_item_specs=group_by_item_specs,
                predicate_pushdown_state=predicate_pushdown_state,
            ),
        )

    @cached_property
    def group_by_item_specs(self) -> OrderedSet[LinkableInstanceSpec]:
        """Return the group-by specs required for this query element."""
        return self.query_properties.group_by_item_specs

    @cached_property
    def predicate_pushdown_state(self) -> PredicatePushdownState:
        """Return filter pushdown properties associated with this query element."""
        return self.query_properties.predicate_pushdown_state

    @cached_property
    def metric_name(self) -> str:
        """Return the metric name for this element."""
        return self.metric_spec.element_name
