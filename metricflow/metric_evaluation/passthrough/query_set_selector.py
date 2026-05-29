from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from typing import Optional

from metricflow_semantics.errors.error_classes import MetricFlowInternalError
from metricflow_semantics.toolkit.collections.ordered_set import FrozenOrderedSet, MutableOrderedSet, OrderedSet
from metricflow_semantics.toolkit.mf_logging.lazy_formattable import LazyFormat

from metricflow.metric_evaluation.plan.me_nodes import MetricQueryNode
from metricflow.metric_evaluation.plan.query_element import MetricQueryElement


class BestMetricQuerySetSelector:
    """Select the candidate query nodes that best satisfy required query elements.

    For example, given input query nodes:

    {
        MetricQueryNode(['bookings', 'listings']),
        MetricQueryNode(['views'])
        MetricQueryNode(['bookings']),
    }

    and desired metrics {'bookings', 'views'} to compute 'bookings_per_view', this would select:

    {
        MetricQueryNode(['bookings', 'listings']),
        MetricQueryNode(['views'])
    }

    See comments below for the selection criteria.
    """

    def __init__(  # noqa: D107
        self,
        query_element_to_level: Mapping[MetricQueryElement, int],
    ) -> None:
        self._query_element_to_level = query_element_to_level

    def find_best_queries(
        self,
        desired_query_elements: OrderedSet[MetricQueryElement],
        candidate_input_nodes: Sequence[MetricQueryNode],
    ) -> FindBestQuerySetResult:
        """Map each required input query element to a candidate query node.

        This prefers candidates that satisfy the largest set of remaining inputs to maximize passthrough reuse and
        reduce the number of joins needed to combine metrics.
        """
        # The most nested derived metric likely has the most passthrough metric specs available, so try to fulfill those
        # first. Passthrough metrics from that output node could be reused to fulfill other elements.
        remaining_desired_query_elements = MutableOrderedSet(
            sorted(
                desired_query_elements,
                key=self._query_element_level,
                reverse=True,
            )
        )

        input_query_node_to_fulfilled_query_elements: dict[MetricQueryNode, FrozenOrderedSet[MetricQueryElement]] = {}
        while remaining_desired_query_elements:
            selected_query_node = self._select_query_with_best_coverage(
                desired_query_elements=remaining_desired_query_elements,
                candidate_input_nodes=candidate_input_nodes,
            )

            if selected_query_node is None:
                return FindBestQuerySetResult(
                    remaining_desired_query_elements=remaining_desired_query_elements.as_frozen(),
                    input_query_node_to_fulfilled_query_elements=input_query_node_to_fulfilled_query_elements,
                )
            fulfilled_query_elements = remaining_desired_query_elements.intersection(
                selected_query_node.output_query_elements
            ).as_frozen()
            input_query_node_to_fulfilled_query_elements[selected_query_node] = fulfilled_query_elements
            remaining_desired_query_elements.difference_update(fulfilled_query_elements)

        return FindBestQuerySetResult(
            remaining_desired_query_elements=remaining_desired_query_elements.as_frozen(),
            input_query_node_to_fulfilled_query_elements=dict(input_query_node_to_fulfilled_query_elements),
        )

    def _query_element_level(self, query_element: MetricQueryElement) -> int:
        """Return the evaluation level for a query element."""
        query_level = self._query_element_to_level.get(query_element)
        if query_level is None:
            raise MetricFlowInternalError(
                LazyFormat(
                    "Missing evaluation level for query element.",
                    query_element=query_element,
                    known_query_elements=tuple(self._query_element_to_level),
                )
            )
        return query_level

    def _select_query_with_best_coverage(
        self,
        desired_query_elements: OrderedSet[MetricQueryElement],
        candidate_input_nodes: Sequence[MetricQueryNode],
    ) -> Optional[MetricQueryNode]:
        """Select the candidate query node that best covers the remaining required query elements."""
        if not candidate_input_nodes:
            return None

        def candidate_score(metric_query_node: MetricQueryNode) -> tuple[int, int]:
            fulfilled_query_elements = desired_query_elements.intersection(metric_query_node.output_query_elements)
            return (len(fulfilled_query_elements), len(metric_query_node.output_query_elements))

        selected_query = max(candidate_input_nodes, key=candidate_score)

        if len(desired_query_elements.intersection(selected_query.output_query_elements)) == 0:
            return None

        return selected_query


@dataclass
class FindBestQuerySetResult:
    """Result of selecting query nodes for the required query elements."""

    remaining_desired_query_elements: FrozenOrderedSet[MetricQueryElement]
    input_query_node_to_fulfilled_query_elements: Mapping[MetricQueryNode, FrozenOrderedSet[MetricQueryElement]]
