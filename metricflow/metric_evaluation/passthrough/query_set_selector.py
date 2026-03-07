from __future__ import annotations

import logging
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from typing import Optional

from metricflow_semantics.toolkit.collections.ordered_set import FrozenOrderedSet, MutableOrderedSet, OrderedSet

from metricflow.metric_evaluation.plan.me_nodes import MetricQueryNode
from metricflow.metric_evaluation.plan.query_element import MetricQueryElement

logger = logging.getLogger(__name__)

MetricQueryElementGroup = OrderedSet[MetricQueryElement]


class BestMetricQuerySetSelector:
    """Helps select the set of queries that can best realize a set of input metrics."""

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
                key=lambda query_element: self._query_element_to_level[query_element],
                reverse=True,
            )
        )

        input_query_node_to_fulfilled_query_elements: dict[MetricQueryNode, OrderedSet[MetricQueryElement]] = {}
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
            )
            input_query_node_to_fulfilled_query_elements[selected_query_node] = fulfilled_query_elements
            remaining_desired_query_elements.difference_update(fulfilled_query_elements)

        return FindBestQuerySetResult(
            remaining_desired_query_elements=remaining_desired_query_elements.as_frozen(),
            input_query_node_to_fulfilled_query_elements=input_query_node_to_fulfilled_query_elements,
        )

    def _select_query_with_best_coverage(
        self,
        desired_query_elements: OrderedSet[MetricQueryElement],
        candidate_input_nodes: Sequence[MetricQueryNode],
    ) -> Optional[MetricQueryNode]:
        """Select the candidate query node that best covers the remaining required query elements."""

        def candidate_score(metric_query_node: MetricQueryNode) -> tuple[int, int]:
            fulfilled_query_elements = desired_query_elements.intersection(metric_query_node.output_query_elements)
            return (len(fulfilled_query_elements), len(metric_query_node.output_query_elements))

        selected_query = max(candidate_input_nodes, key=candidate_score)

        if len(desired_query_elements.intersection(selected_query.output_query_elements)) == 0:
            return None

        return selected_query


@dataclass
class FindBestQuerySetResult:
    """Used to return the result of finding the best set of queries."""

    remaining_desired_query_elements: FrozenOrderedSet[MetricQueryElement]
    input_query_node_to_fulfilled_query_elements: dict[MetricQueryNode, OrderedSet[MetricQueryElement]]
