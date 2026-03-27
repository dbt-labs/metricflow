from __future__ import annotations

import logging
from collections.abc import Iterable, Sequence
from typing import Optional

from dbt_semantic_interfaces.enum_extension import assert_values_exhausted
from dbt_semantic_interfaces.protocols import Metric
from dbt_semantic_interfaces.type_enums import MetricType
from metricflow_semantics.semantic_graph.model_id import SemanticModelId
from metricflow_semantics.specs.instance_spec import LinkableInstanceSpec
from metricflow_semantics.specs.metric_spec import MetricSpec
from metricflow_semantics.specs.where_filter.where_filter_spec_factory import WhereFilterSpecFactory
from metricflow_semantics.toolkit.mf_logging.lazy_formattable import LazyFormat
from typing_extensions import override

from metricflow.metric_evaluation.metric_query_planner import MetricEvaluationPlanner
from metricflow.metric_evaluation.plan.me_edges import MetricQueryDependencyEdge
from metricflow.metric_evaluation.plan.me_nodes import (
    ConversionMetricQueryNode,
    CumulativeMetricQueryNode,
    DerivedMetricsQueryNode,
    MetricQueryNode,
    SimpleMetricsQueryNode,
    TopLevelQueryNode,
)
from metricflow.metric_evaluation.plan.me_plan import (
    MetricEvaluationPlan,
    MutableMetricEvaluationPlan,
)
from metricflow.metric_evaluation.plan.query_element import MetricQueryElement, MetricQueryPropertySet
from metricflow.plan_conversion.node_processor import PredicatePushdownState

logger = logging.getLogger(__name__)


class DepthFirstSearchMetricEvaluationPlanner(MetricEvaluationPlanner):
    """Builds a metric evaluation plan using a depth-first traversal of the metric dependency graph.

    For example, the metric evaluation plan for the query [`bookings_per_listing`, `bookings`] results in a plan
    that has the following edges:

        MetricQuery([`bookings_per_listing`]) -> MetricQuery([`bookings`])
        MetricQuery([`bookings_per_listing`]) -> MetricQuery([`listings`])
        Top Level Query -> MetricQuery([`bookings_per_listing`])
        Top Level Query -> MetricQuery([`bookings`])

    This mirrors the original approach to compute metrics in the `DataflowPlanBuilder`.
    """

    @override
    def build_plan(
        self,
        metric_specs: Sequence[MetricSpec],
        group_by_item_specs: Sequence[LinkableInstanceSpec],
        predicate_pushdown_state: PredicatePushdownState,
        filter_spec_factory: WhereFilterSpecFactory,
    ) -> MetricEvaluationPlan:
        """Build a metric evaluation plan using iterative depth-first traversal.

        This resolves each requested metric into metric-query nodes and dependency edges, then attaches a top-level
        query node that references all requested metrics.
        """
        top_level_query_elements = tuple(
            MetricQueryElement.create(
                metric_spec=metric_spec,
                group_by_item_specs=group_by_item_specs,
                predicate_pushdown_state=predicate_pushdown_state,
            )
            for metric_spec in metric_specs
        )

        evaluation_plan = MutableMetricEvaluationPlan.create()

        # The query elements to process in the iterative DFS traversal loop. The next element is popped from the right
        # so elements are added in reverse to preserve order.
        query_elements_to_process: list[MetricQueryElement] = list(reversed(top_level_query_elements))
        # Keeps track of the query elements that have been processed into a node in the evaluation plan.
        query_element_to_node: dict[MetricQueryElement, MetricQueryNode] = {}

        while query_elements_to_process:
            current_query_element = query_elements_to_process.pop()
            logger.debug(LazyFormat("Handling query element", current_query_element=current_query_element))
            if current_query_element in query_element_to_node:
                continue

            current_metric_spec = current_query_element.metric_spec
            current_query_properties = current_query_element.query_properties
            current_predicate_pushdown_state = current_query_element.predicate_pushdown_state

            metric_name = current_metric_spec.element_name
            metric = self._manifest_object_lookup.get_metric(metric_name)
            metric_type = metric.type

            # Handle non-derived metrics.
            metric_query_node = self._create_base_metric_query_node(
                metric=metric,
                metric_type=metric_type,
                metric_spec=current_metric_spec,
                query_properties=current_query_properties,
            )
            if metric_query_node is not None:
                evaluation_plan.add_node(metric_query_node)
                query_element_to_node[current_query_element] = metric_query_node
                continue

            # Handle derived metrics.
            input_query_elements = self._get_input_metric_query_elements_for_derived_metric(
                metric_spec=current_metric_spec,
                group_by_item_specs=current_query_element.group_by_item_specs,
                predicate_pushdown_state=current_predicate_pushdown_state,
                filter_spec_factory=filter_spec_factory,
            )
            assert len(input_query_elements) > 0, LazyFormat(
                "Expected a ratio or derived metric to have input query elements",
                current_metric_spec=current_metric_spec,
                metric=metric,
            )

            inputs_that_need_processing = tuple(
                input_query_element
                for input_query_element in input_query_elements
                if input_query_element not in query_element_to_node
            )
            # To implement DFS traversal, check if the input nodes have been processed. If not, add the input nodes
            # for processing and then try to process the current node again.
            if len(inputs_that_need_processing) > 0:
                # Add the current node first as the loop pops the next current element from the end.
                query_elements_to_process.append(current_query_element)
                # Adding inputs in reverse order to match traversal order with definition order.
                query_elements_to_process.extend(reversed(inputs_that_need_processing))
                continue

            # All inputs of the derived metric have been processed, so add the node for the derived metric and the
            # edges.
            derived_metric_query_node = DerivedMetricsQueryNode.create(
                computed_metric_specs=[current_metric_spec],
                passthrough_metric_specs=(),
                query_properties=current_query_properties,
            )
            evaluation_plan.add_node(derived_metric_query_node)

            for input_query_element in input_query_elements:
                input_query_node = query_element_to_node[input_query_element]
                evaluation_plan.add_edge(
                    MetricQueryDependencyEdge.create(
                        target_node=derived_metric_query_node,
                        target_node_output_spec=current_metric_spec,
                        source_node=input_query_node,
                        source_node_output_spec=input_query_element.metric_spec,
                    )
                )

            query_element_to_node[current_query_element] = derived_metric_query_node

        # Once nodes for all metrics in the query have been generated, add a `TopLevelQueryNode` to provide a single
        # entry point.
        top_level_query_node = TopLevelQueryNode.create(
            passthrough_metric_specs=metric_specs,
            query_properties=MetricQueryPropertySet.create(group_by_item_specs, predicate_pushdown_state),
        )
        evaluation_plan.add_node(top_level_query_node)

        for top_level_query_element in top_level_query_elements:
            evaluation_plan.add_edge(
                MetricQueryDependencyEdge.create(
                    target_node=top_level_query_node,
                    target_node_output_spec=top_level_query_element.metric_spec,
                    source_node=query_element_to_node[top_level_query_element],
                    source_node_output_spec=top_level_query_element.metric_spec,
                )
            )

        return evaluation_plan

    def _create_base_metric_query_node(
        self,
        metric: Metric,
        metric_type: MetricType,
        metric_spec: MetricSpec,
        query_properties: MetricQueryPropertySet,
    ) -> Optional[MetricQueryNode]:
        """Return a node for base metric types or `None` for metrics that require dependency expansion."""
        if metric_type is MetricType.SIMPLE:
            metric_aggregation_params = metric.type_params.metric_aggregation_params
            if metric_aggregation_params is None:
                raise ValueError(
                    LazyFormat(
                        "Simple metric is missing metric aggregation parameters",
                        metric_spec=metric_spec,
                        metric=metric,
                    )
                )
            return SimpleMetricsQueryNode.create(
                model_id=SemanticModelId.get_instance(metric_aggregation_params.semantic_model),
                metric_specs=(metric_spec,),
                query_properties=query_properties,
            )

        if metric_type is MetricType.CUMULATIVE:
            return CumulativeMetricQueryNode.create(metric_spec=metric_spec, query_properties=query_properties)
        elif metric_type is MetricType.CONVERSION:
            return ConversionMetricQueryNode.create(metric_spec=metric_spec, query_properties=query_properties)
        elif metric_type is MetricType.RATIO or metric_type is MetricType.DERIVED:
            return None
        else:
            assert_values_exhausted(metric_type)

    def _get_input_metric_query_elements_for_derived_metric(
        self,
        metric_spec: MetricSpec,
        group_by_item_specs: Iterable[LinkableInstanceSpec],
        predicate_pushdown_state: PredicatePushdownState,
        filter_spec_factory: WhereFilterSpecFactory,
    ) -> Sequence[MetricQueryElement]:
        """Return input query elements for a ratio / derived metric.

        Input query elements generally inherit group-by and predicate settings from the metric being expanded.
        Time-offset metrics are handled differently - see appropriate section in the `DataflowPlanBuilder`.
        """
        additional_filter_specs = metric_spec.where_filter_specs
        group_by_item_specs_for_inputs = group_by_item_specs
        predicate_pushdown_state_for_inputs = predicate_pushdown_state

        if metric_spec.has_time_offset:
            group_by_item_specs_for_inputs = self._query_helper.resolve_group_by_specs_for_time_offset_metric_input(
                queried_group_by_specs=group_by_item_specs,
                filter_specs=metric_spec.where_filter_specs,
            )
            predicate_pushdown_state_for_inputs = PredicatePushdownState.with_pushdown_disabled()
            # If metric is offset, we'll apply where constraint after offset to avoid removing values
            # unexpectedly. Time constraint will be applied by INNER JOINing to time spine.
            # We may consider encapsulating this in pushdown state later, but as of this moment pushdown
            # is about post-join to pre-join for dimension access, and relies on the builder to collect
            # predicates from query and metric specs and make them available at simple-metric-input level.
            additional_filter_specs = ()

        input_metric_specs = self._build_input_metric_specs_for_derived_metric(
            metric_name=metric_spec.element_name,
            filter_spec_factory=filter_spec_factory,
            additional_filter_specs=additional_filter_specs,
        )
        return tuple(
            MetricQueryElement.create(
                metric_spec=input_metric_spec,
                group_by_item_specs=group_by_item_specs_for_inputs,
                predicate_pushdown_state=predicate_pushdown_state_for_inputs,
            )
            for input_metric_spec in input_metric_specs
        )
