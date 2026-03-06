from __future__ import annotations

import itertools
import logging
from collections import defaultdict
from collections.abc import Iterable, Mapping
from functools import cached_property

from metricflow_semantics.toolkit.collections.ordered_set import FrozenOrderedSet, MutableOrderedSet, OrderedSet

from metricflow.metric_evaluation.plan.me_edges import MetricQueryDependencyEdge
from metricflow.metric_evaluation.plan.me_nodes import DerivedMetricsQueryNode, MetricQueryNode
from metricflow.metric_evaluation.plan.me_plan import MutableMetricEvaluationPlan
from metricflow.metric_evaluation.plan.query_element import MetricQueryPropertySet

logger = logging.getLogger(__name__)


class DerivedMetricSourceGrouper:
    def __init__(
        self,
        derived_metric_nodes: OrderedSet[DerivedMetricsQueryNode],
        source_edges: OrderedSet[MetricQueryDependencyEdge],
    ) -> None:
        self._derived_metric_nodes = derived_metric_nodes.as_frozen()
        self._source_edges = source_edges.as_frozen()
        self._subplan = MutableMetricEvaluationPlan.create()
        self._subplan.add_edges(source_edges)

        # TODO: Add checks for structure.

    @cached_property
    def _groupable_nodes(self) -> FrozenOrderedSet[DerivedMetricsQueryNode]:
        return FrozenOrderedSet(
            node
            for node in self._derived_metric_nodes
            # if all(metric_spec.alias is None for metric_spec in node.output_metric_specs)
        )

    @cached_property
    def _non_groupable_nodes(self) -> FrozenOrderedSet[DerivedMetricsQueryNode]:
        return FrozenOrderedSet(node for node in self._derived_metric_nodes if node not in self._non_groupable_nodes)

    def group_by_source_node_set(
        self, nodes: Iterable[DerivedMetricsQueryNode]
    ) -> Mapping[FrozenOrderedSet[MetricQueryNode], OrderedSet[DerivedMetricsQueryNode]]:
        source_node_set_to_target_nodes: defaultdict[
            FrozenOrderedSet[MetricQueryNode], MutableOrderedSet[DerivedMetricsQueryNode]
        ] = defaultdict(MutableOrderedSet)

        for node in nodes:
            source_node_set_to_target_nodes[self._subplan.source_nodes(node).as_frozen()].add(node)

        return source_node_set_to_target_nodes

    # def group_by_metric_modifier(self, nodes: Iterable[DerivedMetricsQueryNode]) -> dict[MetricModifier, OrderedSet[DerivedMetricsQueryNode]]:
    #     modifier_to_nodes: defaultdict[MetricModifier, MutableOrderedSet[DerivedMetricsQueryNode]] = defaultdict(MutableOrderedSet)
    #
    #     for node in nodes:
    #         metric_modifiers = FrozenOrderedSet(
    #             computed_metric_spec for computed_metric_spec in node.computed_metric_specs
    #         )
    #         assert len(metric_modifiers) == 1
    #         metric_modifier = mf_first_item(metric_modifiers)
    #
    #         modifier_to_nodes[metric_modifier].add(node)
    #     return modifier_to_nodes

    def group_by_query_properties(
        self, nodes: Iterable[DerivedMetricsQueryNode]
    ) -> Mapping[MetricQueryPropertySet, OrderedSet[DerivedMetricsQueryNode]]:
        query_property_set_to_nodes: defaultdict[
            MetricQueryPropertySet, MutableOrderedSet[DerivedMetricsQueryNode]
        ] = defaultdict(MutableOrderedSet)

        for node in nodes:
            query_property_set_to_nodes[node.query_properties].add(node)
        return query_property_set_to_nodes

    def group_nodes(self) -> tuple[OrderedSet[DerivedMetricsQueryNode], OrderedSet[MetricQueryDependencyEdge]]:
        new_nodes: MutableOrderedSet[DerivedMetricsQueryNode] = MutableOrderedSet()
        new_edges: MutableOrderedSet[MetricQueryDependencyEdge] = MutableOrderedSet()

        source_node_set_to_target_nodes = self.group_by_source_node_set(self._derived_metric_nodes)

        for source_node_set, target_nodes in source_node_set_to_target_nodes.items():
            query_properties_to_nodes = self.group_by_query_properties(target_nodes)

            for query_properties, nodes in query_properties_to_nodes.items():
                nodes_without_aliased_metrics: MutableOrderedSet[DerivedMetricsQueryNode] = MutableOrderedSet()
                nodes_with_aliased_metrics: MutableOrderedSet[DerivedMetricsQueryNode] = MutableOrderedSet()

                for node in nodes:
                    if any(metric_spec.alias is not None for metric_spec in node.output_metric_specs):
                        nodes_with_aliased_metrics.add(node)
                    else:
                        nodes_without_aliased_metrics.add(node)

                # For nodes without aliased metrics and sharing the same sources, combine them into a single node
                # that computes the derived metrics in all of them.
                # TODO: Check if passthrough is supposed to be the same
                nodes_without_aliased_metrics_tuple = tuple(nodes_without_aliased_metrics)
                passthrough_metric_specs = MutableOrderedSet(
                    nodes_without_aliased_metrics_tuple[0].passthrough_metric_specs
                    if len(nodes_without_aliased_metrics_tuple) > 0
                    else ()
                )
                for node in nodes_without_aliased_metrics_tuple[1:]:
                    passthrough_metric_specs.intersection_update(node.passthrough_metric_specs)

                computed_metric_specs = FrozenOrderedSet(
                    itertools.chain.from_iterable(node.computed_metric_specs for node in nodes_without_aliased_metrics)
                )
                derived_metric_node = DerivedMetricsQueryNode.create(
                    computed_metric_specs=computed_metric_specs,
                    passthrough_metric_specs=passthrough_metric_specs,
                    query_properties=query_properties,
                )
                new_nodes.add(derived_metric_node)

                for node in nodes_with_aliased_metrics:
                    new_nodes.add(node)

                for edge in self._source_edges:
                    if edge.target_node in nodes_without_aliased_metrics and (
                        edge.source_node_output_spec in passthrough_metric_specs
                        or edge.source_node_output_spec in computed_metric_specs
                    ):
                        new_edges.add(
                            MetricQueryDependencyEdge.create(
                                target_node=derived_metric_node,
                                target_node_output_spec=edge.target_node_output_spec,
                                source_node=edge.source_node,
                                source_node_output_spec=edge.source_node_output_spec,
                            )
                        )
                    else:
                        new_edges.add(edge)

        return (new_nodes, new_edges)
