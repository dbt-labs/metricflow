from __future__ import annotations

import logging
import pathlib
import typing
from abc import ABC, abstractmethod
from collections import defaultdict
from collections.abc import Mapping, Sequence, Set, Iterable
from dataclasses import dataclass
from enum import Enum
from pathlib import Path
from typing import Callable, Generic, Optional, TypeVar

from typing_extensions import Self, override

from metricflow.dataflow.metric_evaluation.me_elements import MetricDescriptor, MetricDescriptorSet
from metricflow.dataflow.metric_evaluation.me_graph import MutableMetricQueryGraph, MetricQueryNode, \
    BaseMetricQueryNode, MetricQueryEdge, RecursiveMetricQueryNode
from metricflow.dataflow.metric_evaluation.me_level_resolver import MetricEvaluationLevelResolver
from metricflow.dataflow.metric_evaluation.me_descriptor_collector import MetricDescriptorCollector
from metricflow.dataflow.metric_evaluation.me_planner import MetricQuery
from metricflow_semantics.semantic_graph.lookups.manifest_object_lookup import ManifestObjectLookup
from metricflow_semantics.specs.metric_spec import MetricSpec
from metricflow_semantics.toolkit.collections.ordered_set import FrozenOrderedSet, OrderedSet, MutableOrderedSet
from metricflow_semantics.toolkit.mf_logging.lazy_formattable import LazyFormat
from metricflow_semantics.toolkit.string_helpers import mf_dedent
from metricflow_semantics.toolkit.syntactic_sugar import mf_first_item

logger = logging.getLogger(__name__)



class GraphMetricEvaluationPlanner:
    def __init__(
        self,
        manifest_object_lookup: ManifestObjectLookup,
    ) -> None:
        self._manifest_object_lookup = manifest_object_lookup
        self._level_resolver: MetricEvaluationLevelResolver = MetricEvaluationLevelResolver(manifest_object_lookup)

    def build_plan(self, metric_specs: Iterable[MetricSpec]) -> None:
        # Figure out all forms of metrics that are required.
        descriptor_collector = MetricDescriptorCollector()
        top_level_query_metric_descriptors = FrozenOrderedSet(
            MetricDescriptor.create_from_spec(metric_spec) for metric_spec in metric_specs
        )
        logger.debug(
            LazyFormat(
                "Building metric evaluation plan",
                top_level_query_metric_descriptors=top_level_query_metric_descriptors,
            )
        )
        for metric_descriptor in top_level_query_metric_descriptors:
            self._recursively_collect_descriptors(
                metric_descriptor=metric_descriptor,
                descriptor_collector=descriptor_collector,
            )

        logger.debug(
            LazyFormat(
                "Recursively collected metric descriptors",
                metric_specs=metric_specs,
                descriptor_to_input_descriptors=descriptor_collector.descriptor_to_input_descriptors,
            )
        )

        # Group them by evaluation level
        level_to_descriptors = self._group_descriptors_by_level(descriptor_collector)
        assert min(level_to_descriptors) == 0
        max_level = max(level_to_descriptors)
        assert tuple(level_to_descriptors) == tuple(range(max_level + 1))
        descriptor_to_level: dict[MetricDescriptor, int] = {}
        for level, descriptors in level_to_descriptors.items():
            for descriptor in descriptors:
                descriptor_to_level[descriptor] = level

        logger.debug(LazyFormat("Grouped metric descriptors by level", level_to_descriptors=level_to_descriptors))

        base_metric_descriptors = level_to_descriptors[0]

        query_graph = MutableMetricQueryGraph.create()

        base_metric_nodes = self._get_nodes_for_base_metrics(base_metric_descriptors)
        logger.debug(
            LazyFormat(
                "Generated queries for base metrics",
                base_metric_descriptors=base_metric_descriptors,
                base_metric_nodes=base_metric_nodes,
            )
        )
        query_graph.add_nodes(base_metric_nodes)

        candidate_queries = base_metric_nodes
        level_to_output_queries: dict[int, OrderedSet[MetricQuery]] = {}
        for level in range(1, max_level + 1):
            metric_descriptors = level_to_descriptors[level]
            output_queries = self._generate_queries_for_recursive_metrics(
                metric_descriptors=metric_descriptors,
                candidate_queries=candidate_queries,
                descriptor_collector=descriptor_collector,
                descriptor_to_level=descriptor_to_level,
            )
            level_to_output_queries[level] = output_queries
            logger.info(
                LazyFormat(
                    "Resolved queries",
                    level=level,
                    metric_descriptor=metric_descriptors,
                    input_queries=candidate_queries,
                    output_queries=output_queries,
                )
            )
            candidate_queries = output_queries.union(candidate_queries)

        top_level_descriptor_to_input_query = self._find_best_input_query_nodes(
            input_descriptors=top_level_query_metric_descriptors,
            candidate_input_query_nodes=candidate_queries,
            descriptor_to_level=descriptor_to_level,
        )
        if top_level_descriptor_to_input_query is None:
            raise RuntimeError(
                LazyFormat(
                    "Unable to get all top-level metrics from candidate queries. This indicates an error in candidate"
                    " query generation.",
                    top_level_query_metric_descriptors=top_level_query_metric_descriptors,
                    candidate_queries=candidate_queries,
                )
            )
        input_query_to_top_level_descriptors: defaultdict[
            MetricQuery, MutableOrderedSet[MetricDescriptor]
        ] = defaultdict(MutableOrderedSet)
        for top_level_descriptor, input_query in top_level_descriptor_to_input_query.items():
            input_query_to_top_level_descriptors[input_query].add(top_level_descriptor)

        top_level_query = RecursiveMetricQuery.create(
            computed_metric_descriptors=(),
            passthrough_metric_descriptors=top_level_query_metric_descriptors,
            input_query_to_required_descriptors=input_query_to_top_level_descriptors,
        )

        logger.debug(
            LazyFormat(
                "Generated top-level query",
                top_level_query=top_level_query,
            )
        )

        logger.debug(
            LazyFormat(
                "Generated pruned top-level query",
                top_level_query=top_level_query.pruned_query(top_level_query_metric_descriptors),
            )
        )
        return None

    def _prune_query(self, metric_query: MetricQuery, required_metrics: OrderedSet[MetricDescriptor]) -> MetricQuery:
        raise NotImplementedError

    def _get_nodes_for_base_metrics(self, metric_descriptors: OrderedSet[MetricDescriptor]) -> Sequence[MetricQueryNode]:
        nodes: list[BaseMetricQueryNode] = []
        model_id_to_descriptors = self._group_simple_metric_descriptors_by_model_id(metric_descriptors)
        for model_id, descriptors in model_id_to_descriptors.items():
            nodes.append(
                BaseMetricQueryNode(
                    output_metric_descriptors=MetricDescriptorSet.create(computed_metric_descriptors=descriptors, passthrough_metric_descriptors=()))
            )
        return nodes

    def _generate_edges_for_recursive_metrics(
        self,
        output_metric_descriptors: Iterable[MetricDescriptor],
        candidate_input_query_nodes: OrderedSet[MetricQueryNode],
        descriptor_collector: MetricDescriptorCollector,
        descriptor_to_level: Mapping[MetricDescriptor, int],
    ) -> OrderedSet[MetricQuery]:
        edges_for_current_level: MutableOrderedSet[MetricQueryEdge] = MutableOrderedSet()

        # Sort to fulfill the metric with the largest number of inputs to use a greedy algorithm.
        sorted_output_metric_descriptors = sorted(
            output_metric_descriptors,
            key=lambda descriptor: len(descriptor_collector.get_input_descriptors(descriptor)),
            reverse=True,
        )

        for output_metric_descriptor in sorted_output_metric_descriptors:

            input_metric_descriptors = descriptor_collector.get_input_descriptors(output_metric_descriptor)
            input_metric_descriptor_to_input_query_node = self._find_best_input_query_nodes(
                input_descriptors=input_metric_descriptors,
                candidate_input_query_nodes=candidate_input_query_nodes,
                descriptor_to_level=descriptor_to_level,
            )

            if input_metric_descriptor_to_input_query_node is None:
                raise RuntimeError(
                    LazyFormat(
                        "Unable to resolve input metrics from candidate queries. The metric dependency graph may not"
                        " have been correctly constructed",
                        metric_descriptor=output_metric_descriptor,
                        input_descriptors=descriptor_collector.get_input_descriptors(output_metric_descriptor),
                        candidate_input_query_nodes=candidate_input_query_nodes,
                    )
                )

            input_query_node_to_required_metric_descriptors: defaultdict[
                MetricQueryNode, MutableOrderedSet[MetricDescriptor]
            ] = defaultdict(MutableOrderedSet)

            for input_descriptor in input_metric_descriptors:
                input_query_node_for_input_descriptor = input_metric_descriptor_to_input_query_node[input_descriptor]
                input_query_node_to_required_metric_descriptors[input_query_node_for_input_descriptor].add(input_descriptor)

            computed_metric_descriptors = FrozenOrderedSet((output_metric_descriptor,))
            passthrough_metric_descriptors: list[MetricDescriptor] = []

            if output_metric_descriptor.allows_passed_metrics:
                for input_descriptor, query_node in input_metric_descriptor_to_input_query_node.items():
                    passthrough_descriptors_for_query = tuple(
                        descriptor
                        for descriptor in query_node.output_metric_descriptors
                        if descriptor not in computed_metric_descriptors
                    )
                    input_query_node_to_required_metric_descriptors[query_node].update(passthrough_descriptors_for_query)
                    passthrough_metric_descriptors.extend(passthrough_descriptors_for_query)

            output_metric_node = RecursiveMetricQueryNode(
                output_metric_descriptors=MetricDescriptorSet.create(
                    computed_metric_descriptors=(output_metric_descriptor,),
                    passthrough_metric_descriptors=passthrough_metric_descriptors,
                )
            )
            

            queries_for_current_level.add(
                RecursiveMetricQuery.create(
                    computed_metric_descriptors=computed_metric_descriptors,
                    passthrough_metric_descriptors=passthrough_metric_descriptors,
                    input_query_to_required_descriptors=input_query_node_to_required_metric_descriptors,
                )
            )

        # TODO: `queries_for_current_level` can be further grouped by `input_queries`.
        return queries_for_current_level

    def _generate_queries_for_recursive_metrics(
        self,
        metric_descriptors: Iterable[MetricDescriptor],
        candidate_queries: OrderedSet[MetricQuery],
        descriptor_collector: MetricDescriptorCollector,
        descriptor_to_level: Mapping[MetricDescriptor, int],
    ) -> OrderedSet[MetricQuery]:
        queries_for_current_level: MutableOrderedSet[MetricQuery] = MutableOrderedSet()

        sorted_metric_descriptors = sorted(
            metric_descriptors,
            key=lambda descriptor: len(descriptor_collector.get_input_descriptors(descriptor)),
            reverse=True,
        )

        for metric_descriptor in sorted_metric_descriptors:
            input_descriptors = descriptor_collector.get_input_descriptors(metric_descriptor)
            input_descriptor_to_query = self._find_best_input_query_nodes(
                input_descriptors=input_descriptors,
                candidate_input_query_nodes=candidate_queries,
                descriptor_to_level=descriptor_to_level,
            )

            if input_descriptor_to_query is None:
                raise RuntimeError(
                    LazyFormat(
                        "Unable to resolve input metrics from candidate queries. The metric dependency graph may not"
                        " have been correctly constructed",
                        metric_descriptor=metric_descriptor,
                        input_descriptors=descriptor_collector.get_input_descriptors(metric_descriptor),
                        candidate_queries=candidate_queries,
                    )
                )
            input_query_to_required_metric_descriptors: defaultdict[
                MetricQuery, MutableOrderedSet[MetricDescriptor]
            ] = defaultdict(MutableOrderedSet)

            for input_descriptor in input_descriptors:
                input_query_for_input_descriptor = input_descriptor_to_query[input_descriptor]
                input_query_to_required_metric_descriptors[input_query_for_input_descriptor].add(input_descriptor)

            computed_metric_descriptors = FrozenOrderedSet((metric_descriptor,))
            passthrough_metric_descriptors: list[MetricDescriptor] = []

            if metric_descriptor.allows_passed_metrics:
                for input_descriptor, query in input_descriptor_to_query.items():
                    passthrough_descriptors_for_query = tuple(
                        descriptor
                        for descriptor in query.descriptor_set.descriptors
                        if descriptor not in computed_metric_descriptors
                    )
                    input_query_to_required_metric_descriptors[query].update(passthrough_descriptors_for_query)
                    passthrough_metric_descriptors.extend(passthrough_descriptors_for_query)

            queries_for_current_level.add(
                RecursiveMetricQuery.create(
                    computed_metric_descriptors=computed_metric_descriptors,
                    passthrough_metric_descriptors=passthrough_metric_descriptors,
                    input_query_to_required_descriptors=input_query_to_required_metric_descriptors,
                )
            )

        # TODO: `queries_for_current_level` can be further grouped by `input_queries`.
        return queries_for_current_level

    def _find_best_input_query_nodes(
        self,
        input_descriptors: Iterable[MetricDescriptor],
        candidate_input_query_nodes: OrderedSet[MetricQueryNode],
        descriptor_to_level: Mapping[MetricDescriptor, int],
    ) -> Optional[Mapping[MetricDescriptor, MetricQueryNode]]:
        if len(candidate_input_query_nodes) == 0:
            return None

        remaining_input_descriptors = MutableOrderedSet(
            sorted(input_descriptors, key=lambda descriptor: descriptor_to_level[descriptor], reverse=True)
        )
        input_metric_descriptor_to_query_node: dict[MetricDescriptor, MetricQueryNode] = {}

        while remaining_input_descriptors:
            input_metric_descriptor = mf_first_item(remaining_input_descriptors)
            input_query_nodes_with_input_descriptor = tuple(
                input_query_node
                for input_query_node in candidate_input_query_nodes
                if input_metric_descriptor in input_query_node.output_metric_descriptors
            )
            match_count = len(input_query_nodes_with_input_descriptor)
            if match_count == 0:
                raise RuntimeError(
                    LazyFormat(
                        "Unable to find a candidate query with the input descriptor. There may be an error"
                        " constructing the metric dependency graph",
                        input_descriptor=input_metric_descriptor,
                        candidate_input_queries=candidate_input_query_nodes,
                    )
                )
            elif match_count != 1:
                raise RuntimeError(
                    LazyFormat(
                        "There should have been only one candidate query that computes the target descriptor",
                        v=input_metric_descriptor,
                        input_queries_with_input_descriptor=input_query_nodes_with_input_descriptor,
                    )
                )
            selected_input_query_node = input_query_nodes_with_input_descriptor[0]

            selected_input_metric_descriptors = remaining_input_descriptors.intersection(
                selected_input_query_node.output_metric_descriptors
            )

            for input_metric_descriptor in selected_input_metric_descriptors:
                input_metric_descriptor_to_query_node[input_metric_descriptor] = selected_input_query_node

            remaining_input_descriptors.difference_update(selected_input_metric_descriptors)

        return input_metric_descriptor_to_query_node

    def _group_simple_metric_descriptors_by_model_id(
        self, metric_descriptors: Iterable[MetricDescriptor]
    ) -> Mapping[SemanticModelId, OrderedSet[MetricDescriptor]]:
        model_id_to_descriptors: defaultdict[SemanticModelId, MutableOrderedSet[MetricDescriptor]] = defaultdict(
            MutableOrderedSet
        )

        for metric_descriptor in metric_descriptors:
            metric = self._manifest_object_lookup.get_metric(metric_descriptor.metric_name)
            metric_type = metric.type

            assert metric_type is MetricType.SIMPLE, LazyFormat("Provided metric is not a simple metric", metric=metric)
            assert metric.type_params.metric_aggregation_params is not None, LazyFormat(
                "A simple metric should have `metric_aggregation_params`", metric=metric
            )
            model_id = SemanticModelId.get_instance(metric.type_params.metric_aggregation_params.semantic_model)
            model_id_to_descriptors[model_id].add(metric_descriptor)

        return model_id_to_descriptors

    def _recursively_collect_descriptors(
        self,
        metric_descriptor: MetricDescriptor,
        descriptor_collector: MetricDescriptorCollector,
    ) -> None:
        if metric_descriptor in descriptor_collector.descriptors:
            return

        metric_name = metric_descriptor.metric_name
        metric = self._manifest_object_lookup.get_metric(metric_name)
        metric_type = metric.type

        if metric_type is MetricType.SIMPLE:
            descriptor_collector.add_descriptor(metric_descriptor, input_descriptors=None)
        elif (
            metric_type is MetricType.RATIO
            or metric_type is MetricType.CUMULATIVE
            or metric_type is MetricType.CONVERSION
            or metric_type is MetricType.DERIVED
            or metric_type is MetricType.RATIO
        ):
            input_descriptors: list[MetricDescriptor] = []
            for input_metric in metric.input_metrics:
                if metric.filter is None:
                    additional_filters = None
                else:
                    additional_filters = (
                        where_filter.where_sql_template for where_filter in metric.filter.where_filters
                    )
                input_descriptor = MetricDescriptor.create_from_input_metric(
                    input_metric, additional_filters=additional_filters
                )
                input_descriptors.append(input_descriptor)
                self._recursively_collect_descriptors(
                    metric_descriptor=input_descriptor, descriptor_collector=descriptor_collector
                )
            descriptor_collector.add_descriptor(metric_descriptor, input_descriptors=input_descriptors)
        else:
            assert_values_exhausted(metric_type)

    def _group_descriptors_by_level(
        self, descriptor_collector: MetricDescriptorCollector
    ) -> Mapping[int, OrderedSet[MetricDescriptor]]:
        level_to_descriptors: defaultdict[int, MutableOrderedSet[MetricDescriptor]] = defaultdict(MutableOrderedSet)

        for metric_descriptor in descriptor_collector.descriptors:
            level_to_descriptors[self._level_resolver.resolve_evaluation_level(metric_descriptor.metric_name)].add(
                metric_descriptor
            )

        sorted_level_to_descriptors: dict[int, FrozenOrderedSet[MetricDescriptor]] = {}
        for level in sorted(level_to_descriptors):
            sorted_level_to_descriptors[level] = FrozenOrderedSet(sorted(level_to_descriptors[level]))

        return sorted_level_to_descriptors
