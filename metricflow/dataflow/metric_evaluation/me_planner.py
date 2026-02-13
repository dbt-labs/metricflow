from __future__ import annotations

import logging
from abc import ABC, abstractmethod
from collections import defaultdict
from collections.abc import Iterable, Mapping, Sequence, Set
from functools import cached_property
from typing import Optional

from dbt_semantic_interfaces.enum_extension import assert_values_exhausted
from dbt_semantic_interfaces.protocols import MetricInput
from dbt_semantic_interfaces.type_enums import MetricType, TimeGranularity
from metricflow_semantics.semantic_graph.lookups.manifest_object_lookup import ManifestObjectLookup
from metricflow_semantics.semantic_graph.model_id import SemanticModelId
from metricflow_semantics.specs.metric_spec import MetricSpec
from metricflow_semantics.toolkit.cache.result_cache import ResultCache
from metricflow_semantics.toolkit.collections.ordered_set import FrozenOrderedSet, MutableOrderedSet, OrderedSet
from metricflow_semantics.toolkit.dataclass_helpers import fast_frozen_dataclass
from metricflow_semantics.toolkit.mf_logging.lazy_formattable import LazyFormat
from metricflow_semantics.toolkit.mf_logging.pretty_formattable import MetricFlowPrettyFormattable
from metricflow_semantics.toolkit.mf_logging.pretty_formatter import PrettyFormatContext
from metricflow_semantics.toolkit.mf_type_aliases import MappingItemsTuple
from metricflow_semantics.toolkit.syntactic_sugar import mf_first_item
from typing_extensions import override

from metricflow.dataflow.metric_evaluation.me_dag import (
    MetricEvaluationPlan,
    MetricSubqueryNode,
)
from metricflow.dataflow.metric_evaluation.me_level_resolver import MetricEvaluationLevelResolver

logger = logging.getLogger(__name__)


class MetricEvaluationPlanner(ABC):
    @abstractmethod
    def build_plan(self, metric_specs: Sequence[MetricSpec]) -> MetricEvaluationPlan:
        raise NotImplementedError


@fast_frozen_dataclass()
class TimeWindow:
    count: int
    granularity: str


@fast_frozen_dataclass()
class MetricDescriptor:
    """class MetricSpec(InstanceSpec):  # noqa: D101
    # Time-over-time could go here
    element_name: str
    filter_spec_set: WhereFilterSpecSet = WhereFilterSpecSet()
    alias: Optional[str] = None
    offset_window: Optional[PydanticMetricTimeWindow] = None
    offset_to_grain: Optional[TimeGranularity] = None
    """

    metric_name: str
    filters: FrozenOrderedSet[str]
    alias: Optional[str]
    offset_window: Optional[TimeWindow]
    offset_to_grain: Optional[TimeGranularity]

    @staticmethod
    def create(
        metric_name: str,
        filters: Optional[Iterable[str]] = None,
        alias: Optional[str] = None,
        offset_window: Optional[TimeWindow] = None,
        offset_to_grain: Optional[TimeGranularity] = None,
    ) -> MetricDescriptor:
        return MetricDescriptor(
            metric_name=metric_name,
            filters=FrozenOrderedSet(filters),
            alias=alias,
            offset_window=offset_window,
            offset_to_grain=offset_to_grain,
        )

    @cached_property
    def allows_passed_metrics(self) -> bool:
        return (
            len(self.filters) == 0
            and self.alias is None
            and self.offset_window is None
            and self.offset_to_grain is None
        )

    @cached_property
    def has_time_offset(self) -> bool:  # noqa: D102
        return self.offset_window is not None or self.offset_to_grain is not None

    @staticmethod
    def create_from_spec(metric_spec: MetricSpec) -> MetricDescriptor:
        return MetricDescriptor(
            metric_name=metric_spec.element_name,
            filters=FrozenOrderedSet(
                filter_spec.where_sql for filter_spec in metric_spec.filter_spec_set.all_filter_specs
            ),
            alias=metric_spec.alias,
            offset_window=TimeWindow(
                count=metric_spec.offset_window.count, granularity=metric_spec.offset_window.granularity
            )
            if metric_spec.offset_window is not None
            else None,
            offset_to_grain=metric_spec.offset_to_grain,
        )

    @staticmethod
    def create_from_input_metric(
        metric_input: MetricInput, additional_filters: Optional[Iterable[str]] = None
    ) -> MetricDescriptor:
        filters: list[str] = []
        if metric_input.filter is not None:
            filters.extend(where_filter.where_sql_template for where_filter in metric_input.filter.where_filters)
        if additional_filters is not None:
            filters.extend(additional_filters)
        return MetricDescriptor(
            metric_name=metric_input.name,
            filters=FrozenOrderedSet(filters),
            alias=metric_input.alias,
            offset_window=TimeWindow(
                count=metric_input.offset_window.count, granularity=metric_input.offset_window.granularity
            )
            if metric_input.offset_window is not None
            else None,
            offset_to_grain=TimeGranularity(metric_input.offset_to_grain)
            if metric_input.offset_to_grain is not None
            else None,
        )


@fast_frozen_dataclass()
class MetricDescriptorSet:
    computed_metric_descriptors: FrozenOrderedSet[MetricDescriptor]
    passthrough_metric_descriptors: FrozenOrderedSet[MetricDescriptor]

    @staticmethod
    def create(
        computed_metric_descriptors: Iterable[MetricDescriptor],
        passthrough_metric_descriptors: Iterable[MetricDescriptor],
    ) -> MetricDescriptorSet:
        return MetricDescriptorSet(
            computed_metric_descriptors=FrozenOrderedSet(computed_metric_descriptors),
            passthrough_metric_descriptors=FrozenOrderedSet(passthrough_metric_descriptors),
        )


@fast_frozen_dataclass()
class MetricQuery(ABC):
    computed_metric_descriptors: FrozenOrderedSet[MetricDescriptor]
    passthrough_metric_descriptors: FrozenOrderedSet[MetricDescriptor]

    @cached_property
    def all_descriptors(self) -> OrderedSet[MetricDescriptor]:
        return self.computed_metric_descriptors.union(self.passthrough_metric_descriptors)

    @abstractmethod
    def pruned_query(self, required_metric_descriptors: Set[MetricDescriptor]) -> Optional[MetricQuery]:
        raise NotImplementedError


@fast_frozen_dataclass()
class BaseMetricQuery(MetricQuery):
    model_id: SemanticModelId

    @staticmethod
    def create(
        model_id: SemanticModelId,
        computed_metric_descriptors: Optional[Iterable[MetricDescriptor]] = None,
    ) -> BaseMetricQuery:
        return BaseMetricQuery(
            computed_metric_descriptors=FrozenOrderedSet(computed_metric_descriptors),
            passthrough_metric_descriptors=FrozenOrderedSet(),
            model_id=model_id,
        )

    @override
    def pruned_query(self, required_metric_descriptors: Set[MetricDescriptor]) -> Optional[MetricQuery]:
        filtered_computed_metric_descriptors = tuple(
            computed_metric_descriptor for computed_metric_descriptor in self.computed_metric_descriptors
            if computed_metric_descriptor in required_metric_descriptors
        )





@fast_frozen_dataclass()
class RecursiveMetricQuery(MetricQuery, MetricFlowPrettyFormattable):
    _input_query_and_required_metric_descriptors_items: MappingItemsTuple[
        MetricQuery, FrozenOrderedSet[MetricDescriptor]
    ]

    @staticmethod
    def create(
        computed_metric_descriptors: Iterable[MetricDescriptor],
        passthrough_metric_descriptors: Iterable[MetricDescriptor],
        input_query_to_required_descriptors: Mapping[MetricQuery, OrderedSet[MetricDescriptor]],
    ) -> RecursiveMetricQuery:
        return RecursiveMetricQuery(
            computed_metric_descriptors=FrozenOrderedSet(computed_metric_descriptors),
            passthrough_metric_descriptors=FrozenOrderedSet(passthrough_metric_descriptors),
            _input_query_and_required_metric_descriptors_items=tuple(
                (input_query, FrozenOrderedSet(passthrough_descriptors))
                for input_query, passthrough_descriptors in input_query_to_required_descriptors.items()
            ),
        )

    @cached_property
    def input_query_to_required_descriptors(self) -> Mapping[MetricQuery, FrozenOrderedSet[MetricDescriptor]]:
        return {
            input_query: passthrough_descriptor
            for input_query, passthrough_descriptor in self._input_query_and_required_metric_descriptors_items
        }

    @override
    def pretty_format(self, format_context: PrettyFormatContext) -> Optional[str]:
        return format_context.formatter.pretty_format_object_by_parts(
            class_name=self.__class__.__name__,
            field_mapping={
                "computed_metric_descriptors": self.computed_metric_descriptors,
                "passthrough_metric_descriptors": self.passthrough_metric_descriptors,
                "input_query_to_required_descriptors": self.input_query_to_required_descriptors,
            },
        )

    # _descriptor_to_query_mapping_items: MappingItemsTuple[MetricDescriptor, MetricQuery]
    #
    # @staticmethod
    # def create(descriptor_to_query: Mapping[MetricDescriptor, MetricQuery]) -> RecursiveMetricQuery:
    #     return RecursiveMetricQuery(_descriptor_to_query_mapping_items=tuple(descriptor_to_query.items()))
    #
    # @cached_property
    # def descriptor_to_query(self) -> Mapping[MetricDescriptor, MetricQuery]:
    #     return {
    #         descriptor: metric_query for descriptor, metric_query in self._descriptor_to_query_mapping_items
    #     }


class MetricQueryGroup:
    def __init__(self, metric_queries: Iterable[MetricQuery]) -> None:
        self._metric_queries: MutableOrderedSet[MetricQuery] = MutableOrderedSet()
        self._metric_name_to_metric_queries: defaultdict[str, MutableOrderedSet[MetricQuery]] = defaultdict(
            MutableOrderedSet
        )

        for metric_query in metric_queries:
            self.add_metric_query(metric_query)

    def add_metric_query(self, metric_query: MetricQuery) -> None:
        self._metric_queries.add(metric_query)
        for computed_metric_descriptor in metric_query.computed_metric_descriptors:
            self._metric_name_to_metric_queries[computed_metric_descriptor.metric_name].add(metric_query)
        for passed_metric_descriptor in metric_query.passthrough_metric_descriptors:
            self._metric_name_to_metric_queries[passed_metric_descriptor.metric_name].add(metric_query)


class MetricDescriptorCollector:
    def __init__(self) -> None:
        self._descriptor_to_input_descriptors: dict[MetricDescriptor, FrozenOrderedSet[MetricDescriptor]] = {}

    def add_descriptor(
        self, metric_descriptor: MetricDescriptor, input_descriptors: Optional[Iterable[MetricDescriptor]]
    ) -> None:
        if metric_descriptor in self._descriptor_to_input_descriptors:
            raise RuntimeError(LazyFormat("Descriptor already added", metric_descriptor=metric_descriptor))
        self._descriptor_to_input_descriptors[metric_descriptor] = (
            FrozenOrderedSet(input_descriptors) if input_descriptors is not None else FrozenOrderedSet()
        )

    @property
    def descriptors(self) -> Set[MetricDescriptor]:
        return self._descriptor_to_input_descriptors.keys()

    def get_input_descriptors(self, metric_descriptor: MetricDescriptor) -> OrderedSet[MetricDescriptor]:
        result = self._descriptor_to_input_descriptors.get(metric_descriptor)
        if result is None:
            raise ValueError(
                LazyFormat(
                    "Unknown metric descriptor",
                    metric_descriptor=metric_descriptor,
                    known_descriptors=self._descriptor_to_input_descriptors.keys(),
                )
            )
        return result

    # def contains_descriptor(self, metric_descriptor: MetricDescriptor) -> bool:
    #     return metric_descriptor in self._descriptor_to_input_descriptors

    @property
    def descriptor_to_input_descriptors(self) -> Mapping[MetricDescriptor, OrderedSet[MetricDescriptor]]:
        return self._descriptor_to_input_descriptors


class JoinCountOptimizedMetricEvaluationPlanner:
    def __init__(
        self,
        manifest_object_lookup: ManifestObjectLookup,
    ) -> None:
        self._manifest_object_lookup = manifest_object_lookup
        self._node_cache: ResultCache[MetricSpec, MetricSubqueryNode] = ResultCache()
        self._level_resolver: MetricEvaluationLevelResolver = MetricEvaluationLevelResolver(manifest_object_lookup)

    def build_plan(self, metric_specs: Iterable[MetricSpec]) -> None:
        # Figure out all forms of metrics that are required.
        descriptor_collector = MetricDescriptorCollector()
        top_level_query_metric_descriptors = tuple(
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

        base_metric_queries = self._generate_queries_for_base_metrics(base_metric_descriptors)

        logger.debug(
            LazyFormat(
                "Generated queries for base metrics",
                base_metric_descriptors=base_metric_descriptors,
                base_metric_queries=base_metric_queries,
            )
        )

        candidate_queries = base_metric_queries
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

        top_level_descriptor_to_input_query = self._find_best_input_queries(
            input_descriptors=top_level_query_metric_descriptors,
            candidate_queries=candidate_queries,
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
        return None

    def _prune_query(self, metric_query: MetricQuery, required_metrics: OrderedSet[MetricDescriptor]) -> MetricQuery:
        raise NotImplementedError

    def _generate_queries_for_base_metrics(
        self, metric_descriptors: OrderedSet[MetricDescriptor]
    ) -> OrderedSet[MetricQuery]:
        model_id_to_descriptors = self._group_simple_metric_descriptors_by_model_id(metric_descriptors)

        metric_queries: MutableOrderedSet[BaseMetricQuery] = MutableOrderedSet()
        for model_id, descriptors in model_id_to_descriptors.items():
            metric_queries.add(BaseMetricQuery.create(model_id=model_id, computed_metric_descriptors=descriptors))

        return metric_queries

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
            input_descriptor_to_query = self._find_best_input_queries(
                input_descriptors=input_descriptors,
                candidate_queries=candidate_queries,
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
                        for descriptor in query.all_descriptors
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

    def _find_best_input_queries(
        self,
        input_descriptors: Iterable[MetricDescriptor],
        candidate_queries: OrderedSet[MetricQuery],
        descriptor_to_level: Mapping[MetricDescriptor, int],
    ) -> Optional[Mapping[MetricDescriptor, MetricQuery]]:
        if len(candidate_queries) == 0:
            return None

        remaining_input_descriptors = MutableOrderedSet(
            sorted(input_descriptors, key=lambda descriptor: descriptor_to_level[descriptor], reverse=True)
        )
        input_descriptor_to_candidate_query: dict[MetricDescriptor, MetricQuery] = {}

        while remaining_input_descriptors:
            input_descriptor = mf_first_item(remaining_input_descriptors)
            queries_with_input_descriptor = tuple(
                query for query in candidate_queries if input_descriptor in query.computed_metric_descriptors
            )
            match_count = len(queries_with_input_descriptor)
            if match_count == 0:
                raise RuntimeError(
                    LazyFormat(
                        "Unable to find a candidate query with the input descriptor. There may be an error"
                        " constructing the metric dependency graph",
                        input_descriptor=input_descriptor,
                        candidate_queries=candidate_queries,
                    )
                )
            elif match_count != 1:
                raise RuntimeError(
                    LazyFormat(
                        "There should have been only one candidate query that computes the target descriptor",
                        v=input_descriptor,
                        queries_with_input_descriptor=queries_with_input_descriptor,
                    )
                )
            input_query = queries_with_input_descriptor[0]

            resolved_input_descriptors = remaining_input_descriptors.intersection(input_query.all_descriptors)

            for metric_descriptor in resolved_input_descriptors:
                input_descriptor_to_candidate_query[metric_descriptor] = input_query

            remaining_input_descriptors.difference_update(resolved_input_descriptors)

        return input_descriptor_to_candidate_query

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
