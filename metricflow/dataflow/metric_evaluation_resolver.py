from __future__ import annotations

import logging
from collections import defaultdict
from collections.abc import Iterable, Sequence
from typing import Optional

from dbt_semantic_interfaces.enum_extension import assert_values_exhausted
from dbt_semantic_interfaces.protocols import MetricInput
from dbt_semantic_interfaces.type_enums import MetricType
from metricflow_semantics.semantic_graph.lookups.manifest_object_lookup import ManifestObjectLookup
from metricflow_semantics.toolkit.cache.result_cache import ResultCache
from metricflow_semantics.toolkit.collections.ordered_set import FrozenOrderedSet, MutableOrderedSet, OrderedSet
from metricflow_semantics.toolkit.dataclass_helpers import fast_frozen_dataclass
from metricflow_semantics.toolkit.mf_type_aliases import AnyLengthTuple

logger = logging.getLogger(__name__)


@fast_frozen_dataclass()
class MetricDependency:
    metric_name: str
    parent_metric_names: FrozenOrderedSet[str]
    evaluation_order: int


@fast_frozen_dataclass()
class MetricDescriptor:
    metric_name: str
    evaluation_level: int
    where_filters: AnyLengthTuple[str]
    alias: Optional[str]
    offset_window_count: Optional[int]
    offset_window_grain: Optional[str]
    offset_to_grain: Optional[str]

    @staticmethod
    def create(
        metric_name: str,
        evaluation_level: int,
        where_filters: Iterable[str],
        alias: Optional[str] = None,
        offset_window_count: Optional[int] = None,
        offset_window_grain: Optional[str] = None,
        offset_to_grain: Optional[str] = None,
    ) -> MetricDescriptor:
        return MetricDescriptor(
            metric_name=metric_name,
            evaluation_level=evaluation_level,
            where_filters=tuple(where_filters),
            alias=alias,
            offset_window_count=offset_window_count,
            offset_window_grain=offset_window_grain,
            offset_to_grain=offset_to_grain,
        )

    @staticmethod
    def create_from_metric_input(metric_input: MetricInput, evaluation_level: int) -> MetricDescriptor:
        return MetricDescriptor(
            metric_name=metric_input.name,
            evaluation_level=evaluation_level,
            alias=metric_input.alias,
            where_filters=tuple(
                where_filter.where_sql_template
                for where_filter in (metric_input.filter.where_filters if metric_input.filter is not None else ())
            ),
            offset_window_grain=metric_input.offset_window.granularity
            if metric_input.offset_window is not None
            else None,
            offset_window_count=metric_input.offset_window.count if metric_input.offset_window is not None else None,
            offset_to_grain=metric_input.offset_to_grain,
        )


@fast_frozen_dataclass()
class MetricEvaluation:
    metric_descriptor: MetricDescriptor
    referenced_levels: FrozenOrderedSet[int]


class MetricEvaluationResolver:
    def __init__(self, manifest_object_lookup: ManifestObjectLookup) -> None:
        self._manifest_object_lookup = manifest_object_lookup
        self._metric_evaluation_level_cache: ResultCache[str, int] = ResultCache()

    def _resolve_evaluation_level_for_metric(self, metric_name: str) -> int:
        cache_key = metric_name
        result = self._metric_evaluation_level_cache.get(cache_key)
        if result:
            return result.value

        metric = self._manifest_object_lookup.get_metric(metric_name)
        metric_type = metric.type

        if metric_type is MetricType.SIMPLE:
            evaluation_level = 0
        elif (
            metric_type is MetricType.RATIO
            or metric_type is MetricType.CUMULATIVE
            or metric_type is MetricType.CONVERSION
            or metric_type is MetricType.DERIVED
            or metric_type is MetricType.RATIO
        ):
            input_metric_names = FrozenOrderedSet(metric.name for metric in metric.input_metrics)
            evaluation_level = (
                max(
                    self._resolve_evaluation_level_for_metric(input_metric_name)
                    for input_metric_name in input_metric_names
                )
                + 1
            )
        else:
            assert_values_exhausted(metric_type)

        return self._metric_evaluation_level_cache.set_and_get(cache_key, evaluation_level)

    def _resolve_evaluation_level_for_metrics(self, metric_names: OrderedSet[str]) -> int:
        return max(self._resolve_evaluation_level_for_metric(metric_name) for metric_name in metric_names)

    def resolve_evaluation_for_query(
        self, metric_names: Sequence[str], where_filters: Sequence[str]
    ) -> Sequence[MetricEvaluation]:
        query_evaluation_level = (
            max(self._resolve_evaluation_level_for_metric(metric_name) for metric_name in metric_names) + 1
        )
        evaluations: list[MetricEvaluation] = []
        for metric_name in metric_names:
            evaluations.extend(
                self._resolve_evaluation(
                    metric_descriptor=MetricDescriptor.create(
                        metric_name=metric_name,
                        evaluation_level=self._resolve_evaluation_level_for_metric(metric_name),
                        where_filters=where_filters,
                    ),
                    referenced_level=query_evaluation_level,
                )
            )
        descriptor_to_referenced_levels: defaultdict[MetricDescriptor, MutableOrderedSet[int]] = defaultdict(
            MutableOrderedSet
        )

        for evaluation in evaluations:
            descriptor_to_referenced_levels[evaluation.metric_descriptor].update(evaluation.referenced_levels)

        merged_evaluations: list[MetricEvaluation] = []
        for descriptor, referenced_levels in descriptor_to_referenced_levels.items():
            merged_evaluations.append(
                MetricEvaluation(
                    metric_descriptor=descriptor,
                    referenced_levels=FrozenOrderedSet(referenced_levels),
                )
            )

        return merged_evaluations

    def _resolve_evaluation(
        self,
        metric_descriptor: MetricDescriptor,
        referenced_level: int,
    ) -> Sequence[MetricEvaluation]:
        metric_name = metric_descriptor.metric_name
        metric = self._manifest_object_lookup.get_metric(metric_name)

        metric_type = metric.type
        results = []
        if metric_type is MetricType.SIMPLE:
            results.append(
                MetricEvaluation(
                    metric_descriptor=metric_descriptor,
                    referenced_levels=FrozenOrderedSet((referenced_level,)),
                )
            )
        elif (
            metric_type is MetricType.CUMULATIVE
            or metric_type is MetricType.CONVERSION
            or metric_type is MetricType.RATIO
            or metric_type is MetricType.DERIVED
        ):
            for metric_input in metric.input_metrics:
                results.extend(
                    self._resolve_evaluation(
                        metric_descriptor=MetricDescriptor.create_from_metric_input(
                            metric_input=metric_input,
                            evaluation_level=self._resolve_evaluation_level_for_metric(metric_input.name),
                        ),
                        referenced_level=metric_descriptor.evaluation_level,
                    )
                )
            results.append(
                MetricEvaluation(
                    metric_descriptor=metric_descriptor,
                    referenced_levels=FrozenOrderedSet((referenced_level,)),
                )
            )

        else:
            assert_values_exhausted(metric_type)

        return results
