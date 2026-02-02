from __future__ import annotations

import itertools
import logging
from abc import ABC, abstractmethod
from collections import defaultdict
from collections.abc import Iterable, Sequence, Set
from typing import Optional, TypeVar

from dbt_semantic_interfaces.enum_extension import assert_values_exhausted
from dbt_semantic_interfaces.protocols import MetricInput
from dbt_semantic_interfaces.type_enums import MetricType
from metricflow_semantics.semantic_graph.lookups.manifest_object_lookup import ManifestObjectLookup
from metricflow_semantics.specs.metric_spec import MetricSpec
from metricflow_semantics.toolkit.cache.result_cache import ResultCache
from metricflow_semantics.toolkit.collections.ordered_set import FrozenOrderedSet, MutableOrderedSet, OrderedSet
from metricflow_semantics.toolkit.dataclass_helpers import fast_frozen_dataclass
from metricflow_semantics.toolkit.mf_logging.lazy_formattable import LazyFormat
from metricflow_semantics.toolkit.mf_logging.pretty_formattable import MetricFlowPrettyFormattable
from metricflow_semantics.toolkit.mf_logging.pretty_formatter import PrettyFormatContext
from metricflow_semantics.toolkit.mf_type_aliases import AnyLengthTuple
from typing_extensions import override

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


@fast_frozen_dataclass()
class ExtendedMetricSpec:
    metric_spec: MetricSpec
    evaluation_level: int
    consumer_levels: FrozenOrderedSet[int]


class MetricEvaluationLookup(ABC):
    @property
    @abstractmethod
    def evaluation_levels(self) -> Sequence[int]:
        raise NotImplementedError

    @abstractmethod
    def get_input_metric_specs(self, metric_spec: MetricSpec) -> Sequence[MetricSpec]:
        raise NotImplementedError

    @abstractmethod
    def get_computed_metric_specs(self, evaluation_level: int) -> OrderedSet[MetricSpec]:
        raise NotImplementedError

    @abstractmethod
    def get_consumer_levels(self, metric_spec: MetricSpec) -> OrderedSet[int]:
        raise NotImplementedError


class MetricEvaluationLevelResolver:
    def __init__(self, manifest_object_lookup: ManifestObjectLookup) -> None:
        self._metric_evaluation_level_cache: ResultCache[str, int] = ResultCache()
        self._manifest_object_lookup = manifest_object_lookup

    def resolve_evaluation_level(self, metric_name: str) -> int:
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
                max(self.resolve_evaluation_level(input_metric_name) for input_metric_name in input_metric_names) + 1
            )
        else:
            assert_values_exhausted(metric_type)

        return self._metric_evaluation_level_cache.set_and_get(cache_key, evaluation_level)


class MutableMetricEvaluationLookup(MetricEvaluationLookup, MetricFlowPrettyFormattable):
    # _metric_spec_to_input_metric_specs: dict[MetricSpec, MutableOrderedSet[MetricSpec]] = dataclasses.field(
    #   default_factory=lambda: defaultdict(MutableOrderedSet)
    # )
    # _metric_name_to_evaluation_level: dict[str, int] = dataclasses.field(default_factory=dict)
    # _metric_spec_to_consumer_evaluation_levels: dict[MetricSpec, MutableOrderedSet[int]] = dataclasses.field(
    #   default_factory=lambda: defaultdict(MutableOrderedSet)
    # )
    # _evaluation_level_to_computed_metric_specs: defaultdict[int, MutableOrderedSet[MetricSpec]] = dataclasses.field(
    #   default_factory=lambda: defaultdict(MutableOrderedSet)
    # )

    def __init__(self, evaluation_level_resolver: MetricEvaluationLevelResolver) -> None:
        # self._metric_spec_to_input_metric_specs: dict[MetricSpec, MutableOrderedSet[MetricSpec]] = defaultdict(
        #     MutableOrderedSet
        # )
        self._metric_name_to_evaluation_level: dict[str, int] = {}
        self._metric_spec_to_consumer_evaluation_levels: defaultdict[MetricSpec, MutableOrderedSet[int]] = defaultdict(
            MutableOrderedSet
        )
        self._evaluation_level_to_computed_metric_specs: defaultdict[int, MutableOrderedSet[MetricSpec]] = defaultdict(
            MutableOrderedSet
        )
        self._consumer_evaluation_level_to_metric_specs: defaultdict[int, MutableOrderedSet[MetricSpec]] = defaultdict(
            MutableOrderedSet
        )
        self._evaluation_level_resolver = evaluation_level_resolver
        self._metric_spec_to_input_metric_specs: dict[MetricSpec, AnyLengthTuple[MetricSpec]] = {}

    def add_metric_spec(
        self, metric_spec: MetricSpec, consumer_evaluation_level: int, input_metric_specs: Sequence[MetricSpec]
    ) -> None:
        metric_name = metric_spec.element_name
        evaluation_level = self._evaluation_level_resolver.resolve_evaluation_level(metric_name)
        logger.debug(
            LazyFormat(
                "Adding metric spec",
                metric_spec=metric_spec,
                evaluation_level=evaluation_level,
                consumer_evaluation_level=consumer_evaluation_level,
            )
        )
        existing_evaluation_level = self._metric_name_to_evaluation_level.get(metric_name)
        if existing_evaluation_level is not None and existing_evaluation_level != evaluation_level:
            raise RuntimeError(
                LazyFormat(
                    "Metric was already added with a different evaluation level",
                    metric_name=metric_name,
                    evaluation_level=evaluation_level,
                    existing_evaluation_level=existing_evaluation_level,
                )
            )
        self._metric_name_to_evaluation_level[metric_name] = evaluation_level
        self._metric_spec_to_consumer_evaluation_levels[metric_spec].add(consumer_evaluation_level)
        self._evaluation_level_to_computed_metric_specs[evaluation_level].add(metric_spec)
        self._consumer_evaluation_level_to_metric_specs[consumer_evaluation_level].add(metric_spec)
        self._metric_spec_to_input_metric_specs[metric_spec] = tuple(input_metric_specs)

    @property
    @override
    def evaluation_levels(self) -> Sequence[int]:
        return sorted(self._evaluation_level_to_computed_metric_specs)

    @override
    def get_input_metric_specs(self, metric_spec: MetricSpec) -> Sequence[MetricSpec]:
        input_metric_specs = self._metric_spec_to_input_metric_specs.get(metric_spec)
        if input_metric_specs is None:
            raise RuntimeError(
                LazyFormat(
                    "Unknown metric spec",
                    metric_spec=metric_spec,
                    known_metric_specs=list(self._metric_spec_to_input_metric_specs),
                )
            )
        return input_metric_specs

    @override
    def get_computed_metric_specs(self, evaluation_level: int) -> OrderedSet[MetricSpec]:
        metric_specs = self._evaluation_level_to_computed_metric_specs.get(evaluation_level)
        if metric_specs is None:
            raise RuntimeError(
                LazyFormat(
                    "Unknown evaluation level",
                    evaluation_level=evaluation_level,
                    known_evaluation_levels=sorted(self._evaluation_level_to_computed_metric_specs),
                )
            )
        return metric_specs

    @override
    def get_consumer_levels(self, metric_spec: MetricSpec) -> OrderedSet[int]:
        consumer_levels = self._metric_spec_to_consumer_evaluation_levels.get(metric_spec)
        if consumer_levels is None:
            raise RuntimeError(
                LazyFormat(
                    "Unknown metric spec",
                    metric_spec=metric_spec,
                    known_metric_specs=list(self._metric_spec_to_consumer_evaluation_levels),
                )
            )
        return consumer_levels

    @override
    def pretty_format(self, format_context: PrettyFormatContext) -> Optional[str]:
        return format_context.formatter.pretty_format_object_by_parts(
            class_name=self.__class__.__name__,
            field_mapping={
                "level_to_computed_metrics": {
                    evaluation_level: self._evaluation_level_to_computed_metric_specs[evaluation_level]
                    for evaluation_level in sorted(self._evaluation_level_to_computed_metric_specs)
                },
                "level_to_consumed_metrics": {
                    evaluation_level: self._consumer_evaluation_level_to_metric_specs[evaluation_level]
                    for evaluation_level in sorted(self._consumer_evaluation_level_to_metric_specs)
                },
                "metric_to_consumer_levels": self._metric_spec_to_consumer_evaluation_levels,
                "metric_to_input_metrics": self._metric_spec_to_input_metric_specs,
            },
        )


class MetricEvaluationResolver:
    def __init__(self, manifest_object_lookup: ManifestObjectLookup) -> None:
        self._metric_evaluation_level_cache: ResultCache[str, int] = ResultCache()
        self._manifest_object_lookup = manifest_object_lookup
        self._evaluation_level_resolver = MetricEvaluationLevelResolver(manifest_object_lookup)

    def resolve_evaluation_lookup_for_query(
        self,
        metric_specs: Sequence[MetricSpec],
    ) -> MetricEvaluationLookup:
        query_evaluation_level = (
            max(
                self._evaluation_level_resolver.resolve_evaluation_level(metric_spec.element_name)
                for metric_spec in metric_specs
            )
            + 1
        )
        evaluation_lookup = MutableMetricEvaluationLookup(self._evaluation_level_resolver)

        for metric_spec in metric_specs:
            if len(metric_spec.filter_spec_set) > 0:
                raise NotImplementedError(LazyFormat("Metric filters not supported"))
            self._traverse_metric_definition_dag(
                metric_spec, consumer_evaluation_level=query_evaluation_level, evaluation_lookup=evaluation_lookup
            )

        return evaluation_lookup

    def _traverse_metric_definition_dag(
        self, metric_spec: MetricSpec, consumer_evaluation_level: int, evaluation_lookup: MutableMetricEvaluationLookup
    ) -> None:
        metric_name = metric_spec.element_name
        metric = self._manifest_object_lookup.get_metric(metric_name)
        metric_type = metric.type
        evaluation_level_for_current_metric = self._evaluation_level_resolver.resolve_evaluation_level(metric_name)
        if metric_type is MetricType.SIMPLE:
            evaluation_lookup.add_metric_spec(
                metric_spec,
                consumer_evaluation_level=consumer_evaluation_level,
                input_metric_specs=(),
            )
        elif (
            metric_type is MetricType.CUMULATIVE
            or metric_type is MetricType.CONVERSION
            or metric_type is MetricType.RATIO
            or metric_type is MetricType.DERIVED
        ):
            input_metric_specs = []
            for metric_input in metric.input_metrics:
                if metric_input.filter is not None:
                    raise NotImplementedError(
                        LazyFormat(
                            "Metric filters not yet supported", metric_spec=metric_spec, metric_input=metric_input
                        )
                    )
                input_metric_spec = MetricSpec(element_name=metric_input.name)
                input_metric_specs.append(input_metric_spec)
                self._traverse_metric_definition_dag(
                    metric_spec=input_metric_spec,
                    consumer_evaluation_level=evaluation_level_for_current_metric,
                    evaluation_lookup=evaluation_lookup,
                )

            evaluation_lookup.add_metric_spec(
                metric_spec=metric_spec,
                consumer_evaluation_level=consumer_evaluation_level,
                input_metric_specs=input_metric_specs,
            )

        else:
            assert_values_exhausted(metric_type)


@fast_frozen_dataclass()
class MetricGroup:
    parent_metric_groups: FrozenOrderedSet[MetricGroup]
    passed_metric_specs: FrozenOrderedSet[MetricSpec]
    computed_metric_specs: FrozenOrderedSet[MetricSpec]

    @staticmethod
    def create(
        parent_metric_groups: Iterable[MetricGroup],
        passed_metric_specs: Iterable[MetricSpec],
        computed_metric_specs: Iterable[MetricSpec],
    ) -> MetricGroup:
        return MetricGroup(
            parent_metric_groups=FrozenOrderedSet(parent_metric_groups),
            computed_metric_specs=FrozenOrderedSet(computed_metric_specs),
            passed_metric_specs=FrozenOrderedSet(passed_metric_specs),
        )


class MetricEvaluationCookbook:
    def __init__(self, evaluation_lookup: MetricEvaluationLookup) -> None:
        self._evaluation_lookup = evaluation_lookup

    def get_recipe(self) -> None:
        available_metric_sets: AnyLengthTuple[Set[MetricSpec]] = ()

        evaluation_levels = self._evaluation_lookup.evaluation_levels
        logger.debug(LazyFormat("Starting with evaluation levels", evaluation_levels=evaluation_levels))
        for current_evaluation_level in evaluation_levels:
            computed_metric_specs = self._evaluation_lookup.get_computed_metric_specs(current_evaluation_level)
            logger.debug(
                LazyFormat(
                    "Starting pass",
                    current_evaluation_level=current_evaluation_level,
                    computed_metric_specs=computed_metric_specs,
                    available_metric_sets=available_metric_sets,
                )
            )

            next_available_metric_sets: list[Set[MetricSpec]] = []
            if current_evaluation_level == 0:
                next_available_metric_sets = list(
                    FrozenOrderedSet((computed_metric,)) for computed_metric in computed_metric_specs
                )
                logger.debug(
                    LazyFormat(
                        "Finished evaluation level",
                        current_evaluation_level=current_evaluation_level,
                        next_available_metric_sets=next_available_metric_sets,
                    )
                )
                available_metric_sets = tuple(next_available_metric_sets)
                continue

            passed_metric_specs: MutableOrderedSet[MetricSpec] = MutableOrderedSet()
            metric_set: Set[MetricSpec]

            for metric_set in available_metric_sets:
                for metric_spec in metric_set:
                    if max(self._evaluation_lookup.get_consumer_levels(metric_spec)) > current_evaluation_level:
                        passed_metric_specs.add(metric_spec)

            remaining_passed_metric_specs = MutableOrderedSet(passed_metric_specs)

            for metric_spec in computed_metric_specs:
                metric_sets_to_use = find_minimum_set_cover(
                    available_sets=available_metric_sets,
                    required_elements=FrozenOrderedSet(self._evaluation_lookup.get_input_metric_specs(metric_spec)),
                )
                covered_passed_metric_specs: MutableOrderedSet[MetricSpec] = MutableOrderedSet()
                for metric_set in metric_sets_to_use:
                    covered_passed_metric_specs.update(remaining_passed_metric_specs.intersection(metric_set))
                    remaining_passed_metric_specs.difference_update(covered_passed_metric_specs)

                next_available_metric_sets.append(
                    FrozenOrderedSet(itertools.chain((metric_spec,), covered_passed_metric_specs))
                )
                logger.debug(
                    LazyFormat(
                        "Found sets to use for metric",
                        metric_spec=metric_spec,
                        metric_sets_to_use=metric_sets_to_use,
                        covered_passed_metric_specs=covered_passed_metric_specs,
                    )
                )

            if len(remaining_passed_metric_specs) > 0:
                metric_sets_to_use = find_minimum_set_cover(
                    available_sets=available_metric_sets,
                    required_elements=remaining_passed_metric_specs,
                )
                logger.debug(
                    LazyFormat(
                        "Found sets to use just for passing",
                        remaining_passed_metric_specs=remaining_passed_metric_specs,
                        metric_sets_to_use=metric_sets_to_use,
                    )
                )
                next_available_metric_sets.extend(metric_sets_to_use)

            logger.debug(
                LazyFormat(
                    "Finished evaluation level",
                    current_evaluation_level=current_evaluation_level,
                    computed_metric_specs=computed_metric_specs,
                    passed_metric_specs=passed_metric_specs,
                    next_available_metric_sets=next_available_metric_sets,
                )
            )

            available_metric_sets = tuple(next_available_metric_sets)

            # required_metric_specs = FrozenOrderedSet(itertools.chain(input_metric_specs, passed_metric_specs))
            # try:
            #     used_metric_sets = find_minimum_set_cover(
            #         available_sets=available_metric_sets,
            #         required_elements=required_metric_specs,
            #     )
            # except ValueError as e:
            #     raise RuntimeError(
            #         LazyFormat(
            #             "Unable to compute / pass all required metric specs",
            #             required_metric_specs=required_metric_specs,
            #             metric_sets_from_previous_level=available_metric_sets,
            #         )
            #     ) from e

        # raise NotImplementedError

    # def _find_metric_sets_to_use(
    #         self,
    #         input_metric_set: FrozenOrderedSet[MetricSpec],
    #         available_metric_sets: Sequence[FrozenOrderedSet[MetricSpec]],
    # ) -> Sequence[FrozenOrderedSet[MetricSpec]]:
    #     required_metric_specs = MutableOrderedSet(input_metric_set)
    #     metric_sets_to_use: list[FrozenOrderedSet[MetricSpec]] = []
    #
    #     while len(required_metric_specs) > 0:
    #         best_set = max(available_metric_sets, key=lambda s: len(s.intersection(required_metric_specs)))
    #         covered_metric_specs = best_set.intersection(required_metric_specs)


T = TypeVar("T")


def find_minimum_set_cover(available_sets: Sequence[Set[T]], required_elements: Set[T]) -> Sequence[Set[T]]:
    """Return a minimal collection of sets whose union covers all required elements.

    Uses a greedy algorithm that repeatedly selects the set covering the most
    uncovered elements.

    Example:
        find_minimum_set_cover(
            available_sets=[{1}, {2}, {3}, {1,2}],
            required_elements={1, 2, 3},
        ) = [{1,2}, {3}]

    Args:
      available_sets: Candidate sets to choose from.
      required_elements: Elements that must be covered by the selected sets.

    Returns:
      A subset of `available_sets` whose union contains all `required_elements`.

    Raises:
      ValueError: If no combination of available sets can cover all required elements.
    """
    elements_remaining = set(required_elements)
    sets_remaining = list(available_sets)
    selected_sets: list[Set[T]] = []

    while elements_remaining and sets_remaining:
        best_set = max(sets_remaining, key=lambda s: len(s & elements_remaining))
        newly_covered = best_set & elements_remaining

        if not newly_covered:
            break

        selected_sets.append(best_set)
        elements_remaining -= newly_covered
        sets_remaining.remove(best_set)

    if elements_remaining:
        raise ValueError(f"Cannot cover all required elements. Missing: {elements_remaining}")

    return selected_sets
