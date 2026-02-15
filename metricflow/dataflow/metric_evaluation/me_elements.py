from __future__ import annotations

import itertools
import logging
import pathlib
import typing
from abc import ABC, abstractmethod
from collections import defaultdict
from collections.abc import Mapping, Sequence, Set, Iterator
from dataclasses import dataclass
from enum import Enum
from functools import cached_property
from pathlib import Path
from typing import Callable, Generic, Optional, TypeVar, Iterable, Sized, AbstractSet, override

from dbt_semantic_interfaces.protocols import MetricInput
from dbt_semantic_interfaces.type_enums import TimeGranularity

from typing_extensions import Self, override

from metricflow_semantics.specs.metric_spec import MetricSpec
from metricflow_semantics.toolkit.collections.ordered_set import FrozenOrderedSet, OrderedSet

from metricflow_semantics.toolkit.dataclass_helpers import fast_frozen_dataclass
from metricflow_semantics.toolkit.mf_logging.lazy_formattable import LazyFormat
from metricflow_semantics.toolkit.string_helpers import mf_dedent

logger = logging.getLogger(__name__)


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
class MetricDescriptorSet(Sized, Iterable[MetricDescriptor]):

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

    @cached_property
    def descriptors(self) -> OrderedSet[MetricDescriptor]:
        return self.computed_metric_descriptors.union(self.passthrough_metric_descriptors)

    def prune(self, allowed_descriptors: Set[MetricDescriptor]) -> MetricDescriptorSet:
        return MetricDescriptorSet.create(
            computed_metric_descriptors=(
                descriptor for descriptor in self.computed_metric_descriptors if descriptor in allowed_descriptors
            ),
            passthrough_metric_descriptors=(
                descriptor for descriptor in self.passthrough_metric_descriptors if descriptor in allowed_descriptors
            ),
        )

    @override
    def __len__(self) -> int:
        return len(self.computed_metric_descriptors) + len(self.passthrough_metric_descriptors)

    @override
    def __iter__(self) -> Iterator[MetricDescriptor]:
        return itertools.chain(self.computed_metric_descriptors, self.passthrough_metric_descriptors)
