from __future__ import annotations

import dataclasses
import itertools
from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import TYPE_CHECKING, Generic, Iterable, List, Sequence, Tuple, TypeVar

from dbt_semantic_interfaces.dataclass_serialization import SerializableDataclass
from typing_extensions import override

from metricflow_semantics.specs.instance_spec import InstanceSpec, InstanceSpecVisitor, LinkableInstanceSpec
from metricflow_semantics.toolkit.merger import Mergeable

if TYPE_CHECKING:
    from metricflow_semantics.specs.dimension_spec import DimensionSpec
    from metricflow_semantics.specs.entity_spec import EntitySpec
    from metricflow_semantics.specs.group_by_metric_spec import GroupByMetricSpec
    from metricflow_semantics.specs.metadata_spec import MetadataSpec
    from metricflow_semantics.specs.metric_spec import MetricSpec
    from metricflow_semantics.specs.simple_metric_input_spec import SimpleMetricInputSpec
    from metricflow_semantics.specs.time_dimension_spec import TimeDimensionSpec


@dataclass(frozen=True)
class InstanceSpecSet(Mergeable, SerializableDataclass):
    """Consolidates all specs used in an instance set."""

    metric_specs: Tuple[MetricSpec, ...] = ()
    simple_metric_input_specs: Tuple[SimpleMetricInputSpec, ...] = ()
    dimension_specs: Tuple[DimensionSpec, ...] = ()
    entity_specs: Tuple[EntitySpec, ...] = ()
    time_dimension_specs: Tuple[TimeDimensionSpec, ...] = ()
    group_by_metric_specs: Tuple[GroupByMetricSpec, ...] = ()
    metadata_specs: Tuple[MetadataSpec, ...] = ()

    @override
    def merge(self, other: InstanceSpecSet) -> InstanceSpecSet:
        return InstanceSpecSet(
            metric_specs=self.metric_specs + other.metric_specs,
            simple_metric_input_specs=self.simple_metric_input_specs + other.simple_metric_input_specs,
            dimension_specs=self.dimension_specs + other.dimension_specs,
            entity_specs=self.entity_specs + other.entity_specs,
            group_by_metric_specs=self.group_by_metric_specs + other.group_by_metric_specs,
            time_dimension_specs=self.time_dimension_specs + other.time_dimension_specs,
            metadata_specs=self.metadata_specs + other.metadata_specs,
        )

    @override
    @classmethod
    def empty_instance(cls) -> InstanceSpecSet:
        return InstanceSpecSet()

    def dedupe(self) -> InstanceSpecSet:
        """De-duplicates repeated elements.

        TBD: Have merge de-duplicate instead.
        """
        metric_specs_deduped = []
        for metric_spec in self.metric_specs:
            if metric_spec not in metric_specs_deduped:
                metric_specs_deduped.append(metric_spec)

        specs_deduped = []
        for simple_metric_input_specs in self.simple_metric_input_specs:
            if simple_metric_input_specs not in specs_deduped:
                specs_deduped.append(simple_metric_input_specs)

        dimension_specs_deduped = []
        for dimension_spec in self.dimension_specs:
            if dimension_spec not in dimension_specs_deduped:
                dimension_specs_deduped.append(dimension_spec)

        time_dimension_specs_deduped = []
        for time_dimension_spec in self.time_dimension_specs:
            if time_dimension_spec not in time_dimension_specs_deduped:
                time_dimension_specs_deduped.append(time_dimension_spec)

        entity_specs_deduped = []
        for entity_spec in self.entity_specs:
            if entity_spec not in entity_specs_deduped:
                entity_specs_deduped.append(entity_spec)

        group_by_metric_specs_deduped = []
        for group_by_metric_spec in self.group_by_metric_specs:
            if group_by_metric_spec not in group_by_metric_specs_deduped:
                group_by_metric_specs_deduped.append(group_by_metric_spec)

        return InstanceSpecSet(
            metric_specs=tuple(metric_specs_deduped),
            simple_metric_input_specs=tuple(specs_deduped),
            dimension_specs=tuple(dimension_specs_deduped),
            time_dimension_specs=tuple(time_dimension_specs_deduped),
            entity_specs=tuple(entity_specs_deduped),
            group_by_metric_specs=tuple(group_by_metric_specs_deduped),
        )

    @property
    def linkable_specs(self) -> Sequence[LinkableInstanceSpec]:
        """All linkable specs in this set."""
        return list(
            itertools.chain(
                self.dimension_specs, self.time_dimension_specs, self.entity_specs, self.group_by_metric_specs
            )
        )

    @property
    def all_specs(self) -> Sequence[InstanceSpec]:  # noqa: D102
        return tuple(
            itertools.chain(
                self.simple_metric_input_specs,
                self.dimension_specs,
                self.time_dimension_specs,
                self.entity_specs,
                self.group_by_metric_specs,
                self.metric_specs,
                self.metadata_specs,
            )
        )

    def transform(  # noqa: D102
        self, transform_function: InstanceSpecSetTransform[TransformOutputT]
    ) -> TransformOutputT:
        return transform_function.transform(self)

    @property
    def metric_time_specs(self) -> Sequence[TimeDimensionSpec]:
        """Returns any specs referring to metric time at any grain."""
        return tuple(
            time_dimension_spec
            for time_dimension_spec in self.time_dimension_specs
            if time_dimension_spec.is_metric_time
        )

    @staticmethod
    def create_from_specs(specs: Sequence[InstanceSpec]) -> InstanceSpecSet:  # noqa: D102
        return group_specs_by_type(specs)


TransformOutputT = TypeVar("TransformOutputT")


class InstanceSpecSetTransform(Generic[TransformOutputT], ABC):
    """Function to use for transforming spec sets."""

    @abstractmethod
    def transform(self, spec_set: InstanceSpecSet) -> TransformOutputT:  # noqa: D102
        pass


@dataclass
class _GroupSpecByTypeVisitor(InstanceSpecVisitor[None]):
    """Groups a spec by type into an `InstanceSpecSet`."""

    metric_specs: List[MetricSpec] = dataclasses.field(default_factory=list)
    simple_metric_input_specs: List[SimpleMetricInputSpec] = dataclasses.field(default_factory=list)
    dimension_specs: List[DimensionSpec] = dataclasses.field(default_factory=list)
    entity_specs: List[EntitySpec] = dataclasses.field(default_factory=list)
    time_dimension_specs: List[TimeDimensionSpec] = dataclasses.field(default_factory=list)
    group_by_metric_specs: List[GroupByMetricSpec] = dataclasses.field(default_factory=list)
    metadata_specs: List[MetadataSpec] = dataclasses.field(default_factory=list)

    @override
    def visit_simple_metric_input_spec(self, spec: SimpleMetricInputSpec) -> None:
        self.simple_metric_input_specs.append(spec)

    @override
    def visit_dimension_spec(self, dimension_spec: DimensionSpec) -> None:
        self.dimension_specs.append(dimension_spec)

    @override
    def visit_time_dimension_spec(self, time_dimension_spec: TimeDimensionSpec) -> None:
        self.time_dimension_specs.append(time_dimension_spec)

    @override
    def visit_entity_spec(self, entity_spec: EntitySpec) -> None:
        self.entity_specs.append(entity_spec)

    @override
    def visit_group_by_metric_spec(self, group_by_metric_spec: GroupByMetricSpec) -> None:
        self.group_by_metric_specs.append(group_by_metric_spec)

    @override
    def visit_metric_spec(self, metric_spec: MetricSpec) -> None:
        self.metric_specs.append(metric_spec)

    @override
    def visit_metadata_spec(self, metadata_spec: MetadataSpec) -> None:
        self.metadata_specs.append(metadata_spec)


def group_specs_by_type(specs: Iterable[InstanceSpec]) -> InstanceSpecSet:
    """Groups a sequence of specs by type."""
    grouper = _GroupSpecByTypeVisitor()
    for spec in specs:
        spec.accept(grouper)

    return InstanceSpecSet(
        metric_specs=tuple(grouper.metric_specs),
        simple_metric_input_specs=tuple(grouper.simple_metric_input_specs),
        dimension_specs=tuple(grouper.dimension_specs),
        entity_specs=tuple(grouper.entity_specs),
        time_dimension_specs=tuple(grouper.time_dimension_specs),
        group_by_metric_specs=tuple(grouper.group_by_metric_specs),
        metadata_specs=tuple(grouper.metadata_specs),
    )


def group_spec_by_type(spec: InstanceSpec) -> InstanceSpecSet:
    """Similar to group_specs_by_type() but for a single spec."""
    return group_specs_by_type((spec,))
