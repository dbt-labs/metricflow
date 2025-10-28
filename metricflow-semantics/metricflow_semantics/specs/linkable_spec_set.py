from __future__ import annotations

import dataclasses
import itertools
import typing
from dataclasses import dataclass
from typing import Dict, List, Sequence, Tuple

from dbt_semantic_interfaces.dataclass_serialization import SerializableDataclass
from typing_extensions import override

from metricflow_semantics.specs.dimension_spec import DimensionSpec
from metricflow_semantics.specs.entity_spec import EntitySpec
from metricflow_semantics.specs.group_by_metric_spec import GroupByMetricSpec
from metricflow_semantics.specs.instance_spec import InstanceSpecVisitor, LinkableInstanceSpec
from metricflow_semantics.specs.spec_set import InstanceSpecSet
from metricflow_semantics.specs.time_dimension_spec import TimeDimensionSpec
from metricflow_semantics.toolkit.merger import Mergeable

if typing.TYPE_CHECKING:
    from metricflow_semantics.specs.metadata_spec import MetadataSpec
    from metricflow_semantics.specs.metric_spec import MetricSpec
    from metricflow_semantics.specs.simple_metric_input_spec import SimpleMetricInputSpec


@dataclass(frozen=True)
class LinkableSpecSet(Mergeable, SerializableDataclass):
    """Groups linkable specs."""

    dimension_specs: Tuple[DimensionSpec, ...] = ()
    time_dimension_specs: Tuple[TimeDimensionSpec, ...] = ()
    entity_specs: Tuple[EntitySpec, ...] = ()
    group_by_metric_specs: Tuple[GroupByMetricSpec, ...] = ()

    @property
    def contains_metric_time(self) -> bool:
        """Returns true if this set contains a spec referring to metric time at any grain."""
        return len(self.metric_time_specs) > 0

    @property
    def time_dimension_specs_with_custom_grain(self) -> Tuple[TimeDimensionSpec, ...]:  # noqa: D102
        return tuple([spec for spec in self.time_dimension_specs if spec.has_custom_grain])

    def replace_custom_granularity_with_base_granularity(self) -> LinkableSpecSet:
        """Return the same spec set, replacing any custom time granularity with its base granularity."""
        return LinkableSpecSet(
            dimension_specs=self.dimension_specs,
            time_dimension_specs=tuple(
                [time_dimension_spec.with_base_grain() for time_dimension_spec in self.time_dimension_specs]
            ),
            entity_specs=self.entity_specs,
            group_by_metric_specs=self.group_by_metric_specs,
        )

    @property
    def metric_time_specs(self) -> Sequence[TimeDimensionSpec]:
        """Returns any specs referring to metric time at any grain."""
        return tuple(
            time_dimension_spec
            for time_dimension_spec in self.time_dimension_specs
            if time_dimension_spec.is_metric_time
        )

    @property
    def as_tuple(self) -> Tuple[LinkableInstanceSpec, ...]:  # noqa: D102
        return tuple(
            itertools.chain(
                self.dimension_specs, self.time_dimension_specs, self.entity_specs, self.group_by_metric_specs
            )
        )

    def add_specs(
        self,
        dimension_specs: Tuple[DimensionSpec, ...] = (),
        time_dimension_specs: Tuple[TimeDimensionSpec, ...] = (),
        entity_specs: Tuple[EntitySpec, ...] = (),
        group_by_metric_specs: Tuple[GroupByMetricSpec, ...] = (),
    ) -> LinkableSpecSet:
        """Return a new set with the new specs in addition to the existing ones."""
        return LinkableSpecSet(
            dimension_specs=self.dimension_specs + dimension_specs,
            time_dimension_specs=self.time_dimension_specs + time_dimension_specs,
            entity_specs=self.entity_specs + entity_specs,
            group_by_metric_specs=self.group_by_metric_specs + group_by_metric_specs,
        )

    @override
    def merge(self, other: LinkableSpecSet) -> LinkableSpecSet:
        return self.add_specs(
            dimension_specs=other.dimension_specs,
            time_dimension_specs=other.time_dimension_specs,
            entity_specs=other.entity_specs,
            group_by_metric_specs=other.group_by_metric_specs,
        )

    @classmethod
    @override
    def empty_instance(cls) -> LinkableSpecSet:
        return LinkableSpecSet()

    def dedupe(self) -> LinkableSpecSet:  # noqa: D102
        # Use dictionaries to dedupe as it preserves insertion order.

        dimension_spec_dict: Dict[DimensionSpec, None] = {}
        for dimension_spec in self.dimension_specs:
            dimension_spec_dict[dimension_spec] = None

        time_dimension_spec_dict: Dict[TimeDimensionSpec, None] = {}
        for time_dimension_spec in self.time_dimension_specs:
            time_dimension_spec_dict[time_dimension_spec] = None

        entity_spec_dict: Dict[EntitySpec, None] = {}
        for entity_spec in self.entity_specs:
            entity_spec_dict[entity_spec] = None

        group_by_metric_spec_dict: Dict[GroupByMetricSpec, None] = {}
        for group_by_metric in self.group_by_metric_specs:
            group_by_metric_spec_dict[group_by_metric] = None

        return LinkableSpecSet(
            dimension_specs=tuple(dimension_spec_dict.keys()),
            time_dimension_specs=tuple(time_dimension_spec_dict.keys()),
            entity_specs=tuple(entity_spec_dict.keys()),
            group_by_metric_specs=tuple(group_by_metric_spec_dict.keys()),
        )

    def is_subset_of(self, other_set: LinkableSpecSet) -> bool:  # noqa: D102
        return set(self.as_tuple).issubset(set(other_set.as_tuple))

    def difference(self, other: LinkableSpecSet) -> LinkableSpecSet:  # noqa: D102
        return LinkableSpecSet(
            dimension_specs=tuple(set(self.dimension_specs) - set(other.dimension_specs)),
            time_dimension_specs=tuple(set(self.time_dimension_specs) - set(other.time_dimension_specs)),
            entity_specs=tuple(set(self.entity_specs) - set(other.entity_specs)),
            group_by_metric_specs=tuple(set(self.group_by_metric_specs) - set(other.group_by_metric_specs)),
        )

    @staticmethod
    def create_from_specs(specs: Sequence[LinkableInstanceSpec]) -> LinkableSpecSet:  # noqa: D102
        return _group_specs_by_type(specs)

    @property
    def as_instance_spec_set(self) -> InstanceSpecSet:  # noqa: D102
        return InstanceSpecSet(
            dimension_specs=self.dimension_specs,
            entity_specs=self.entity_specs,
            time_dimension_specs=self.time_dimension_specs,
            group_by_metric_specs=self.group_by_metric_specs,
        )


@dataclass
class _GroupSpecByTypeVisitor(InstanceSpecVisitor[None]):
    """Groups a spec by type into an `InstanceSpecSet`."""

    dimension_specs: List[DimensionSpec] = dataclasses.field(default_factory=list)
    entity_specs: List[EntitySpec] = dataclasses.field(default_factory=list)
    time_dimension_specs: List[TimeDimensionSpec] = dataclasses.field(default_factory=list)
    group_by_metric_specs: List[GroupByMetricSpec] = dataclasses.field(default_factory=list)

    @override
    def visit_simple_metric_input_spec(self, spec: SimpleMetricInputSpec) -> None:
        pass

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
        pass

    @override
    def visit_metadata_spec(self, metadata_spec: MetadataSpec) -> None:
        pass


def _group_specs_by_type(specs: Sequence[LinkableInstanceSpec]) -> LinkableSpecSet:
    """Groups a sequence of specs by type."""
    grouper = _GroupSpecByTypeVisitor()
    for spec in specs:
        spec.accept(grouper)

    return LinkableSpecSet(
        dimension_specs=tuple(grouper.dimension_specs),
        entity_specs=tuple(grouper.entity_specs),
        time_dimension_specs=tuple(grouper.time_dimension_specs),
        group_by_metric_specs=tuple(grouper.group_by_metric_specs),
    )
