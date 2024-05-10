from __future__ import annotations

import itertools
import typing
from dataclasses import dataclass
from typing import Dict, List, Sequence, Tuple

from dbt_semantic_interfaces.dataclass_serialization import SerializableDataclass
from dbt_semantic_interfaces.references import MeasureReference, MetricReference
from typing_extensions import override

from metricflow_semantics.collection_helpers.merger import Mergeable
from metricflow_semantics.specs.spec_classes import (
    DimensionSpec,
    EntitySpec,
    GroupByMetricSpec,
    InstanceSpec,
    LinkableInstanceSpec,
    TimeDimensionSpec,
)
from metricflow_semantics.specs.spec_set import InstanceSpecSet

if typing.TYPE_CHECKING:
    from metricflow_semantics.model.semantics.metric_lookup import MetricLookup
    from metricflow_semantics.model.semantics.semantic_model_lookup import SemanticModelLookup


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

    def included_agg_time_dimension_specs_for_metric(
        self, metric_reference: MetricReference, metric_lookup: MetricLookup
    ) -> List[TimeDimensionSpec]:
        """Get the time dims included that are valid agg time dimensions for the specified metric."""
        queried_metric_time_specs = list(self.metric_time_specs)

        valid_agg_time_dimensions = metric_lookup.get_valid_agg_time_dimensions_for_metric(metric_reference)
        queried_agg_time_dimension_specs = (
            list(set(self.time_dimension_specs).intersection(set(valid_agg_time_dimensions)))
            + queried_metric_time_specs
        )

        return queried_agg_time_dimension_specs

    def included_agg_time_dimension_specs_for_measure(
        self, measure_reference: MeasureReference, semantic_model_lookup: SemanticModelLookup
    ) -> List[TimeDimensionSpec]:
        """Get the time dims included that are valid agg time dimensions for the specified measure."""
        queried_metric_time_specs = list(self.metric_time_specs)

        valid_agg_time_dimensions = semantic_model_lookup.get_agg_time_dimension_specs_for_measure(measure_reference)
        queried_agg_time_dimension_specs = (
            list(set(self.time_dimension_specs).intersection(set(valid_agg_time_dimensions)))
            + queried_metric_time_specs
        )

        return queried_agg_time_dimension_specs

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

    @override
    def merge(self, other: LinkableSpecSet) -> LinkableSpecSet:
        return LinkableSpecSet(
            dimension_specs=self.dimension_specs + other.dimension_specs,
            time_dimension_specs=self.time_dimension_specs + other.time_dimension_specs,
            entity_specs=self.entity_specs + other.entity_specs,
            group_by_metric_specs=self.group_by_metric_specs + other.group_by_metric_specs,
        )

    @override
    @classmethod
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

    @property
    def as_spec_set(self) -> InstanceSpecSet:  # noqa: D102
        return InstanceSpecSet(
            dimension_specs=self.dimension_specs,
            time_dimension_specs=self.time_dimension_specs,
            entity_specs=self.entity_specs,
            group_by_metric_specs=self.group_by_metric_specs,
        )

    def difference(self, other: LinkableSpecSet) -> LinkableSpecSet:  # noqa: D102
        return LinkableSpecSet(
            dimension_specs=tuple(set(self.dimension_specs) - set(other.dimension_specs)),
            time_dimension_specs=tuple(set(self.time_dimension_specs) - set(other.time_dimension_specs)),
            entity_specs=tuple(set(self.entity_specs) - set(other.entity_specs)),
            group_by_metric_specs=tuple(set(self.group_by_metric_specs) - set(other.group_by_metric_specs)),
        )

    def __len__(self) -> int:  # noqa: D105
        return len(self.dimension_specs) + len(self.time_dimension_specs) + len(self.entity_specs)

    @staticmethod
    def create_from_spec_set(spec_set: InstanceSpecSet) -> LinkableSpecSet:  # noqa: D102
        return LinkableSpecSet(
            dimension_specs=spec_set.dimension_specs,
            time_dimension_specs=spec_set.time_dimension_specs,
            entity_specs=spec_set.entity_specs,
            group_by_metric_specs=spec_set.group_by_metric_specs,
        )

    @staticmethod
    def create_from_specs(specs: Sequence[InstanceSpec]) -> LinkableSpecSet:  # noqa: D102
        return LinkableSpecSet.create_from_spec_set(InstanceSpecSet.create_from_specs(specs))
