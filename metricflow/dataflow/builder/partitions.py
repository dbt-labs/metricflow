from __future__ import annotations

from collections import OrderedDict
from dataclasses import dataclass
from typing import Tuple, Sequence, List

from metricflow.specs import (
    DimensionSpec,
    TimeDimensionSpec,
    InstanceSpecSet,
    PartitionSpecSet,
    DimensionReference,
)
from metricflow.model.semantics.semantic_containers import DataSourceSemantics


@dataclass(frozen=True)
class PartitionDimensionJoinDescription:
    """Describes which partition dimensions should be joined between nodes."""

    start_node_dimension_spec: DimensionSpec
    node_to_join_dimension_spec: DimensionSpec


@dataclass(frozen=True)
class PartitionTimeDimensionJoinDescription:
    """Describes which partition time dimensions should be joined between nodes."""

    start_node_time_dimension_spec: TimeDimensionSpec
    node_to_join_time_dimension_spec: TimeDimensionSpec


class PartitionJoinResolver:
    """When joining data sets, this class helps to figure out the necessary partition specs to join on."""

    def __init__(self, data_source_semantics: DataSourceSemantics) -> None:  # noqa: D
        self._data_source_semantics = data_source_semantics

    def _get_partitions(self, spec_set: InstanceSpecSet) -> PartitionSpecSet:
        """Returns the specs from the instance set that correspond to partition specs."""
        partition_dimension_specs = tuple(
            x
            for x in spec_set.dimension_specs
            if self._data_source_semantics.get_dimension(
                dimension_reference=DimensionReference(element_name=x.element_name)
            ).is_partition
        )
        partition_time_dimension_specs = tuple(
            x
            for x in spec_set.time_dimension_specs
            if self._data_source_semantics.get_dimension(
                dimension_reference=DimensionReference(element_name=x.element_name)
            ).is_partition
        )

        return PartitionSpecSet(
            dimension_specs=partition_dimension_specs,
            time_dimension_specs=partition_time_dimension_specs,
        )

    @staticmethod
    def _get_simplest_dimension_spec(dimension_specs: Sequence[DimensionSpec]) -> DimensionSpec:
        """Return the time dimension spec with the fewest identifier links."""
        assert len(dimension_specs) > 0
        sorted_dimension_specs = sorted(dimension_specs, key=lambda x: len(x.identifier_links))
        return sorted_dimension_specs[0]

    def resolve_partition_dimension_joins(
        self, start_node_spec_set: InstanceSpecSet, node_to_join_spec_set: InstanceSpecSet
    ) -> Tuple[PartitionDimensionJoinDescription, ...]:
        """Figures out which partition dimensions to join on."""
        start_node_partitions = self._get_partitions(start_node_spec_set)
        join_node_partitions = self._get_partitions(node_to_join_spec_set)

        partition_join_descriptions = []

        start_node_dimension_element_names = tuple(
            OrderedDict.fromkeys(x.element_name for x in start_node_partitions.dimension_specs)
        )

        for element_name in start_node_dimension_element_names:
            start_node_spec = PartitionJoinResolver._get_simplest_dimension_spec(
                tuple(x for x in start_node_partitions.dimension_specs if x.element_name == element_name)
            )
            join_node_time_dimension_element_names = tuple(
                OrderedDict.fromkeys(x.element_name for x in join_node_partitions.dimension_specs)
            )
            if element_name in join_node_time_dimension_element_names:
                join_node_spec = PartitionJoinResolver._get_simplest_dimension_spec(
                    tuple(x for x in join_node_partitions.dimension_specs if x.element_name == element_name)
                )
                partition_join_descriptions.append(
                    PartitionDimensionJoinDescription(
                        start_node_dimension_spec=start_node_spec,
                        node_to_join_dimension_spec=join_node_spec,
                    )
                )

        return tuple(partition_join_descriptions)

    @staticmethod
    def _get_simplest_time_dimension_spec(time_dimension_specs: Sequence[TimeDimensionSpec]) -> TimeDimensionSpec:
        """Return the time dimension spec with the smallest granularity, then fewest identifier links."""
        assert len(time_dimension_specs) > 0
        sorted_specs = sorted(time_dimension_specs, key=lambda x: (x.time_granularity, len(x.identifier_links)))
        return sorted_specs[0]

    def resolve_partition_time_dimension_joins(
        self, start_node_spec_set: InstanceSpecSet, node_to_join_spec_set: InstanceSpecSet
    ) -> Tuple[PartitionTimeDimensionJoinDescription, ...]:
        """Figures out which partition time dimensions to join on."""
        start_node_partitions = self._get_partitions(start_node_spec_set)
        join_node_partitions = self._get_partitions(node_to_join_spec_set)
        partition_join_descriptions: List[PartitionTimeDimensionJoinDescription] = []

        # Get all unique element names
        time_dimension_element_names = tuple(
            OrderedDict.fromkeys(x.element_name for x in start_node_partitions.time_dimension_specs)
        )
        for element_name in time_dimension_element_names:
            start_node_spec = PartitionJoinResolver._get_simplest_time_dimension_spec(
                tuple(x for x in start_node_partitions.time_dimension_specs if x.element_name == element_name)
            )
            join_node_time_dimension_element_names = tuple(
                OrderedDict.fromkeys(x.element_name for x in join_node_partitions.time_dimension_specs)
            )
            if element_name in join_node_time_dimension_element_names:
                join_node_spec = PartitionJoinResolver._get_simplest_time_dimension_spec(
                    tuple(x for x in join_node_partitions.time_dimension_specs if x.element_name == element_name)
                )
                partition_join_descriptions.append(
                    PartitionTimeDimensionJoinDescription(
                        start_node_time_dimension_spec=start_node_spec,
                        node_to_join_time_dimension_spec=join_node_spec,
                    )
                )
        return tuple(partition_join_descriptions)
