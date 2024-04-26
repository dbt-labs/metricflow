from __future__ import annotations

from dataclasses import dataclass
from typing import Tuple

from dbt_semantic_interfaces.dataclass_serialization import SerializableDataclass

from metricflow_semantics.specs.spec_classes import DimensionSpec, TimeDimensionSpec


@dataclass(frozen=True)
class PartitionSpecSet(SerializableDataclass):
    """Grouping of the linkable specs."""

    dimension_specs: Tuple[DimensionSpec, ...] = ()
    time_dimension_specs: Tuple[TimeDimensionSpec, ...] = ()
