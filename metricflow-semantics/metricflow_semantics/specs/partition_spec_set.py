from __future__ import annotations

from dataclasses import dataclass
from typing import Tuple

from dbt_semantic_interfaces.dataclass_serialization import SerializableDataclass

from metricflow_semantics.specs.dimension_spec import DimensionSpec
from metricflow_semantics.specs.time_dimension_spec import TimeDimensionSpec


@dataclass(frozen=True)
class PartitionSpecSet(SerializableDataclass):
    """Grouping of the linkable specs."""

    dimension_specs: Tuple[DimensionSpec, ...] = ()
    time_dimension_specs: Tuple[TimeDimensionSpec, ...] = ()
