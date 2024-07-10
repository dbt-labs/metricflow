from __future__ import annotations

from dataclasses import dataclass

from dbt_semantic_interfaces.dataclass_serialization import SerializableDataclass

from metricflow_semantics.specs.instance_spec import InstanceSpec


@dataclass(frozen=True)
class OrderBySpec(SerializableDataclass):  # noqa: D101
    instance_spec: InstanceSpec
    descending: bool
