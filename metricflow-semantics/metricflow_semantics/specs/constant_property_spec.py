from __future__ import annotations

from dataclasses import dataclass

from dbt_semantic_interfaces.dataclass_serialization import SerializableDataclass

from metricflow_semantics.specs.instance_spec import LinkableInstanceSpec


@dataclass(frozen=True)
class ConstantPropertySpec(SerializableDataclass):
    """Includes the specs that are joined for conversion metric's constant properties."""

    base_spec: LinkableInstanceSpec
    conversion_spec: LinkableInstanceSpec
