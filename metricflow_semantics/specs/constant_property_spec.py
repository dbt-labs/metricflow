from __future__ import annotations

from dataclasses import dataclass

from metricflow_semantics.specs.instance_spec import LinkableInstanceSpec

from metricflow_semantic_interfaces.dataclass_serialization import SerializableDataclass


@dataclass(frozen=True)
class ConstantPropertySpec(SerializableDataclass):
    """Includes the specs that are joined for conversion metric's constant properties."""

    base_spec: LinkableInstanceSpec
    conversion_spec: LinkableInstanceSpec
