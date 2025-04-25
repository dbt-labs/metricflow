from __future__ import annotations

from dataclasses import dataclass
from typing import Optional

from dbt_semantic_interfaces.dataclass_serialization import SerializableDataclass

from metricflow_semantics.specs.instance_spec import InstanceSpec


@dataclass(frozen=True)
class OrderBySpec(SerializableDataclass):  # noqa: D101
    instance_spec: InstanceSpec
    descending: bool

    def with_alias(self, alias: Optional[str]) -> OrderBySpec:
        """Return a order by spec that's the same as self but with alias replaced."""
        return OrderBySpec(instance_spec=self.instance_spec.with_alias(alias), descending=self.descending)
