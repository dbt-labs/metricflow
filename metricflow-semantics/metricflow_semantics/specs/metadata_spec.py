from __future__ import annotations

from dataclasses import dataclass
from typing import Optional

from dbt_semantic_interfaces.naming.keywords import DUNDER
from dbt_semantic_interfaces.type_enums import AggregationType

from metricflow_semantics.specs.instance_spec import InstanceSpec, InstanceSpecVisitor
from metricflow_semantics.toolkit.visitor import VisitorOutputT


@dataclass(frozen=True)
class MetadataSpec(InstanceSpec):
    """A specification for a specification that is built during the dataflow plan and not defined in config."""

    element_name: str
    agg_type: Optional[AggregationType] = None

    @property
    def dunder_name(self) -> str:  # noqa: D102
        return f"{self.element_name}{DUNDER}{self.agg_type.value}" if self.agg_type else self.element_name

    def accept(self, visitor: InstanceSpecVisitor[VisitorOutputT]) -> VisitorOutputT:  # noqa: D102
        return visitor.visit_metadata_spec(self)
