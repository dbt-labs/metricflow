from __future__ import annotations

from dataclasses import dataclass
from functools import cached_property
from typing import Optional

from dbt_semantic_interfaces.type_enums import AggregationType
from typing_extensions import override

from metricflow_semantics.naming.linkable_spec_name import StructuredLinkableSpecName
from metricflow_semantics.specs.instance_spec import InstanceSpec, InstanceSpecVisitor
from metricflow_semantics.visitor import VisitorOutputT


@dataclass(frozen=True)
class MetadataSpec(InstanceSpec):
    """A specification for a specification that is built during the dataflow plan and not defined in config."""

    element_name: str
    agg_type: Optional[AggregationType] = None

    @override
    @cached_property
    def structured_name(self) -> StructuredLinkableSpecName:
        if self.agg_type is not None:
            return StructuredLinkableSpecName(
                entity_link_names=(self.element_name,),
                element_name=self.agg_type.value,
            )

        return StructuredLinkableSpecName(entity_link_names=(), element_name=self.element_name)

    def accept(self, visitor: InstanceSpecVisitor[VisitorOutputT]) -> VisitorOutputT:  # noqa: D102
        return visitor.visit_metadata_spec(self)
