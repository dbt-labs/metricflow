from __future__ import annotations

from dataclasses import dataclass
from typing import Optional

from dbt_semantic_interfaces.implementations.metric import PydanticMetricTimeWindow
from dbt_semantic_interfaces.protocols.metric import MetricTimeWindow
from dbt_semantic_interfaces.references import MetricReference
from dbt_semantic_interfaces.type_enums import TimeGranularity

from metricflow_semantics.specs.instance_spec import InstanceSpec, InstanceSpecVisitor
from metricflow_semantics.specs.where_filter.where_filter_spec_set import WhereFilterSpecSet
from metricflow_semantics.toolkit.visitor import VisitorOutputT


@dataclass(frozen=True)
class MetricSpec(InstanceSpec):  # noqa: D101
    # Time-over-time could go here
    element_name: str
    filter_spec_set: WhereFilterSpecSet = WhereFilterSpecSet()
    alias: Optional[str] = None
    offset_window: Optional[PydanticMetricTimeWindow] = None
    offset_to_grain: Optional[TimeGranularity] = None

    @staticmethod
    def from_element_name(element_name: str) -> MetricSpec:  # noqa: D102
        return MetricSpec(element_name=element_name)

    @property
    def dunder_name(self) -> str:  # noqa: D102
        return self.element_name

    @staticmethod
    def from_reference(reference: MetricReference) -> MetricSpec:
        """Initialize from a metric reference instance."""
        return MetricSpec(element_name=reference.element_name)

    def accept(self, visitor: InstanceSpecVisitor[VisitorOutputT]) -> VisitorOutputT:  # noqa: D102
        return visitor.visit_metric_spec(self)

    @property
    def reference(self) -> MetricReference:
        """Return the reference object that is used for referencing the associated metric in the manifest."""
        return MetricReference(element_name=self.element_name)

    @property
    def has_time_offset(self) -> bool:  # noqa: D102
        return bool(self.offset_window or self.offset_to_grain)

    def without_offset(self) -> MetricSpec:
        """Represents the metric spec with any time offsets removed."""
        return MetricSpec(element_name=self.element_name, filter_spec_set=self.filter_spec_set, alias=self.alias)

    def with_alias(self, alias: Optional[str]) -> MetricSpec:
        """Add the alias to the metric spec."""
        return MetricSpec(
            element_name=self.element_name,
            filter_spec_set=self.filter_spec_set,
            alias=alias,
            offset_window=self.offset_window,
            offset_to_grain=self.offset_to_grain,
        )

    def without_filter_specs(self) -> MetricSpec:  # noqa: D102
        return MetricSpec(
            element_name=self.element_name,
            alias=self.alias,
            offset_window=self.offset_window,
            offset_to_grain=self.offset_to_grain,
        )

    @property
    def standard_offset_window(self) -> Optional[MetricTimeWindow]:
        """Return the offset window if it exists and uses a standard granularity."""
        if self.offset_window and self.offset_window.is_standard_granularity:
            return self.offset_window
        return None

    @property
    def custom_offset_window(self) -> Optional[MetricTimeWindow]:
        """Return the offset window if it exists and uses a custom granularity."""
        if self.offset_window and not self.offset_window.is_standard_granularity:
            return self.offset_window
        return None
