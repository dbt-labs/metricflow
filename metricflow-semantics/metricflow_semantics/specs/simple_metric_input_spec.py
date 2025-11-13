from __future__ import annotations

from dataclasses import dataclass
from functools import cached_property
from typing import Optional

from dbt_semantic_interfaces.protocols import MetricTimeWindow
from dbt_semantic_interfaces.type_enums import TimeGranularity

from metricflow_semantics.model.semantics.simple_metric_input import SimpleMetricInput
from metricflow_semantics.specs.instance_spec import InstanceSpec, InstanceSpecVisitor
from metricflow_semantics.specs.where_filter.where_filter_spec_set import WhereFilterSpecSet
from metricflow_semantics.sql.sql_join_type import SqlJoinType
from metricflow_semantics.toolkit.dataclass_helpers import fast_frozen_dataclass
from metricflow_semantics.toolkit.visitor import VisitorOutputT


@dataclass(frozen=True)
class SimpleMetricInputSpec(InstanceSpec):  # noqa: D101
    element_name: str
    fill_nulls_with: Optional[int] = None

    @property
    def dunder_name(self) -> str:  # noqa: D102
        return self.element_name

    def accept(self, visitor: InstanceSpecVisitor[VisitorOutputT]) -> VisitorOutputT:  # noqa: D102
        return visitor.visit_simple_metric_input_spec(self)


@dataclass(frozen=True)
class CumulativeDescription:
    """If a simple metric is a part of a cumulative metric, this represents the associated parameters."""

    cumulative_window: Optional[MetricTimeWindow]
    cumulative_grain_to_date: Optional[TimeGranularity]


@fast_frozen_dataclass()
class SimpleMetricRecipe:
    """Describes how to build a simple metric but with modifications."""

    simple_metric_input: SimpleMetricInput

    # For the filter defined in `simple_metric_input`
    metric_filter_spec_set: WhereFilterSpecSet
    # For additional filters that might be needed (e.g. a filter defined in a derived metric or query).
    additional_filter_spec_set: WhereFilterSpecSet

    offset_window: Optional[MetricTimeWindow]
    offset_to_grain: Optional[TimeGranularity]
    cumulative_description: Optional[CumulativeDescription]
    before_aggregation_time_spine_join_description: Optional[JoinToTimeSpineDescription]
    after_aggregation_time_spine_join_description: Optional[JoinToTimeSpineDescription]

    @cached_property
    def combined_filter_spec_set(self) -> WhereFilterSpecSet:  # noqa: D102
        return self.metric_filter_spec_set.merge(self.additional_filter_spec_set)


@dataclass(frozen=True)
class JoinToTimeSpineDescription:
    """Describes how a time spine join should be performed."""

    join_type: SqlJoinType
    offset_window: Optional[MetricTimeWindow]
    offset_to_grain: Optional[TimeGranularity]

    @property
    def standard_offset_window(self) -> Optional[MetricTimeWindow]:
        """Return the offset window if it uses a standard granularity."""
        if self.offset_window and self.offset_window.is_standard_granularity:
            return self.offset_window
        return None

    @property
    def custom_offset_window(self) -> Optional[MetricTimeWindow]:
        """Return the offset window if it uses a custom granularity."""
        if self.offset_window and not self.offset_window.is_standard_granularity:
            return self.offset_window
        return None

    @property
    def uses_offset(self) -> bool:
        """Return True if the simple-metric input uses an offset."""
        return self.offset_window is not None or self.offset_to_grain is not None
