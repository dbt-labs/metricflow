from __future__ import annotations

from dataclasses import dataclass
from typing import Optional

from dbt_semantic_interfaces.dataclass_serialization import SerializableDataclass
from dbt_semantic_interfaces.protocols import MetricTimeWindow
from dbt_semantic_interfaces.references import MeasureReference
from dbt_semantic_interfaces.type_enums import TimeGranularity

from metricflow_semantics.specs.instance_spec import InstanceSpec, InstanceSpecVisitor
from metricflow_semantics.specs.non_additive_dimension_spec import NonAdditiveDimensionSpec
from metricflow_semantics.specs.where_filter.where_filter_spec_set import WhereFilterSpecSet
from metricflow_semantics.sql.sql_join_type import SqlJoinType
from metricflow_semantics.visitor import VisitorOutputT


@dataclass(frozen=True)
class MeasureSpec(InstanceSpec):  # noqa: D101
    element_name: str
    non_additive_dimension_spec: Optional[NonAdditiveDimensionSpec] = None
    fill_nulls_with: Optional[int] = None

    @staticmethod
    def from_reference(reference: MeasureReference) -> MeasureSpec:
        """Initialize from a measure reference instance."""
        return MeasureSpec(element_name=reference.element_name)

    @property
    def qualified_name(self) -> str:  # noqa: D102
        return self.element_name

    @property
    def reference(self) -> MeasureReference:  # noqa: D102
        return MeasureReference(element_name=self.element_name)

    def accept(self, visitor: InstanceSpecVisitor[VisitorOutputT]) -> VisitorOutputT:  # noqa: D102
        return visitor.visit_measure_spec(self)


@dataclass(frozen=True)
class CumulativeMeasureDescription:
    """If a measure is a part of a cumulative metric, this represents the associated parameters."""

    cumulative_window: Optional[MetricTimeWindow]
    cumulative_grain_to_date: Optional[TimeGranularity]


@dataclass(frozen=True)
class MetricInputMeasureSpec(SerializableDataclass):
    """The spec for a measure defined as a base metric input.

    This is necessary because the MeasureSpec is used as a key linking the measures used in the query
    to the measures defined in the semantic models. Adding metric-specific information, like constraints,
    causes lookups connecting query -> semantic model to fail in strange ways. This spec, then, provides
    both the key (in the form of a MeasureSpec) along with whatever measure-specific attributes
    a user might specify in a metric definition or query accessing the metric itself.
    """

    measure_spec: MeasureSpec
    fill_nulls_with: Optional[int] = None
    offset_window: Optional[MetricTimeWindow] = None
    offset_to_grain: Optional[TimeGranularity] = None
    cumulative_description: Optional[CumulativeMeasureDescription] = None
    filter_spec_set: WhereFilterSpecSet = WhereFilterSpecSet()
    alias: Optional[str] = None
    before_aggregation_time_spine_join_description: Optional[JoinToTimeSpineDescription] = None
    after_aggregation_time_spine_join_description: Optional[JoinToTimeSpineDescription] = None

    @property
    def post_aggregation_spec(self) -> MeasureSpec:
        """Return a MeasureSpec instance representing the post-aggregation spec state for the underlying measure."""
        if self.alias:
            return MeasureSpec(
                element_name=self.alias,
                non_additive_dimension_spec=self.measure_spec.non_additive_dimension_spec,
                fill_nulls_with=self.fill_nulls_with,
            )
        else:
            return MeasureSpec(
                element_name=self.measure_spec.element_name,
                non_additive_dimension_spec=self.measure_spec.non_additive_dimension_spec,
                fill_nulls_with=self.fill_nulls_with,
            )


@dataclass(frozen=True)
class JoinToTimeSpineDescription:
    """Describes how a time spine join should be performed."""

    join_type: SqlJoinType
    offset_window: Optional[MetricTimeWindow]
    offset_to_grain: Optional[TimeGranularity]
