from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass
from functools import cached_property
from typing import Optional, Tuple

from metricflow_semantics.model.semantics.simple_metric_input import SimpleMetricInput
from metricflow_semantics.specs.instance_spec import InstanceSpec, InstanceSpecVisitor
from metricflow_semantics.specs.linkable_spec_set import LinkableSpecSet
from metricflow_semantics.specs.time_dimension_spec import TimeDimensionSpec
from metricflow_semantics.specs.time_window import TimeWindow
from metricflow_semantics.specs.where_filter.where_filter_spec import WhereFilterSpec
from metricflow_semantics.sql.sql_join_type import SqlJoinType
from metricflow_semantics.toolkit.collections.ordered_set import FrozenOrderedSet
from metricflow_semantics.toolkit.dataclass_helpers import fast_frozen_dataclass
from metricflow_semantics.toolkit.visitor import VisitorOutputT

from metricflow_semantic_interfaces.type_enums import TimeGranularity


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

    cumulative_window: Optional[TimeWindow]
    cumulative_grain_to_date: Optional[TimeGranularity]


@fast_frozen_dataclass()
class SimpleMetricRecipe:
    """Describes how to build a simple metric but with modifications."""

    simple_metric_input: SimpleMetricInput

    # The specs group-by items in the query
    queried_linkable_specs: LinkableSpecSet
    # Of the above, the ones that are aggregation time dimensions for the metric.
    queried_agg_time_dimension_specs: FrozenOrderedSet[TimeDimensionSpec]

    # Filters that should be applied before aggregation.
    pre_aggregation_filter_specs: Tuple[WhereFilterSpec, ...]
    # Describes the operation for cumulative metrics.
    cumulative_description: Optional[CumulativeDescription]
    # For metrics with a time offset or with `join_to_timespine`, descriptions of how the time-spin join should
    # be applied.
    before_aggregation_time_spine_join_description: Optional[JoinToTimeSpineDescription]
    after_aggregation_time_spine_join_description: Optional[JoinToTimeSpineDescription]

    # Filters intentionally deferred from pre-aggregation application. They are applied after aggregation and after
    # an optional post-aggregation time-spine join.
    deferred_filter_specs: Tuple[WhereFilterSpec, ...]

    @cached_property
    def combined_filter_specs(self) -> Sequence[WhereFilterSpec]:
        """All filters that are referenced in the recipe."""
        combined_filter_specs = list(self.pre_aggregation_filter_specs)

        for time_spine_join_description in (
            self.before_aggregation_time_spine_join_description,
            self.after_aggregation_time_spine_join_description,
        ):
            if time_spine_join_description is not None:
                combined_filter_specs.extend(time_spine_join_description.time_spine_filter_specs)

        combined_filter_specs.extend(self.deferred_filter_specs)
        return tuple(combined_filter_specs)


@dataclass(frozen=True)
class JoinToTimeSpineDescription:
    """Describes how a time spine join should be performed."""

    join_type: SqlJoinType
    offset_window: Optional[TimeWindow]
    offset_to_grain: Optional[TimeGranularity]
    # Filters that should apply to the time spine.
    time_spine_filter_specs: Tuple[WhereFilterSpec, ...] = ()

    @property
    def standard_offset_window(self) -> Optional[TimeWindow]:
        """Return the offset window if it uses a standard granularity."""
        if self.offset_window and self.offset_window.is_standard_granularity:
            return self.offset_window
        return None

    @property
    def custom_offset_window(self) -> Optional[TimeWindow]:
        """Return the offset window if it uses a custom granularity."""
        if self.offset_window and not self.offset_window.is_standard_granularity:
            return self.offset_window
        return None

    @property
    def uses_offset(self) -> bool:
        """Return True if the simple-metric input uses an offset."""
        return self.offset_window is not None or self.offset_to_grain is not None
