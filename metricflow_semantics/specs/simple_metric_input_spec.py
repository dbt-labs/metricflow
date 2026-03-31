from __future__ import annotations

import typing
from collections.abc import Sequence
from dataclasses import dataclass
from functools import cached_property
from typing import Optional, Tuple

from dbt_semantic_interfaces.type_enums import TimeGranularity
from metricflow_semantics.model.semantics.simple_metric_input import SimpleMetricInput
from metricflow_semantics.specs.instance_spec import InstanceSpec, InstanceSpecVisitor, LinkableInstanceSpec
from metricflow_semantics.specs.linkable_spec_set import LinkableSpecSet
from metricflow_semantics.specs.time_window import TimeWindow
from metricflow_semantics.specs.where_filter.where_filter_spec import WhereFilterSpec
from metricflow_semantics.sql.sql_join_type import SqlJoinType
from metricflow_semantics.toolkit.collections.ordered_set import FrozenOrderedSet
from metricflow_semantics.toolkit.dataclass_helpers import fast_frozen_dataclass
from metricflow_semantics.toolkit.visitor import VisitorOutputT

if typing.TYPE_CHECKING:
    from metricflow.plan_conversion.node_processor import PredicatePushdownState


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

    # For the filter defined in `simple_metric_input`
    metric_filter_specs: Tuple[WhereFilterSpec, ...]
    # For additional filters that might be needed (e.g. a filter defined in a derived metric or query).
    additional_filter_specs: Tuple[WhereFilterSpec, ...]

    cumulative_description: Optional[CumulativeDescription]
    before_aggregation_time_spine_join_description: Optional[JoinToTimeSpineDescription]
    after_aggregation_time_spine_join_description: Optional[JoinToTimeSpineDescription]

    @cached_property
    def combined_filter_specs(self) -> Sequence[WhereFilterSpec]:  # noqa: D102
        return self.metric_filter_specs + self.additional_filter_specs


@fast_frozen_dataclass()
class SimpleMetricRecipe2:
    """Describes how to build a simple metric but with modifications."""

    simple_metric_input: SimpleMetricInput

    queried_linkable_specs: LinkableSpecSet
    queried_agg_time_dimension_specs: FrozenOrderedSet[LinkableInstanceSpec]
    predicate_pushdown_state: PredicatePushdownState

    pre_aggregation_filter_specs: Tuple[WhereFilterSpec, ...]
    cumulative_description: Optional[CumulativeDescription]
    before_aggregation_time_spine_join_description: Optional[JoinToTimeSpineDescription]
    after_aggregation_time_spine_join_description: Optional[JoinToTimeSpineDescriptionWithFilters]
    # final_filter_specs: Tuple[WhereFilterSpec, ...]

    @cached_property
    def combined_filter_specs(self) -> Sequence[WhereFilterSpec]:  # noqa: D102
        if self.after_aggregation_time_spine_join_description is None:
            return self.pre_aggregation_filter_specs

        return self.pre_aggregation_filter_specs + self.after_aggregation_time_spine_join_description.time_spine_filters


@dataclass(frozen=True)
class JoinToTimeSpineDescription:
    """Describes how a time spine join should be performed."""

    join_type: SqlJoinType
    offset_window: Optional[TimeWindow]
    offset_to_grain: Optional[TimeGranularity]

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


@dataclass(frozen=True)
class JoinToTimeSpineDescriptionWithFilters(JoinToTimeSpineDescription):
    time_spine_filters: Tuple[WhereFilterSpec, ...]
    filter_specs_after_time_spine_Join: Tuple[WhereFilterSpec, ...]