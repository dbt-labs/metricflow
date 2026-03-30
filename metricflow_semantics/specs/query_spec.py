from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass, field
from typing import Optional, Tuple

from dbt_semantic_interfaces.dataclass_serialization import SerializableDataclass
from dbt_semantic_interfaces.implementations.filters.where_filter import PydanticWhereFilterIntersection
from dbt_semantic_interfaces.protocols import WhereFilterIntersection
from metricflow_semantics.errors.error_classes import MetricFlowInternalError
from metricflow_semantics.filters.time_constraint import TimeRangeConstraint
from metricflow_semantics.query.group_by_item.filter_spec_resolution.filter_spec_lookup import (
    FilterSpecResolutionLookUp,
)
from metricflow_semantics.specs.dimension_spec import DimensionSpec
from metricflow_semantics.specs.entity_spec import EntitySpec
from metricflow_semantics.specs.group_by_metric_spec import GroupByMetricSpec
from metricflow_semantics.specs.instance_spec import InstanceSpec
from metricflow_semantics.specs.linkable_spec_set import LinkableSpecSet
from metricflow_semantics.specs.metric_spec import MetricSpec
from metricflow_semantics.specs.order_by_spec import OrderBySpec
from metricflow_semantics.specs.time_dimension_spec import TimeDimensionSpec
from metricflow_semantics.toolkit.dataclass_helpers import fast_frozen_dataclass
from metricflow_semantics.toolkit.mf_logging.lazy_formattable import LazyFormat


@fast_frozen_dataclass()
class InputSpecOrder:
    """Represents the order of specs in the query input."""

    group_by_item_specs: tuple[InstanceSpec, ...]
    metric_specs: tuple[MetricSpec, ...]

    def __post_init__(self) -> None:  # noqa: D105
        spec_to_count: defaultdict[InstanceSpec, int] = defaultdict(int)

        for spec in self.group_by_item_specs + self.metric_specs:
            spec_to_count[spec] += 1

        duplicate_specs = {spec: count for spec, count in spec_to_count.items() if count > 1}

        if duplicate_specs:
            raise MetricFlowInternalError(
                LazyFormat(
                    "Duplicate specs found in the order",
                    duplicate_specs=duplicate_specs,
                    input_spec_order=self,
                )
            )


@dataclass(frozen=True)
class MetricFlowQuerySpec(SerializableDataclass):
    """Specs needed for running a query."""

    metric_specs: Tuple[MetricSpec, ...] = ()
    dimension_specs: Tuple[DimensionSpec, ...] = ()
    entity_specs: Tuple[EntitySpec, ...] = ()
    time_dimension_specs: Tuple[TimeDimensionSpec, ...] = ()
    group_by_metric_specs: Tuple[GroupByMetricSpec, ...] = ()
    order_by_specs: Tuple[OrderBySpec, ...] = ()
    time_range_constraint: Optional[TimeRangeConstraint] = None
    limit: Optional[int] = None
    filter_intersection: WhereFilterIntersection = field(
        default_factory=lambda: PydanticWhereFilterIntersection(where_filters=[])
    )
    filter_spec_resolution_lookup: FilterSpecResolutionLookUp = FilterSpecResolutionLookUp.empty_instance()
    min_max_only: bool = False
    apply_group_by: bool = True

    # Use the following to track the order in which specs were provided in the query input.
    input_spec_order: InputSpecOrder = field(
        default_factory=lambda: InputSpecOrder(group_by_item_specs=(), metric_specs=())
    )

    @property
    def linkable_specs(self) -> LinkableSpecSet:  # noqa: D102
        return LinkableSpecSet(
            dimension_specs=self.dimension_specs,
            time_dimension_specs=self.time_dimension_specs,
            entity_specs=self.entity_specs,
            group_by_metric_specs=self.group_by_metric_specs,
        )

    def with_time_range_constraint(self, time_range_constraint: Optional[TimeRangeConstraint]) -> MetricFlowQuerySpec:
        """Return a query spec that's the same as self but with a different time_range_constraint."""
        return MetricFlowQuerySpec(
            metric_specs=self.metric_specs,
            dimension_specs=self.dimension_specs,
            entity_specs=self.entity_specs,
            time_dimension_specs=self.time_dimension_specs,
            group_by_metric_specs=self.group_by_metric_specs,
            order_by_specs=self.order_by_specs,
            time_range_constraint=time_range_constraint,
            limit=self.limit,
            filter_intersection=self.filter_intersection,
            filter_spec_resolution_lookup=self.filter_spec_resolution_lookup,
            min_max_only=self.min_max_only,
            apply_group_by=self.apply_group_by,
            input_spec_order=self.input_spec_order,
        )

    def without_aliases(self) -> MetricFlowQuerySpec:
        """Return a query spec that's the same as self but with all aliases removed."""
        return MetricFlowQuerySpec(
            metric_specs=tuple(metric_spec.with_alias(None) for metric_spec in self.metric_specs),
            dimension_specs=tuple(dimension_spec.with_alias(None) for dimension_spec in self.dimension_specs),
            entity_specs=tuple(entity_spec.with_alias(None) for entity_spec in self.entity_specs),
            time_dimension_specs=tuple(
                time_dimension_spec.with_alias(None) for time_dimension_spec in self.time_dimension_specs
            ),
            group_by_metric_specs=tuple(
                group_by_metric_spec.with_alias(None) for group_by_metric_spec in self.group_by_metric_specs
            ),
            order_by_specs=tuple(order_by_spec.with_alias(None) for order_by_spec in self.order_by_specs),
            time_range_constraint=self.time_range_constraint,
            limit=self.limit,
            filter_intersection=self.filter_intersection,
            filter_spec_resolution_lookup=self.filter_spec_resolution_lookup,
            min_max_only=self.min_max_only,
            apply_group_by=self.apply_group_by,
            input_spec_order=InputSpecOrder(
                group_by_item_specs=tuple(spec.with_alias(None) for spec in self.input_spec_order.group_by_item_specs),
                metric_specs=tuple(metric_spec.with_alias(None) for metric_spec in self.input_spec_order.metric_specs),
            ),
        )

    # def with_filters(self, filt) -> MetricFlowQuerySpec:
    #     """Return a query spec that's the same as self but with all aliases removed."""
    #     return MetricFlowQuerySpec(
    #         metric_specs=tuple(metric_spec.with_alias(None) for metric_spec in self.metric_specs),
    #         dimension_specs=tuple(dimension_spec.with_alias(None) for dimension_spec in self.dimension_specs),
    #         entity_specs=tuple(entity_spec.with_alias(None) for entity_spec in self.entity_specs),
    #         time_dimension_specs=tuple(
    #             time_dimension_spec.with_alias(None) for time_dimension_spec in self.time_dimension_specs
    #         ),
    #         group_by_metric_specs=tuple(
    #             group_by_metric_spec.with_alias(None) for group_by_metric_spec in self.group_by_metric_specs
    #         ),
    #         order_by_specs=tuple(order_by_spec.with_alias(None) for order_by_spec in self.order_by_specs),
    #         time_range_constraint=self.time_range_constraint,
    #         limit=self.limit,
    #         filter_intersection=self.filter_intersection,
    #         filter_spec_resolution_lookup=self.filter_spec_resolution_lookup,
    #         min_max_only=self.min_max_only,
    #         apply_group_by=self.apply_group_by,
    #         input_spec_order=InputSpecOrder(
    #             group_by_item_specs=tuple(spec.with_alias(None) for spec in self.input_spec_order.group_by_item_specs),
    #             metric_specs=tuple(metric_spec.with_alias(None) for metric_spec in self.input_spec_order.metric_specs),
    #         ),
    #     )
