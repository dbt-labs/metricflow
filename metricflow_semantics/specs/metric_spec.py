from __future__ import annotations

from collections.abc import Iterable
from dataclasses import dataclass
from functools import cached_property
from typing import Optional, Tuple

from dbt_semantic_interfaces.protocols import MetricInput
from dbt_semantic_interfaces.references import MetricReference
from dbt_semantic_interfaces.type_enums import TimeGranularity
from metricflow_semantics.query.group_by_item.filter_spec_resolution.filter_location import WhereFilterLocation
from metricflow_semantics.specs.instance_spec import InstanceSpec, InstanceSpecVisitor
from metricflow_semantics.specs.time_window import TimeWindow
from metricflow_semantics.specs.where_filter.where_filter_spec import WhereFilterSpec
from metricflow_semantics.specs.where_filter.where_filter_spec_factory import WhereFilterSpecFactory
from metricflow_semantics.toolkit.dataclass_helpers import fast_frozen_dataclass
from metricflow_semantics.toolkit.visitor import VisitorOutputT


@dataclass(frozen=True, order=True)
class MetricSpec(InstanceSpec):  # noqa: D101
    # Time-over-time could go here
    element_name: str
    where_filter_specs: Tuple[WhereFilterSpec, ...]
    alias: Optional[str]
    offset_window: Optional[TimeWindow]
    offset_to_grain: Optional[TimeGranularity]

    @staticmethod
    def create(  # noqa: D102
        element_name: str,
        where_filter_specs: Iterable[WhereFilterSpec] = (),
        alias: Optional[str] = None,
        offset_window: Optional[TimeWindow] = None,
        offset_to_grain: Optional[TimeGranularity] = None,
    ) -> MetricSpec:
        return MetricSpec(
            element_name=element_name,
            where_filter_specs=tuple(where_filter_specs),
            alias=alias,
            offset_window=offset_window,
            offset_to_grain=offset_to_grain,
        )

    @staticmethod
    def from_element_name(element_name: str) -> MetricSpec:  # noqa: D102
        return MetricSpec.create(element_name=element_name)

    @staticmethod
    def create_from_input_metric(  # noqa: D102
        metric_input: MetricInput,
        filter_spec_factory: WhereFilterSpecFactory,
        additional_filter_specs: Optional[Iterable[WhereFilterSpec]] = None,
    ) -> MetricSpec:
        filter_specs: list[WhereFilterSpec] = []
        if metric_input.filter is not None:
            filter_specs.extend(
                filter_spec_factory.create_from_where_filter_intersection(
                    filter_location=WhereFilterLocation.for_input_metric(
                        metric_input.as_reference,
                    ),
                    filter_intersection=metric_input.filter,
                )
            )
        if additional_filter_specs:
            filter_specs.extend(additional_filter_specs)

        return MetricSpec(
            element_name=metric_input.name,
            where_filter_specs=tuple(filter_specs),
            alias=metric_input.alias,
            offset_window=TimeWindow(
                count=metric_input.offset_window.count, granularity=metric_input.offset_window.granularity
            )
            if metric_input.offset_window is not None
            else None,
            offset_to_grain=TimeGranularity(metric_input.offset_to_grain)
            if metric_input.offset_to_grain is not None
            else None,
        )

    @property
    def dunder_name(self) -> str:  # noqa: D102
        return self.element_name

    @staticmethod
    def from_reference(reference: MetricReference) -> MetricSpec:
        """Initialize from a metric reference instance."""
        return MetricSpec.create(element_name=reference.element_name)

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
        return MetricSpec.create(
            element_name=self.element_name, where_filter_specs=self.where_filter_specs, alias=self.alias
        )

    def with_alias(self, alias: Optional[str]) -> MetricSpec:
        """Add the alias to the metric spec."""
        return MetricSpec.create(
            element_name=self.element_name,
            where_filter_specs=self.where_filter_specs,
            alias=alias,
            offset_window=self.offset_window,
            offset_to_grain=self.offset_to_grain,
        )

    def without_filter_specs(self) -> MetricSpec:  # noqa: D102
        return MetricSpec.create(
            element_name=self.element_name,
            alias=self.alias,
            offset_window=self.offset_window,
            offset_to_grain=self.offset_to_grain,
        )

    @property
    def standard_offset_window(self) -> Optional[TimeWindow]:
        """Return the offset window if it exists and uses a standard granularity."""
        if self.offset_window and self.offset_window.is_standard_granularity:
            return self.offset_window
        return None

    @property
    def custom_offset_window(self) -> Optional[TimeWindow]:
        """Return the offset window if it exists and uses a custom granularity."""
        if self.offset_window and not self.offset_window.is_standard_granularity:
            return self.offset_window
        return None

    def with_where_filter_specs(self, where_filter_specs: Iterable[WhereFilterSpec]) -> MetricSpec:  # noqa: D102
        return MetricSpec(
            element_name=self.element_name,
            where_filter_specs=tuple(where_filter_specs),
            alias=self.alias,
            offset_window=self.offset_window,
            offset_to_grain=self.offset_to_grain,
        )

    @cached_property
    def metric_modifier(self) -> MetricModifier:  # noqa: D102
        return MetricModifier(
            where_filter_specs=self.where_filter_specs,
            alias=self.alias,
            offset_window=self.offset_window,
            offset_to_grain=self.offset_to_grain,
        )


@fast_frozen_dataclass()
class MetricModifier:
    """Describes how a metric should be modified.

    This is used to help group metrics that can be consolidated into a single query. e.g. metrics that have the same
    filter.
    """

    where_filter_specs: tuple[WhereFilterSpec, ...]
    alias: Optional[str]
    offset_window: Optional[TimeWindow]
    offset_to_grain: Optional[TimeGranularity]
