from __future__ import annotations

from metricflow_semantics.naming.linkable_spec_name import DUNDER
from metricflow_semantics.specs.column_assoc import (
    ColumnAssociation,
    ColumnAssociationResolver,
)
from metricflow_semantics.specs.dimension_spec import DimensionSpec
from metricflow_semantics.specs.entity_spec import EntitySpec
from metricflow_semantics.specs.group_by_metric_spec import GroupByMetricSpec
from metricflow_semantics.specs.instance_spec import InstanceSpec, InstanceSpecVisitor
from metricflow_semantics.specs.measure_spec import MeasureSpec
from metricflow_semantics.specs.metadata_spec import MetadataSpec
from metricflow_semantics.specs.metric_spec import MetricSpec
from metricflow_semantics.specs.time_dimension_spec import TimeDimensionSpec


class DunderColumnAssociationResolver(ColumnAssociationResolver):
    """Uses a double underscore to map specs to column names.

    For example:

    DimensionSpec(element_name='country', entity_links=['listing'])

    ->

    listing__country
    """

    def __init__(self) -> None:  # noqa: D107
        self._visitor_helper = DunderColumnAssociationResolverVisitor()

    def resolve_spec(self, spec: InstanceSpec) -> ColumnAssociation:  # noqa: D102
        return spec.accept(self._visitor_helper)


class DunderColumnAssociationResolverVisitor(InstanceSpecVisitor[ColumnAssociation]):
    """Visitor helper class for DefaultColumnAssociationResolver2."""

    def visit_metric_spec(self, metric_spec: MetricSpec) -> ColumnAssociation:  # noqa: D102
        return ColumnAssociation(metric_spec.element_name if metric_spec.alias is None else metric_spec.alias)

    def visit_measure_spec(self, measure_spec: MeasureSpec) -> ColumnAssociation:  # noqa: D102
        return ColumnAssociation(measure_spec.element_name)

    def visit_dimension_spec(self, dimension_spec: DimensionSpec) -> ColumnAssociation:  # noqa: D102
        return ColumnAssociation(dimension_spec.qualified_name)

    def visit_time_dimension_spec(self, time_dimension_spec: TimeDimensionSpec) -> ColumnAssociation:  # noqa: D102
        return ColumnAssociation(
            time_dimension_spec.qualified_name
            + (
                f"{DUNDER}{time_dimension_spec.aggregation_state.value.lower()}"
                if time_dimension_spec.aggregation_state
                else ""
            )
            + (
                f"{DUNDER}{time_dimension_spec.window_function.value.lower()}"
                if time_dimension_spec.window_function
                else ""
            )
        )

    def visit_entity_spec(self, entity_spec: EntitySpec) -> ColumnAssociation:  # noqa: D102
        return ColumnAssociation(entity_spec.qualified_name)

    def visit_group_by_metric_spec(self, group_by_metric_spec: GroupByMetricSpec) -> ColumnAssociation:  # noqa: D102
        return ColumnAssociation(group_by_metric_spec.qualified_name)

    def visit_metadata_spec(self, metadata_spec: MetadataSpec) -> ColumnAssociation:  # noqa: D102
        return ColumnAssociation(metadata_spec.qualified_name)
