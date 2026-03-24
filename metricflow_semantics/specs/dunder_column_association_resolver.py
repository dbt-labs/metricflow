from __future__ import annotations

from typing import Optional

from typing_extensions import override

from metricflow_semantics.naming.linkable_spec_name import DUNDER
from metricflow_semantics.specs.column_assoc import (
    ColumnAssociation,
    ColumnAssociationResolver,
)
from metricflow_semantics.specs.dimension_spec import DimensionSpec
from metricflow_semantics.specs.entity_spec import EntitySpec
from metricflow_semantics.specs.group_by_metric_spec import GroupByMetricSpec
from metricflow_semantics.specs.instance_spec import InstanceSpec, InstanceSpecVisitor
from metricflow_semantics.specs.metadata_spec import MetadataSpec
from metricflow_semantics.specs.metric_spec import MetricSpec
from metricflow_semantics.specs.simple_metric_input_spec import SimpleMetricInputSpec
from metricflow_semantics.specs.time_dimension_spec import TimeDimensionSpec


class DunderColumnAssociationResolver(ColumnAssociationResolver):
    """Uses a double underscore to map specs to column names.

    For example:

    DimensionSpec(element_name='country', entity_links=['listing'])

    ->

    listing__country
    """

    def __init__(self, dunder_prefix_simple_metric_inputs: Optional[bool] = None) -> None:  # noqa: D107
        self._visitor_helper = DunderColumnAssociationResolverVisitor(
            dunder_prefix_simple_metric_inputs is None or dunder_prefix_simple_metric_inputs
        )
        self._expose_simple_metric_inputs = dunder_prefix_simple_metric_inputs

    @override
    def resolve_spec(self, spec: InstanceSpec) -> ColumnAssociation:
        return spec.accept(self._visitor_helper)

    @override
    def with_options(self, dunder_prefix_simple_metric_inputs: bool) -> DunderColumnAssociationResolver:
        return DunderColumnAssociationResolver(dunder_prefix_simple_metric_inputs=dunder_prefix_simple_metric_inputs)


class DunderColumnAssociationResolverVisitor(InstanceSpecVisitor[ColumnAssociation]):
    """Visitor helper class for DefaultColumnAssociationResolver."""

    def __init__(self, dunder_prefix_simple_metric_inputs: bool) -> None:  # noqa: D107
        self._dunder_prefix_simple_metric_inputs = dunder_prefix_simple_metric_inputs

    def visit_metric_spec(self, metric_spec: MetricSpec) -> ColumnAssociation:  # noqa: D102
        return ColumnAssociation(metric_spec.alias or metric_spec.element_name)

    def visit_simple_metric_input_spec(self, spec: SimpleMetricInputSpec) -> ColumnAssociation:  # noqa: D102
        if not self._dunder_prefix_simple_metric_inputs:
            return ColumnAssociation(spec.element_name)

        return ColumnAssociation(DUNDER + spec.element_name)

    def visit_dimension_spec(self, dimension_spec: DimensionSpec) -> ColumnAssociation:  # noqa: D102
        return ColumnAssociation(dimension_spec.alias or dimension_spec.dunder_name)

    def visit_time_dimension_spec(self, time_dimension_spec: TimeDimensionSpec) -> ColumnAssociation:  # noqa: D102
        return ColumnAssociation(
            time_dimension_spec.alias
            or (
                time_dimension_spec.dunder_name
                + (
                    f"{DUNDER}{time_dimension_spec.aggregation_state.value.lower()}"
                    if time_dimension_spec.aggregation_state
                    else ""
                )
                + (
                    f"{DUNDER}{DUNDER.join([window_function.value.lower() for window_function in time_dimension_spec.window_functions])}"
                    if time_dimension_spec.window_functions
                    else ""
                )
            )
        )

    def visit_entity_spec(self, entity_spec: EntitySpec) -> ColumnAssociation:  # noqa: D102
        return ColumnAssociation(entity_spec.alias or entity_spec.dunder_name)

    def visit_group_by_metric_spec(self, group_by_metric_spec: GroupByMetricSpec) -> ColumnAssociation:  # noqa: D102
        return ColumnAssociation(group_by_metric_spec.dunder_name)

    def visit_metadata_spec(self, metadata_spec: MetadataSpec) -> ColumnAssociation:  # noqa: D102
        return ColumnAssociation(metadata_spec.dunder_name)
