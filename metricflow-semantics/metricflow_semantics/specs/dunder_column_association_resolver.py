from __future__ import annotations

from metricflow_semantics.model.semantic_manifest_lookup import SemanticManifestLookup
from metricflow_semantics.naming.linkable_spec_name import DUNDER
from metricflow_semantics.specs.column_assoc import (
    ColumnAssociation,
    ColumnAssociationResolver,
    SingleColumnCorrelationKey,
)
from metricflow_semantics.specs.spec_classes import (
    DimensionSpec,
    EntitySpec,
    GroupByMetricSpec,
    InstanceSpec,
    InstanceSpecVisitor,
    MeasureSpec,
    MetadataSpec,
    MetricSpec,
    TimeDimensionSpec,
)


class DunderColumnAssociationResolver(ColumnAssociationResolver):
    """Uses a double underscore to map specs to column names.

    For example:

    DimensionSpec(element_name='country', entity_links=['listing'])

    ->

    listing__country
    """

    def __init__(self, semantic_manifest_lookup: SemanticManifestLookup) -> None:  # noqa: D107
        self._visitor_helper = DunderColumnAssociationResolverVisitor(semantic_manifest_lookup)

    def resolve_spec(self, spec: InstanceSpec) -> ColumnAssociation:  # noqa: D102
        return spec.accept(self._visitor_helper)


class DunderColumnAssociationResolverVisitor(InstanceSpecVisitor[ColumnAssociation]):
    """Visitor helper class for DefaultColumnAssociationResolver2."""

    def __init__(self, semantic_manifest_lookup: SemanticManifestLookup) -> None:  # noqa: D107
        self._semantic_manifest_lookup = semantic_manifest_lookup

    def visit_metric_spec(self, metric_spec: MetricSpec) -> ColumnAssociation:  # noqa: D102
        return ColumnAssociation(
            column_name=metric_spec.element_name if metric_spec.alias is None else metric_spec.alias,
            single_column_correlation_key=SingleColumnCorrelationKey(),
        )

    def visit_measure_spec(self, measure_spec: MeasureSpec) -> ColumnAssociation:  # noqa: D102
        return ColumnAssociation(
            column_name=measure_spec.element_name,
            single_column_correlation_key=SingleColumnCorrelationKey(),
        )

    def visit_dimension_spec(self, dimension_spec: DimensionSpec) -> ColumnAssociation:  # noqa: D102
        return ColumnAssociation(
            column_name=dimension_spec.qualified_name,
            single_column_correlation_key=SingleColumnCorrelationKey(),
        )

    def visit_time_dimension_spec(self, time_dimension_spec: TimeDimensionSpec) -> ColumnAssociation:  # noqa: D102
        return ColumnAssociation(
            column_name=time_dimension_spec.qualified_name
            + (
                f"{DUNDER}{time_dimension_spec.aggregation_state.value.lower()}"
                if time_dimension_spec.aggregation_state
                else ""
            ),
            single_column_correlation_key=SingleColumnCorrelationKey(),
        )

    def visit_entity_spec(self, entity_spec: EntitySpec) -> ColumnAssociation:  # noqa: D102
        return ColumnAssociation(
            column_name=entity_spec.qualified_name,
            single_column_correlation_key=SingleColumnCorrelationKey(),
        )

    def visit_group_by_metric_spec(self, group_by_metric_spec: GroupByMetricSpec) -> ColumnAssociation:  # noqa: D102
        return ColumnAssociation(
            column_name=group_by_metric_spec.qualified_name,
            single_column_correlation_key=SingleColumnCorrelationKey(),
        )

    def visit_metadata_spec(self, metadata_spec: MetadataSpec) -> ColumnAssociation:  # noqa: D102
        return ColumnAssociation(
            column_name=metadata_spec.qualified_name,
            single_column_correlation_key=SingleColumnCorrelationKey(),
        )
