from __future__ import annotations

import logging

from metricflow.model.semantic_manifest_lookup import SemanticManifestLookup
from metricflow.naming.linkable_spec_name import DUNDER, StructuredLinkableSpecName
from metricflow.specs.column_assoc import (
    ColumnAssociation,
    ColumnAssociationResolver,
    SingleColumnCorrelationKey,
)
from metricflow.specs.specs import (
    DimensionSpec,
    EntitySpec,
    InstanceSpec,
    InstanceSpecVisitor,
    MeasureSpec,
    MetadataSpec,
    MetricSpec,
    TimeDimensionSpec,
)

logger = logging.getLogger(__name__)


class DunderColumnAssociationResolverVisitor(InstanceSpecVisitor[ColumnAssociation]):
    """Visitor helper class for DefaultColumnAssociationResolver2."""

    def __init__(self, semantic_manifest_lookup: SemanticManifestLookup) -> None:  # noqa: D
        self._semantic_manifest_lookup = semantic_manifest_lookup

    def visit_metric_spec(self, metric_spec: MetricSpec) -> ColumnAssociation:  # noqa: D
        return ColumnAssociation(
            column_name=metric_spec.element_name,
            single_column_correlation_key=SingleColumnCorrelationKey(),
        )

    def visit_measure_spec(self, measure_spec: MeasureSpec) -> ColumnAssociation:  # noqa: D
        return ColumnAssociation(
            column_name=measure_spec.element_name,
            single_column_correlation_key=SingleColumnCorrelationKey(),
        )

    def visit_dimension_spec(self, dimension_spec: DimensionSpec) -> ColumnAssociation:  # noqa: D
        return ColumnAssociation(
            column_name=StructuredLinkableSpecName(
                entity_link_names=tuple(x.element_name for x in dimension_spec.entity_links),
                element_name=dimension_spec.element_name,
            ).qualified_name,
            single_column_correlation_key=SingleColumnCorrelationKey(),
        )

    def visit_time_dimension_spec(self, time_dimension_spec: TimeDimensionSpec) -> ColumnAssociation:  # noqa: D
        column_name = StructuredLinkableSpecName(
            entity_link_names=tuple(x.element_name for x in time_dimension_spec.entity_links),
            element_name=time_dimension_spec.element_name,
            time_granularity=time_dimension_spec.time_granularity,
            date_part=time_dimension_spec.date_part,
        ).qualified_name

        return ColumnAssociation(
            column_name=column_name
            + (
                f"{DUNDER}{time_dimension_spec.aggregation_state.value.lower()}"
                if time_dimension_spec.aggregation_state
                else ""
            ),
            single_column_correlation_key=SingleColumnCorrelationKey(),
        )

    def visit_entity_spec(self, entity_spec: EntitySpec) -> ColumnAssociation:  # noqa: D
        return ColumnAssociation(
            column_name=StructuredLinkableSpecName(
                entity_link_names=tuple(x.element_name for x in entity_spec.entity_links),
                element_name=entity_spec.element_name,
            ).qualified_name,
            single_column_correlation_key=SingleColumnCorrelationKey(),
        )

    def visit_metadata_spec(self, metadata_spec: MetadataSpec) -> ColumnAssociation:  # noqa: D
        return ColumnAssociation(
            column_name=metadata_spec.element_name,
            single_column_correlation_key=SingleColumnCorrelationKey(),
        )


class DunderColumnAssociationResolver(ColumnAssociationResolver):
    """Uses a double underscore to map specs to column names.

    For example:

    DimensionSpec(element_name='country', entity_links=['listing'])

    ->

    listing__country
    """

    def __init__(self, semantic_manifest_lookup: SemanticManifestLookup) -> None:  # noqa: D
        self._visitor_helper = DunderColumnAssociationResolverVisitor(semantic_manifest_lookup)

    def resolve_spec(self, spec: InstanceSpec) -> ColumnAssociation:  # noqa: D
        return spec.accept(self._visitor_helper)
