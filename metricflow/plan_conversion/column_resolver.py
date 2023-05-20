import logging
from typing import Optional

from metricflow.aggregation_properties import AggregationState
from metricflow.specs.column_assoc import (
    SingleColumnCorrelationKey,
    ColumnAssociation,
)
from metricflow.naming.linkable_spec_name import StructuredLinkableSpecName
from metricflow.specs.specs import (
    MetadataSpec,
    MetricSpec,
    MeasureSpec,
    DimensionSpec,
    TimeDimensionSpec,
    EntitySpec,
    ColumnAssociationResolver,
)
from metricflow.model.semantic_manifest_lookup import SemanticManifestLookup
from dbt_semantic_interfaces.type_enums.time_granularity import TimeGranularity

logger = logging.getLogger(__name__)


class DefaultColumnAssociationResolver(ColumnAssociationResolver):
    """Implements the ColumnAssociationResolver."""

    def __init__(self, semantic_manifest_lookup: SemanticManifestLookup) -> None:  # noqa: D
        self._semantic_manifest_lookup = semantic_manifest_lookup

    def resolve_metric_spec(self, metric_spec: MetricSpec) -> ColumnAssociation:  # noqa: D
        return ColumnAssociation(
            column_name=metric_spec.element_name,
            single_column_correlation_key=SingleColumnCorrelationKey(),
        )

    def resolve_measure_spec(self, measure_spec: MeasureSpec) -> ColumnAssociation:  # noqa: D
        return ColumnAssociation(
            column_name=measure_spec.element_name,
            single_column_correlation_key=SingleColumnCorrelationKey(),
        )

    def resolve_dimension_spec(self, dimension_spec: DimensionSpec) -> ColumnAssociation:  # noqa: D
        return ColumnAssociation(
            column_name=StructuredLinkableSpecName(
                entity_link_names=tuple(x.element_name for x in dimension_spec.entity_links),
                element_name=dimension_spec.element_name,
            ).qualified_name,
            single_column_correlation_key=SingleColumnCorrelationKey(),
        )

    def resolve_time_dimension_spec(  # noqa: D
        self, time_dimension_spec: TimeDimensionSpec, aggregation_state: Optional[AggregationState] = None
    ) -> ColumnAssociation:
        if time_dimension_spec.time_granularity == TimeGranularity.DAY:
            column_name = StructuredLinkableSpecName(
                entity_link_names=tuple(x.element_name for x in time_dimension_spec.entity_links),
                element_name=time_dimension_spec.element_name,
            ).qualified_name
        else:
            column_name = StructuredLinkableSpecName(
                entity_link_names=tuple(x.element_name for x in time_dimension_spec.entity_links),
                element_name=time_dimension_spec.element_name,
                time_granularity=time_dimension_spec.time_granularity,
            ).qualified_name

        return ColumnAssociation(
            column_name=column_name + (f"__{aggregation_state.value.lower()}" if aggregation_state else ""),
            single_column_correlation_key=SingleColumnCorrelationKey(),
        )

    def resolve_entity_spec(self, entity_spec: EntitySpec) -> ColumnAssociation:  # noqa: D
        return ColumnAssociation(
            column_name=StructuredLinkableSpecName(
                entity_link_names=tuple(x.element_name for x in entity_spec.entity_links),
                element_name=entity_spec.element_name,
            ).qualified_name,
            single_column_correlation_key=SingleColumnCorrelationKey(),
        )

    def resolve_metadata_spec(self, metadata_spec: MetadataSpec) -> ColumnAssociation:  # noqa: D
        return ColumnAssociation(
            column_name=metadata_spec.element_name,
            single_column_correlation_key=SingleColumnCorrelationKey(),
        )
