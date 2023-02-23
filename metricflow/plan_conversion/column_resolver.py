import logging
from typing import Optional, Tuple

from dbt.semantic.aggregation_properties import AggregationState
from metricflow.column_assoc import (
    SingleColumnCorrelationKey,
    ColumnAssociation,
    CompositeColumnCorrelationKey,
)
from metricflow.naming.linkable_spec_name import StructuredLinkableSpecName
from metricflow.specs import (
    MetadataSpec,
    MetricSpec,
    MeasureSpec,
    DimensionSpec,
    TimeDimensionSpec,
    IdentifierSpec,
    ColumnAssociationResolver,
)
from metricflow.model.semantic_model import SemanticModel
from dbt.semantic.time import TimeGranularity

logger = logging.getLogger(__name__)


class DefaultColumnAssociationResolver(ColumnAssociationResolver):
    """Implements the ColumnAssociationResolver."""

    def __init__(self, semantic_model: SemanticModel) -> None:  # noqa: D
        self._semantic_model = semantic_model

    def resolve_metric_spec(self, metric_spec: MetricSpec) -> ColumnAssociation:  # noqa: D
        return ColumnAssociation(
            column_name=metric_spec.name,
            single_column_correlation_key=SingleColumnCorrelationKey(),
        )

    def resolve_measure_spec(self, measure_spec: MeasureSpec) -> ColumnAssociation:  # noqa: D
        return ColumnAssociation(
            column_name=measure_spec.name,
            single_column_correlation_key=SingleColumnCorrelationKey(),
        )

    def resolve_dimension_spec(self, dimension_spec: DimensionSpec) -> ColumnAssociation:  # noqa: D
        return ColumnAssociation(
            column_name=StructuredLinkableSpecName(
                identifier_link_names=tuple(x.name for x in dimension_spec.identifier_links),
                name=dimension_spec.name,
            ).qualified_name,
            single_column_correlation_key=SingleColumnCorrelationKey(),
        )

    def resolve_time_dimension_spec(  # noqa: D
        self, time_dimension_spec: TimeDimensionSpec, aggregation_state: Optional[AggregationState] = None
    ) -> ColumnAssociation:
        if time_dimension_spec.time_granularity == TimeGranularity.DAY:
            column_name = StructuredLinkableSpecName(
                identifier_link_names=tuple(x.name for x in time_dimension_spec.identifier_links),
                name=time_dimension_spec.name,
            ).qualified_name
        else:
            column_name = StructuredLinkableSpecName(
                identifier_link_names=tuple(x.name for x in time_dimension_spec.identifier_links),
                name=time_dimension_spec.name,
                time_granularity=time_dimension_spec.time_granularity,
            ).qualified_name

        return ColumnAssociation(
            column_name=column_name + (f"__{aggregation_state.value.lower()}" if aggregation_state else ""),
            single_column_correlation_key=SingleColumnCorrelationKey(),
        )

    def resolve_identifier_spec(self, identifier_spec: IdentifierSpec) -> Tuple[ColumnAssociation, ...]:  # noqa: D
        sub_id_references = []
        for entity in self._semantic_model.user_configured_model.entities:
            for identifier in entity.identifiers:
                if identifier.reference.name == identifier_spec.name:
                    sub_id_references = [sub_id.reference for sub_id in identifier.identifiers]
                    break

        # composite identifier case
        if len(sub_id_references) != 0:
            column_associations: Tuple[ColumnAssociation, ...] = ()
            for sub_id_reference in sub_id_references:
                if sub_id_reference is not None:
                    sub_id_name = f"{identifier_spec.name}___{sub_id_reference.name}"
                    sub_identifier = StructuredLinkableSpecName(
                        identifier_link_names=tuple(x.name for x in identifier_spec.identifier_links),
                        name=sub_id_name,
                    ).qualified_name
                    column_associations += (
                        ColumnAssociation(
                            column_name=sub_identifier,
                            composite_column_correlation_key=CompositeColumnCorrelationKey(
                                sub_identifier=StructuredLinkableSpecName(
                                    identifier_link_names=(),
                                    name=sub_id_name,
                                ).qualified_name
                            ),
                        ),
                    )
            return column_associations

        return (
            ColumnAssociation(
                column_name=StructuredLinkableSpecName(
                    identifier_link_names=tuple(x.name for x in identifier_spec.identifier_links),
                    name=identifier_spec.name,
                ).qualified_name,
                single_column_correlation_key=SingleColumnCorrelationKey(),
            ),
        )

    def resolve_metadata_spec(self, metadata_spec: MetadataSpec) -> ColumnAssociation:  # noqa: D
        return ColumnAssociation(
            column_name=metadata_spec.name,
            single_column_correlation_key=SingleColumnCorrelationKey(),
        )
