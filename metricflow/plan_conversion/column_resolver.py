import logging
from typing import Tuple

from metricflow.column_assoc import (
    SingleColumnCorrelationKey,
    ColumnAssociation,
    CompositeColumnCorrelationKey,
)
from metricflow.naming.linkable_spec_name import StructuredLinkableSpecName
from metricflow.specs import (
    MetricSpec,
    MeasureSpec,
    DimensionSpec,
    TimeDimensionSpec,
    IdentifierSpec,
    ColumnAssociationResolver,
)
from metricflow.model.semantic_model import SemanticModel
from metricflow.time.time_granularity import TimeGranularity

logger = logging.getLogger(__name__)


class DefaultColumnAssociationResolver(ColumnAssociationResolver):
    """Implements the ColumnAssociationResolver."""

    def __init__(self, semantic_model: SemanticModel) -> None:  # noqa: D
        self._semantic_model = semantic_model

    def resolve_metric_spec(self, metric_spec: MetricSpec) -> ColumnAssociation:  # noqa: D
        return ColumnAssociation(
            column_name=metric_spec.element_name,
            column_correlation_key=SingleColumnCorrelationKey(),
        )

    def resolve_measure_spec(self, measure_spec: MeasureSpec) -> ColumnAssociation:  # noqa: D
        return ColumnAssociation(
            column_name=measure_spec.element_name,
            column_correlation_key=SingleColumnCorrelationKey(),
        )

    def resolve_dimension_spec(self, dimension_spec: DimensionSpec) -> ColumnAssociation:  # noqa: D
        return ColumnAssociation(
            column_name=StructuredLinkableSpecName(
                identifier_link_names=tuple(x.element_name for x in dimension_spec.identifier_links),
                element_name=dimension_spec.element_name,
            ).qualified_name,
            column_correlation_key=SingleColumnCorrelationKey(),
        )

    def resolve_time_dimension_spec(self, time_dimension_spec: TimeDimensionSpec) -> ColumnAssociation:  # noqa: D
        if time_dimension_spec.time_granularity == TimeGranularity.DAY:
            column_name = StructuredLinkableSpecName(
                identifier_link_names=tuple(x.element_name for x in time_dimension_spec.identifier_links),
                element_name=time_dimension_spec.element_name,
            ).qualified_name
        else:
            column_name = StructuredLinkableSpecName(
                identifier_link_names=tuple(x.element_name for x in time_dimension_spec.identifier_links),
                element_name=time_dimension_spec.element_name,
                time_granularity=time_dimension_spec.time_granularity,
            ).qualified_name

        return ColumnAssociation(
            column_name=column_name,
            column_correlation_key=SingleColumnCorrelationKey(),
        )

    def resolve_identifier_spec(self, identifier_spec: IdentifierSpec) -> Tuple[ColumnAssociation, ...]:  # noqa: D
        sub_id_specs = []
        for data_source in self._semantic_model.user_configured_model.data_sources:
            for identifier in data_source.identifiers:
                if identifier.name.element_name == identifier_spec.element_name:
                    sub_id_specs = [sub_id.name for sub_id in identifier.identifiers]
                    break

        # composite identifier case
        if len(sub_id_specs) != 0:
            column_associations: Tuple[ColumnAssociation, ...] = ()
            for sub_id_spec in sub_id_specs:
                if sub_id_spec is not None:
                    sub_id_name = f"{identifier_spec.element_name}___{sub_id_spec.element_name}"
                    sub_identifier = StructuredLinkableSpecName(
                        identifier_link_names=tuple(x.element_name for x in identifier_spec.identifier_links),
                        element_name=sub_id_name,
                    ).qualified_name
                    column_associations += (
                        ColumnAssociation(
                            column_name=sub_identifier,
                            column_correlation_key=CompositeColumnCorrelationKey(
                                sub_identifier=StructuredLinkableSpecName(
                                    identifier_link_names=(),
                                    element_name=sub_id_name,
                                ).qualified_name
                            ),
                        ),
                    )
            return column_associations

        return (
            ColumnAssociation(
                column_name=StructuredLinkableSpecName(
                    identifier_link_names=tuple(x.element_name for x in identifier_spec.identifier_links),
                    element_name=identifier_spec.element_name,
                ).qualified_name,
                column_correlation_key=SingleColumnCorrelationKey(),
            ),
        )
