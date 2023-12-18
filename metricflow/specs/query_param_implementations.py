from __future__ import annotations

from dataclasses import dataclass
from typing import Optional

from dbt_semantic_interfaces.protocols import ProtocolHint
from dbt_semantic_interfaces.references import EntityReference
from dbt_semantic_interfaces.type_enums.date_part import DatePart
from dbt_semantic_interfaces.type_enums.time_granularity import TimeGranularity
from typing_extensions import override

from metricflow.naming.linkable_spec_name import StructuredLinkableSpecName
from metricflow.naming.metric_scheme import MetricNamingScheme
from metricflow.naming.object_builder_scheme import ObjectBuilderNamingScheme
from metricflow.protocols.query_parameter import (
    DimensionOrEntityQueryParameter,
    InputOrderByParameter,
    TimeDimensionQueryParameter,
)
from metricflow.protocols.query_parameter import SavedQueryParameter as SavedQueryParameterProtocol
from metricflow.query.resolver_inputs.query_resolver_inputs import (
    ResolverInputForGroupByItem,
    ResolverInputForMetric,
    ResolverInputForOrderByItem,
)
from metricflow.specs.patterns.entity_link_pattern import (
    EntityLinkPattern,
    EntityLinkPatternParameterSet,
    ParameterSetField,
)


@dataclass(frozen=True)
class TimeDimensionParameter(ProtocolHint[TimeDimensionQueryParameter]):
    """Time dimension requested in a query."""

    def _implements_protocol(self) -> TimeDimensionQueryParameter:
        return self

    name: str
    grain: Optional[TimeGranularity] = None
    date_part: Optional[DatePart] = None

    def __post_init__(self) -> None:  # noqa: D
        parsed_name = StructuredLinkableSpecName.from_name(self.name)
        if parsed_name.time_granularity:
            raise ValueError("Must use object syntax for `grain` parameter if `date_part` is requested.")

    @property
    def query_resolver_input(self) -> ResolverInputForGroupByItem:  # noqa: D
        fields_to_compare = [
            ParameterSetField.ELEMENT_NAME,
            ParameterSetField.ENTITY_LINKS,
            ParameterSetField.DATE_PART,
        ]
        if self.grain is not None:
            fields_to_compare.append(ParameterSetField.TIME_GRANULARITY)

        name_structure = StructuredLinkableSpecName.from_name(self.name.lower())

        return ResolverInputForGroupByItem(
            input_obj=self,
            input_obj_naming_scheme=ObjectBuilderNamingScheme(),
            spec_pattern=EntityLinkPattern(
                EntityLinkPatternParameterSet.from_parameters(
                    fields_to_compare=tuple(fields_to_compare),
                    element_name=name_structure.element_name,
                    entity_links=tuple(EntityReference(link_name) for link_name in name_structure.entity_link_names),
                    time_granularity=self.grain,
                    date_part=self.date_part,
                )
            ),
        )


@dataclass(frozen=True)
class DimensionOrEntityParameter(ProtocolHint[DimensionOrEntityQueryParameter]):
    """Group by parameter requested in a query.

    Might represent an entity or a dimension.
    """

    name: str

    @override
    def _implements_protocol(self) -> DimensionOrEntityQueryParameter:
        return self

    @property
    def query_resolver_input(self) -> ResolverInputForGroupByItem:  # noqa: D
        name_structure = StructuredLinkableSpecName.from_name(self.name.lower())

        return ResolverInputForGroupByItem(
            input_obj=self,
            input_obj_naming_scheme=ObjectBuilderNamingScheme(),
            spec_pattern=EntityLinkPattern(
                EntityLinkPatternParameterSet.from_parameters(
                    fields_to_compare=(
                        ParameterSetField.ELEMENT_NAME,
                        ParameterSetField.ENTITY_LINKS,
                        ParameterSetField.DATE_PART,
                    ),
                    element_name=name_structure.element_name,
                    entity_links=tuple(EntityReference(link_name) for link_name in name_structure.entity_link_names),
                    time_granularity=None,
                    date_part=None,
                )
            ),
        )


@dataclass(frozen=True)
class MetricParameter:
    """Metric requested in a query."""

    name: str

    @property
    def query_resolver_input(self) -> ResolverInputForMetric:  # noqa: D
        naming_scheme = MetricNamingScheme()
        return ResolverInputForMetric(
            input_obj=self,
            naming_scheme=naming_scheme,
            spec_pattern=naming_scheme.spec_pattern(self.name),
        )


@dataclass(frozen=True)
class OrderByParameter:
    """Order by requested in a query."""

    order_by: InputOrderByParameter
    descending: bool = False

    @property
    def query_resolver_input(self) -> ResolverInputForOrderByItem:  # noqa: D
        return ResolverInputForOrderByItem(
            input_obj=self,
            possible_inputs=(self.order_by.query_resolver_input,),
            descending=self.descending,
        )


@dataclass(frozen=True)
class SavedQueryParameter(ProtocolHint[SavedQueryParameterProtocol]):
    """Dataclass implementation of SavedQueryParameterProtocol."""

    name: str

    @override
    def _implements_protocol(self) -> SavedQueryParameterProtocol:
        return self
