from __future__ import annotations

from dataclasses import dataclass
from typing import Optional, Union

from dbt_semantic_interfaces.protocols import ProtocolHint
from dbt_semantic_interfaces.references import EntityReference
from dbt_semantic_interfaces.type_enums.date_part import DatePart
from typing_extensions import override

from metricflow_semantics.model.semantic_manifest_lookup import SemanticManifestLookup
from metricflow_semantics.naming.linkable_spec_name import StructuredLinkableSpecName
from metricflow_semantics.naming.metric_scheme import MetricNamingScheme
from metricflow_semantics.naming.object_builder_scheme import ObjectBuilderNamingScheme
from metricflow_semantics.protocols.query_parameter import (
    DimensionOrEntityQueryParameter,
    MetricQueryParameter,
    OrderByQueryParameter,
    TimeDimensionQueryParameter,
)
from metricflow_semantics.protocols.query_parameter import SavedQueryParameter as SavedQueryParameterProtocol
from metricflow_semantics.query.resolver_inputs.query_resolver_inputs import (
    ResolverInputForGroupByItem,
    ResolverInputForMetric,
    ResolverInputForOrderByItem,
)
from metricflow_semantics.specs.patterns.entity_link_pattern import (
    EntityLinkPattern,
    ParameterSetField,
    SpecPatternParameterSet,
)
from metricflow_semantics.specs.patterns.typed_patterns import TimeDimensionPattern


@dataclass(frozen=True)
class TimeDimensionParameter(ProtocolHint[TimeDimensionQueryParameter]):
    """Time dimension requested in a query."""

    def _implements_protocol(self) -> TimeDimensionQueryParameter:
        return self

    name: str
    grain: Optional[str] = None
    date_part: Optional[DatePart] = None
    alias: Optional[str] = None

    def query_resolver_input(  # noqa: D102
        self,
        semantic_manifest_lookup: SemanticManifestLookup,
    ) -> ResolverInputForGroupByItem:
        name_structure = StructuredLinkableSpecName.from_name(
            qualified_name=self.name.lower(),
            custom_granularity_names=semantic_manifest_lookup.semantic_model_lookup.custom_granularity_names,
        )

        return ResolverInputForGroupByItem(
            input_obj=self,
            input_obj_naming_scheme=ObjectBuilderNamingScheme(),
            spec_pattern=EntityLinkPattern(
                SpecPatternParameterSet.from_parameters(
                    fields_to_compare=TimeDimensionPattern.get_fields_to_compare(
                        time_granularity_name=self.grain, date_part=self.date_part
                    ),
                    element_name=name_structure.element_name,
                    entity_links=tuple(EntityReference(link_name) for link_name in name_structure.entity_link_names),
                    time_granularity_name=self.grain,
                    date_part=self.date_part,
                )
            ),
            alias=self.alias,
        )

    def with_alias(self, alias: Optional[str]) -> TimeDimensionParameter:
        """Returns a new TimeDimensionParameter with the alias replaced."""
        return TimeDimensionParameter(name=self.name, grain=self.grain, date_part=self.date_part, alias=alias)


@dataclass(frozen=True)
class DimensionOrEntityParameter(ProtocolHint[DimensionOrEntityQueryParameter]):
    """Group by parameter requested in a query.

    Might represent an entity or a dimension.
    """

    name: str
    alias: Optional[str] = None

    @override
    def _implements_protocol(self) -> DimensionOrEntityQueryParameter:
        return self

    def query_resolver_input(self, semantic_manifest_lookup: SemanticManifestLookup) -> ResolverInputForGroupByItem:
        """Produces resolver input from a query parameter representing a dimension or entity.

        Note these parameters do not currently have a direct need for the semantic_manifest_lookup, but since these
        can be lumped in with other items that do require it we keep this method signature consistent across
        the class sets.

        TODO: Refine these query input classes so that this kind of thing is either enforced in self-documenting
        ways or removed from the codebase
        """
        name_structure = StructuredLinkableSpecName.from_name(
            qualified_name=self.name.lower(),
            custom_granularity_names=semantic_manifest_lookup.semantic_model_lookup.custom_granularity_names,
        )

        return ResolverInputForGroupByItem(
            input_obj=self,
            input_obj_naming_scheme=ObjectBuilderNamingScheme(),
            spec_pattern=EntityLinkPattern(
                SpecPatternParameterSet.from_parameters(
                    fields_to_compare=(
                        ParameterSetField.ELEMENT_NAME,
                        ParameterSetField.ENTITY_LINKS,
                        ParameterSetField.DATE_PART,
                    ),
                    element_name=name_structure.element_name,
                    entity_links=tuple(EntityReference(link_name) for link_name in name_structure.entity_link_names),
                    time_granularity_name=None,
                    date_part=None,
                )
            ),
            alias=self.alias,
        )

    def with_alias(self, alias: Optional[str]) -> DimensionOrEntityParameter:
        """Returns a new DimensionOrEntityParameter with the alias replaced."""
        return DimensionOrEntityParameter(name=self.name, alias=alias)


@dataclass(frozen=True)
class MetricParameter(ProtocolHint[MetricQueryParameter]):
    """Metric requested in a query."""

    name: str
    alias: Optional[str] = None

    @override
    def _implements_protocol(self) -> MetricQueryParameter:
        return self

    def query_resolver_input(  # noqa: D102
        self, semantic_manifest_lookup: SemanticManifestLookup
    ) -> ResolverInputForMetric:
        naming_scheme = MetricNamingScheme()
        return ResolverInputForMetric(
            input_obj=self,
            naming_scheme=naming_scheme,
            spec_pattern=naming_scheme.spec_pattern(self.name, semantic_manifest_lookup=semantic_manifest_lookup),
            alias=self.alias,
        )

    def with_alias(self, alias: Optional[str]) -> MetricParameter:
        """Returns a new MetricParameter with the alias replaced."""
        return MetricParameter(name=self.name, alias=alias)


InputOrderByParameter = Union[MetricParameter, DimensionOrEntityParameter, TimeDimensionParameter]


@dataclass(frozen=True)
class OrderByParameter(ProtocolHint[OrderByQueryParameter]):
    """Order by requested in a query."""

    order_by: InputOrderByParameter
    descending: bool = False

    @override
    def _implements_protocol(self) -> OrderByQueryParameter:
        return self

    def query_resolver_input(  # noqa: D102
        self, semantic_manifest_lookup: SemanticManifestLookup
    ) -> ResolverInputForOrderByItem:
        return ResolverInputForOrderByItem(
            input_obj=self,
            possible_inputs=(self.order_by.query_resolver_input(semantic_manifest_lookup=semantic_manifest_lookup),),
            descending=self.descending,
        )

    def with_alias(self, alias: Optional[str]) -> OrderByParameter:
        """Returns a new OrderByParameter with the alias replaced."""
        return OrderByParameter(order_by=self.order_by.with_alias(alias), descending=self.descending)


@dataclass(frozen=True)
class SavedQueryParameter(ProtocolHint[SavedQueryParameterProtocol]):
    """Dataclass implementation of SavedQueryParameterProtocol."""

    name: str

    @override
    def _implements_protocol(self) -> SavedQueryParameterProtocol:
        return self
