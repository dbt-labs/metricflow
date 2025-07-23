from __future__ import annotations

from dataclasses import dataclass
from functools import cached_property
from typing import Tuple

from dbt_semantic_interfaces.dataclass_serialization import SerializableDataclass
from typing_extensions import override

from metricflow_semantics.collection_helpers.merger import Mergeable
from metricflow_semantics.experimental.semantic_graph.attribute_resolution.annotated_spec_linkable_element_set import (
    AnnotatedSpecLinkableElementSet,
)
from metricflow_semantics.specs.instance_spec import LinkableInstanceSpec
from metricflow_semantics.specs.linkable_spec_set import LinkableSpecSet
from metricflow_semantics.specs.spec_set import group_specs_by_type
from metricflow_semantics.sql.sql_bind_parameters import SqlBindParameterSet


@dataclass(frozen=True)
class WhereFilterSpec(Mergeable, SerializableDataclass):
    """Similar to the WhereFilter, but with the where_sql_template rendered and used elements extracted.

    For example:

    WhereFilter(where_sql_template="{{ Dimension('listing__country') }} == 'US'"))

    ->

    WhereFilterSpec(
        where_sql="listing__country == 'US'",
        bind_parameter_set: SqlBindParameters(),
        linkable_specs: (
            DimensionSpec(
                element_name='country',
                entity_links=('listing',),
        ),
        linkable_elements: (
            LinkableDimension(
                semantic_model_origin=SemanticModelReference(semantic_model_name='listings_latest')
                element_name='country',
                ...
            )
        )
    )
    """

    # Debating whether where_sql / bind_parameter_set belongs here. where_sql may become dialect specific if we introduce
    # quoted identifiers later.
    where_sql: str
    bind_parameters: SqlBindParameterSet
    # Temporarily use `AnnotatedSpecLinkableElementSet` to simplify migration. This should be changed to a simpler type
    # as this class needs to be serializable.
    element_set: AnnotatedSpecLinkableElementSet

    @cached_property
    def linkable_spec_set(self) -> LinkableSpecSet:
        """Return the `LinkableSpecSet` of the group-by items referenced in this filter."""
        spec_set = group_specs_by_type(annotated_spec.spec for annotated_spec in self.element_set.annotated_specs)
        return LinkableSpecSet(
            dimension_specs=spec_set.dimension_specs,
            time_dimension_specs=spec_set.time_dimension_specs,
            entity_specs=spec_set.entity_specs,
            group_by_metric_specs=spec_set.group_by_metric_specs,
        )

    @property
    def linkable_specs(self) -> Tuple[LinkableInstanceSpec, ...]:  # noqa: D102
        return self.element_set.specs

    def merge(self, other: WhereFilterSpec) -> WhereFilterSpec:  # noqa: D102
        if self == WhereFilterSpec.empty_instance():
            return other

        if other == WhereFilterSpec.empty_instance():
            return self

        if self == other:
            return self

        return WhereFilterSpec(
            where_sql=f"({self.where_sql}) AND ({other.where_sql})",
            bind_parameters=self.bind_parameters.merge(other.bind_parameters),
            element_set=self.element_set.union(other.element_set),
        )

    @classmethod
    @override
    def empty_instance(cls) -> WhereFilterSpec:
        # Need to revisit making WhereFilterSpec Mergeable as it's current not a collection, and it's odd to return this
        # no-op filter. Use cases would need to check whether a WhereSpec is a no-op before rendering it to avoid an
        # un-necessary WHERE clause. Making WhereFilterSpec map to a WhereFilterIntersection would make this more in
        # line with other cases of Mergeable.
        return WhereFilterSpec(
            where_sql="TRUE",
            bind_parameters=SqlBindParameterSet(),
            element_set=AnnotatedSpecLinkableElementSet(),
        )
