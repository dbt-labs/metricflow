from __future__ import annotations

from dataclasses import dataclass
from typing import Tuple

from dbt_semantic_interfaces.dataclass_serialization import SerializableDataclass
from typing_extensions import override

from metricflow_semantics.collection_helpers.dedupe import ordered_dedupe
from metricflow_semantics.collection_helpers.merger import Mergeable
from metricflow_semantics.model.semantics.linkable_element import LinkableElement
from metricflow_semantics.specs.instance_spec import LinkableInstanceSpec
from metricflow_semantics.specs.linkable_spec_set import LinkableSpecSet
from metricflow_semantics.sql.sql_bind_parameters import SqlBindParameters


@dataclass(frozen=True)
class WhereFilterSpec(Mergeable, SerializableDataclass):
    """Similar to the WhereFilter, but with the where_sql_template rendered and used elements extracted.

    For example:

    WhereFilter(where_sql_template="{{ Dimension('listing__country') }} == 'US'"))

    ->

    WhereFilterSpec(
        where_sql="listing__country == 'US'",
        bind_parameters: SqlBindParameters(),
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

    # Debating whether where_sql / bind_parameters belongs here. where_sql may become dialect specific if we introduce
    # quoted identifiers later.
    where_sql: str
    bind_parameters: SqlBindParameters
    linkable_elements: Tuple[LinkableElement, ...]
    linkable_spec_set: LinkableSpecSet

    @property
    def linkable_specs(self) -> Tuple[LinkableInstanceSpec, ...]:  # noqa: D102
        return self.linkable_spec_set.as_tuple

    def merge(self, other: WhereFilterSpec) -> WhereFilterSpec:  # noqa: D102
        if self == WhereFilterSpec.empty_instance():
            return other

        if other == WhereFilterSpec.empty_instance():
            return self

        if self == other:
            return self

        return WhereFilterSpec(
            where_sql=f"({self.where_sql}) AND ({other.where_sql})",
            bind_parameters=self.bind_parameters.combine(other.bind_parameters),
            linkable_spec_set=self.linkable_spec_set.merge(other.linkable_spec_set).dedupe(),
            linkable_elements=ordered_dedupe(self.linkable_elements, other.linkable_elements),
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
            bind_parameters=SqlBindParameters(),
            linkable_spec_set=LinkableSpecSet(),
            linkable_elements=(),
        )
