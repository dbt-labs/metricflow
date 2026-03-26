from __future__ import annotations

from dataclasses import dataclass
from functools import cached_property
from typing import Tuple

from dbt_semantic_interfaces.dataclass_serialization import SerializableDataclass
from metricflow_semantics.semantic_graph.attribute_resolution.group_by_item_set import (
    GroupByItemSet,
)
from metricflow_semantics.specs.instance_spec import LinkableInstanceSpec
from metricflow_semantics.specs.linkable_spec_set import LinkableSpecSet
from metricflow_semantics.specs.spec_set import InstanceSpecSet
from metricflow_semantics.sql.sql_bind_parameters import SqlBindParameterSet


@dataclass(frozen=True)
class WhereFilterSpec(SerializableDataclass):
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
    element_set: GroupByItemSet

    @cached_property
    def linkable_spec_set(self) -> LinkableSpecSet:
        """Return the `LinkableSpecSet` of the group-by items referenced in this filter."""
        return LinkableSpecSet.create_from_specs(self.element_set.specs)

    @cached_property
    def linkable_specs(self) -> Tuple[LinkableInstanceSpec, ...]:  # noqa: D102
        return self.element_set.specs

    @cached_property
    def instance_spec_set(self) -> InstanceSpecSet:  # noqa: D102
        return InstanceSpecSet.create_from_specs(self.linkable_specs)
