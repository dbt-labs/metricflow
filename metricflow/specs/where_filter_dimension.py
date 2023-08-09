from typing import Sequence
from metricflow.specs.column_assoc import ColumnAssociationResolver
from metricflow.specs.query_interface import QueryInterfaceDimension
from dbt_semantic_interfaces.naming.dundered import DunderedNameFormatter

from typing import Sequence, Type

from dbt_semantic_interfaces.call_parameter_sets import (
    DimensionCallParameterSet,
)
from dbt_semantic_interfaces.naming.dundered import DunderedNameFormatter
from dbt_semantic_interfaces.references import (
    DimensionReference,
    EntityReference,
)

from metricflow.specs.query_interface import (
    QueryInterfaceDimension,
)
from metricflow.specs.specs import DimensionSpec
from dbt_semantic_interfaces.call_parameter_sets import FilterCallParameterSets


def get_where_filter_dimension_cls(
    call_parameter_sets: FilterCallParameterSets,
    dimension_specs: DimensionSpec,
    column_association_resolver: ColumnAssociationResolver,
):
    class WhereFilterDimension(QueryInterfaceDimension):
        def __init__(self, name: str, entity_path: Sequence[str] = ()) -> None:  # noqa: D
            structured_name = DunderedNameFormatter.parse_name(name)
            call_parameter_set = DimensionCallParameterSet(
                dimension_reference=DimensionReference(element_name=structured_name.element_name),
                entity_path=(
                    tuple(EntityReference(element_name=arg) for arg in entity_path) + structured_name.entity_links
                ),
            )
            assert call_parameter_set in call_parameter_sets.dimension_call_parameter_sets

            dimension_spec = self._convert_to_dimension_spec(call_parameter_set)
            dimension_specs.append(dimension_spec)
            self.column_name = column_association_resolver.resolve_spec(dimension_spec).column_name

        def grain(self, _grain: str) -> QueryInterfaceDimension:
            """The time granularity."""
            raise NotImplementedError

        def alias(self, _alias: str) -> QueryInterfaceDimension:
            """Renaming the column."""
            raise NotImplementedError

        def _convert_to_dimension_spec(
            self,
            parameter_set: DimensionCallParameterSet,
        ) -> DimensionSpec:  # noqa: D
            return DimensionSpec(
                element_name=parameter_set.dimension_reference.element_name,
                entity_links=parameter_set.entity_path,
            )

        def __str__(self) -> str:  # noqa
            return self.column_name

    return WhereFilterDimension
