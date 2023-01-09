from typing import Sequence, List

from metricflow.plan_conversion.select_column_gen import SelectColumnSet
from metricflow.plan_conversion.sql_expression_builders import make_coalesced_expr
from metricflow.specs import (
    InstanceSpecSetTransform,
    InstanceSpecSet,
    ColumnAssociationResolver,
)
from metricflow.sql.sql_plan import SqlSelectColumn


class CreateSelectCoalescedColumnsForLinkableSpecs(InstanceSpecSetTransform[SelectColumnSet]):
    """Create select columns that coalesce columns corresponding to linkable specs.

    e.g.

    dimension_specs = [DimensionSpec(element_name="is_instant")]
    table_aliases = ["a", "b"]

    ->

    COALESCE(a.is_instant, b.is_instant) AS is_instant
    """

    def __init__(  # noqa: D
        self,
        column_association_resolver: ColumnAssociationResolver,
        table_aliases: Sequence[str],
    ) -> None:
        self._column_association_resolver = column_association_resolver
        self._table_aliases = table_aliases

    def transform(self, spec_set: InstanceSpecSet) -> SelectColumnSet:  # noqa: D

        dimension_columns: List[SqlSelectColumn] = []
        time_dimension_columns: List[SqlSelectColumn] = []
        identifier_columns: List[SqlSelectColumn] = []

        for dimension_spec in spec_set.dimension_specs:
            column_name = self._column_association_resolver.resolve_dimension_spec(dimension_spec).column_name
            dimension_columns.append(
                SqlSelectColumn(
                    expr=make_coalesced_expr(self._table_aliases, column_name),
                    column_alias=column_name,
                )
            )

        for time_dimension_spec in spec_set.time_dimension_specs:
            column_name = self._column_association_resolver.resolve_time_dimension_spec(time_dimension_spec).column_name
            time_dimension_columns.append(
                SqlSelectColumn(
                    expr=make_coalesced_expr(self._table_aliases, column_name),
                    column_alias=column_name,
                )
            )

        for identifier_spec in spec_set.identifier_specs:
            column_associations = self._column_association_resolver.resolve_identifier_spec(identifier_spec)
            assert len(column_associations) == 1, "Composite identifiers not supported"
            column_name = column_associations[0].column_name

            identifier_columns.append(
                SqlSelectColumn(
                    expr=make_coalesced_expr(self._table_aliases, column_name),
                    column_alias=column_name,
                )
            )

        return SelectColumnSet(
            dimension_columns=dimension_columns,
            time_dimension_columns=time_dimension_columns,
            identifier_columns=identifier_columns,
        )


class SelectOnlyLinkableSpecs(InstanceSpecSetTransform[InstanceSpecSet]):
    """Removes metrics and measures from the spec set."""

    def transform(self, spec_set: InstanceSpecSet) -> InstanceSpecSet:  # noqa: D
        return InstanceSpecSet(
            metric_specs=(),
            measure_specs=(),
            dimension_specs=spec_set.dimension_specs,
            time_dimension_specs=spec_set.time_dimension_specs,
            identifier_specs=spec_set.identifier_specs,
        )
