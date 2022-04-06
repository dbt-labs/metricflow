from typing import Sequence, List

from metricflow.specs import (
    InstanceSpecSetTransform,
    InstanceSpecSet,
    ColumnAssociationResolver,
    TransformOutputT,
)
from metricflow.sql.sql_exprs import (
    SqlExpressionNode,
    SqlLogicalExpression,
    SqlLogicalOperator,
    SqlComparisonExpression,
    SqlComparison,
    SqlColumnReferenceExpression,
    SqlColumnReference,
    SqlFunctionExpression,
    SqlFunction,
)
from metricflow.sql.sql_plan import SqlSelectColumn


def _make_coalesced_expr(table_aliases: Sequence[str], column_alias: str) -> SqlExpressionNode:
    """Makes a coalesced expression of the given column from the given table aliases.

    e.g.

    table_aliases = ["a", "b"]
    column_alias = "is_instant"

    ->

    COALESCE(a.is_instant, b.is_instant)
    """
    if len(table_aliases) == 1:
        return SqlColumnReferenceExpression(
            col_ref=SqlColumnReference(
                table_alias=table_aliases[0],
                column_name=column_alias,
            )
        )
    else:
        columns_to_coalesce: List[SqlExpressionNode] = []
        for table_alias in table_aliases:
            columns_to_coalesce.append(
                SqlColumnReferenceExpression(
                    col_ref=SqlColumnReference(
                        table_alias=table_alias,
                        column_name=column_alias,
                    )
                )
            )
        return SqlFunctionExpression(
            sql_function=SqlFunction.COALESCE,
            sql_function_args=columns_to_coalesce,
        )


class CreateOnConditionForCombiningMetrics(InstanceSpecSetTransform[SqlExpressionNode]):
    """Creates an expression that can go in the ON condition when coalescing linkables with a FULL OUTER JOIN.

    e.g.

    dimension_specs = ["is_instant", "ds"]
    table_aliases_in_coalesce = ["a", "b"]
    table_alias_on_right_equality = ["c"]

    ->

    COALESCE(a.is_instant, b.is_instant) = c.is_instant
    AND COALESCE(a.ds, b.ds) = c.ds

    """

    def __init__(  # noqa: D
        self,
        column_association_resolver: ColumnAssociationResolver,
        table_aliases_in_coalesce: Sequence[str],
        table_alias_on_right_equality: str,
    ) -> None:
        self._column_association_resolver = column_association_resolver
        self._table_aliases_in_coalesce = table_aliases_in_coalesce
        self._table_alias_on_right_equality = table_alias_on_right_equality

    def _make_equality_expr(self, column_alias: str) -> SqlExpressionNode:
        return SqlComparisonExpression(
            left_expr=_make_coalesced_expr(self._table_aliases_in_coalesce, column_alias),
            comparison=SqlComparison.EQUALS,
            right_expr=SqlColumnReferenceExpression(
                col_ref=SqlColumnReference(
                    table_alias=self._table_alias_on_right_equality,
                    column_name=column_alias,
                )
            ),
        )

    def transform(self, spec_set: InstanceSpecSet) -> SqlExpressionNode:  # noqa: D
        equality_exprs = []

        for dimension_spec in spec_set.dimension_specs:
            equality_exprs.append(
                self._make_equality_expr(
                    column_alias=self._column_association_resolver.resolve_dimension_spec(dimension_spec).column_name
                )
            )

        for time_dimension_spec in spec_set.time_dimension_specs:
            equality_exprs.append(
                self._make_equality_expr(
                    column_alias=self._column_association_resolver.resolve_time_dimension_spec(
                        time_dimension_spec
                    ).column_name
                )
            )

        for identifier_spec in spec_set.identifier_specs:
            column_associations = self._column_association_resolver.resolve_identifier_spec(identifier_spec)
            assert len(column_associations) == 1, "Composite identifiers not supported"
            column_association = column_associations[0]

            equality_exprs.append(self._make_equality_expr(column_alias=column_association.column_name))
        assert len(equality_exprs) > 0

        if len(equality_exprs) == 1:
            return equality_exprs[0]
        else:
            return SqlLogicalExpression(
                operator=SqlLogicalOperator.AND,
                args=tuple(equality_exprs),
            )


class CreateSelectCoalescedColumnsForLinkableSpecs(InstanceSpecSetTransform[Sequence[SqlSelectColumn]]):
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

    def transform(self, spec_set: InstanceSpecSet) -> Sequence[SqlSelectColumn]:  # noqa: D
        select_columns: List[SqlSelectColumn] = []

        for dimension_spec in spec_set.dimension_specs:
            column_name = self._column_association_resolver.resolve_dimension_spec(dimension_spec).column_name
            select_columns.append(
                SqlSelectColumn(
                    expr=_make_coalesced_expr(self._table_aliases, column_name),
                    column_alias=column_name,
                )
            )

        for time_dimension_spec in spec_set.time_dimension_specs:
            column_name = self._column_association_resolver.resolve_time_dimension_spec(time_dimension_spec).column_name
            select_columns.append(
                SqlSelectColumn(
                    expr=_make_coalesced_expr(self._table_aliases, column_name),
                    column_alias=column_name,
                )
            )

        for identifier_spec in spec_set.identifier_specs:
            column_associations = self._column_association_resolver.resolve_identifier_spec(identifier_spec)
            assert len(column_associations) == 1, "Composite identifiers not supported"
            column_name = column_associations[0].column_name

            select_columns.append(
                SqlSelectColumn(
                    expr=_make_coalesced_expr(self._table_aliases, column_name),
                    column_alias=column_name,
                )
            )

        return select_columns


class SelectOnlyLinkableSpecs(InstanceSpecSetTransform[InstanceSpecSet]):
    """Removes metrics and measures from the spec set."""

    def transform(self, spec_set: InstanceSpecSet) -> TransformOutputT:  # noqa: D
        return InstanceSpecSet(
            metric_specs=(),
            measure_specs=(),
            dimension_specs=spec_set.dimension_specs,
            time_dimension_specs=spec_set.time_dimension_specs,
            identifier_specs=spec_set.identifier_specs,
        )
