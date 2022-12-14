from typing import Sequence, List

from metricflow.plan_conversion.select_column_gen import SelectColumnSet
from metricflow.specs import (
    InstanceSpecSetTransform,
    InstanceSpecSet,
    ColumnAssociationResolver,
)
from metricflow.sql.sql_exprs import (
    SqlExpressionNode,
    SqlLogicalExpression,
    SqlLogicalOperator,
    SqlComparisonExpression,
    SqlComparison,
    SqlColumnReferenceExpression,
    SqlColumnReference,
    SqlAggregateFunctionExpression,
    SqlFunction,
)
from metricflow.sql.sql_plan import SqlJoinTimeOffset, MetricTimeOffset
from metricflow.sql.sql_plan import SqlSelectColumn
from metricflow.sql.sql_exprs import SqlDateTruncExpression, SqlTimeDeltaExpression


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
        return SqlAggregateFunctionExpression(
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

    This is necessary for cases where 3 or more subqueries will be linked via FULL OUTER JOINs. If that happens,
    the set of join key from subquery 3 must be compared to the join keys from both subquery 1 and subquery 2,
    otherwise duplicate rows might appear in the output.

    For example:

    table1: [('a', 10), ('b', 20)]
    table2: [('a', 100), ('c', 200)]
    table3: [('a', 1000), ('c', 2000)]

    ->

    output without COALESCE: [('a', 10, 100, 1000), ('c', NULL, 200, NULL), ('c', NULL, NULL, 2000)]
    output with COALESCE: [('a', 10, 100, 1000), ('c', NULL, 200, 2000)]

    The latter scenario consolidates the rows keyed by 'c' into a single entry.
    """

    def __init__(  # noqa: D
        self,
        column_association_resolver: ColumnAssociationResolver,
        table_aliases_in_coalesce: Sequence[str],
        table_alias_on_right_equality: str,
        offset: SqlJoinTimeOffset = SqlJoinTimeOffset(),
    ) -> None:
        self._column_association_resolver = column_association_resolver
        self._table_aliases_in_coalesce = table_aliases_in_coalesce
        self._table_alias_on_right_equality = table_alias_on_right_equality
        self._offset = offset

    def _make_equality_expr(
        self, column_alias: str, offset: SqlJoinTimeOffset = SqlJoinTimeOffset()
    ) -> SqlExpressionNode:
        left_expr: SqlExpressionNode = _make_coalesced_expr(self._table_aliases_in_coalesce, column_alias)
        right_expr: SqlExpressionNode = SqlColumnReferenceExpression(
            col_ref=SqlColumnReference(
                table_alias=self._table_alias_on_right_equality,
                column_name=column_alias,
            )
        )
        if offset.left_offset:
            left_expr = self._offset_sql_expr(sql_expr=left_expr, offset=offset.left_offset)
        if offset.right_offset:
            right_expr = self._offset_sql_expr(sql_expr=right_expr, offset=offset.right_offset)
        return SqlComparisonExpression(
            left_expr=left_expr,
            comparison=SqlComparison.EQUALS,
            right_expr=right_expr,
        )

    def _offset_sql_expr(self, sql_expr: SqlExpressionNode, offset: MetricTimeOffset) -> SqlExpressionNode:
        # Should I do some validation here to confirm this column is metric_time?
        if offset.offset_window:
            offset_expr: SqlExpressionNode = SqlTimeDeltaExpression(
                arg=sql_expr, count=offset.offset_window.count, granularity=offset.offset_window.granularity
            )
        elif offset.offset_to_grain_to_date:
            offset_expr = SqlDateTruncExpression(time_granularity=offset.offset_to_grain_to_date, arg=sql_expr)

        return offset_expr

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
        print("join exprs:", equality_exprs)
        if len(equality_exprs) == 1:
            return equality_exprs[0]
        else:
            return SqlLogicalExpression(
                operator=SqlLogicalOperator.AND,
                args=tuple(equality_exprs),
            )


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
                    expr=_make_coalesced_expr(self._table_aliases, column_name),
                    column_alias=column_name,
                )
            )

        for time_dimension_spec in spec_set.time_dimension_specs:
            column_name = self._column_association_resolver.resolve_time_dimension_spec(time_dimension_spec).column_name
            time_dimension_columns.append(
                SqlSelectColumn(
                    expr=_make_coalesced_expr(self._table_aliases, column_name),
                    column_alias=column_name,
                )
            )

        for identifier_spec in spec_set.identifier_specs:
            column_associations = self._column_association_resolver.resolve_identifier_spec(identifier_spec)
            assert len(column_associations) == 1, "Composite identifiers not supported"
            column_name = column_associations[0].column_name

            identifier_columns.append(
                SqlSelectColumn(
                    expr=_make_coalesced_expr(self._table_aliases, column_name),
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
