"""Nodes used in defining SQL expressions."""

from __future__ import annotations

import itertools
from abc import ABC, abstractmethod
from dataclasses import dataclass
from enum import Enum
from typing import Dict, Generic, List, Mapping, Optional, Sequence, Tuple

import more_itertools
from dbt_semantic_interfaces.enum_extension import assert_values_exhausted
from dbt_semantic_interfaces.protocols.measure import MeasureAggregationParameters
from dbt_semantic_interfaces.type_enums.aggregation_type import AggregationType
from dbt_semantic_interfaces.type_enums.time_granularity import TimeGranularity

from metricflow.dag.id_generation import (
    SQL_EXPR_BETWEEN_PREFIX,
    SQL_EXPR_CAST_TO_TIMESTAMP_PREFIX,
    SQL_EXPR_COLUMN_REFERENCE_ID_PREFIX,
    SQL_EXPR_COMPARISON_ID_PREFIX,
    SQL_EXPR_DATE_TRUNC,
    SQL_EXPR_EXTRACT,
    SQL_EXPR_FUNCTION_ID_PREFIX,
    SQL_EXPR_GENERATE_UUID_PREFIX,
    SQL_EXPR_IS_NULL_PREFIX,
    SQL_EXPR_LOGICAL_OPERATOR_PREFIX,
    SQL_EXPR_NULL_PREFIX,
    SQL_EXPR_PERCENTILE_ID_PREFIX,
    SQL_EXPR_RATIO_COMPUTATION,
    SQL_EXPR_STRING_ID_PREFIX,
    SQL_EXPR_STRING_LITERAL_PREFIX,
    SQL_EXPR_SUBTRACT_TIME_INTERVAL_PREFIX,
    SQL_EXPR_WINDOW_FUNCTION_ID_PREFIX,
)
from metricflow.dag.mf_dag import DagNode, DisplayedProperty, NodeId
from metricflow.sql.sql_bind_parameters import SqlBindParameters
from metricflow.time.date_part import DatePart
from metricflow.visitor import Visitable, VisitorOutputT


class SqlExpressionNode(DagNode, Visitable, ABC):
    """An SQL expression like my_table.my_column, CONCAT(a, b) or 1 + 1 that evaluates to a value."""

    def __init__(self, node_id: NodeId, parent_nodes: List[SqlExpressionNode]) -> None:  # noqa: D
        self._parent_nodes = parent_nodes
        super().__init__(node_id=node_id)

    @property
    @abstractmethod
    def requires_parenthesis(self) -> bool:
        """Should expression needs be rendered with parenthesis when rendering inside other expressions.

        Useful for string expressions where we can't infer the structure. For example, in rendering

        SqlMathExpression(operator="*", left_expr=SqlStringExpression("a"), right_expr=SqlStringExpression("b + c")

        this can be used to differentiate between

        a * b + c vs. a * (b + c)
        """
        pass

    @abstractmethod
    def accept(self, visitor: SqlExpressionNodeVisitor[VisitorOutputT]) -> VisitorOutputT:
        """Called when a visitor needs to visit this node."""
        pass

    @property
    def bind_parameters(self) -> SqlBindParameters:
        """Execution parameters when running a query containing this expression.

        * See: https://docs.sqlalchemy.org/en/14/core/tutorial.html#using-textual-sql
        * Generally only defined for string expressions.
        """
        return SqlBindParameters()

    @property
    def parent_nodes(self) -> Sequence[SqlExpressionNode]:  # noqa: D
        return self._parent_nodes

    @property
    def as_column_reference_expression(self) -> Optional[SqlColumnReferenceExpression]:
        """If this is a column reference expression, return self."""
        return None

    @property
    def as_string_expression(self) -> Optional[SqlStringExpression]:
        """If this is a string expression, return self."""
        return None

    @abstractmethod
    def rewrite(
        self,
        column_replacements: Optional[SqlColumnReplacements] = None,
        should_render_table_alias: Optional[bool] = None,
    ) -> SqlExpressionNode:
        """Return the same semantic expression but with re-written according to the input.

        Args:
            column_replacements: Replaces column references according to this map.
            should_render_table_alias: Change if table aliases should be rendered for column reference expressions.
        """
        pass

    @property
    @abstractmethod
    def lineage(self) -> SqlExpressionTreeLineage:
        """Returns all nodes in the paths from this node to the root nodes."""
        pass

    def _parents_match(self, other: SqlExpressionNode) -> bool:  # noqa: D
        return all(x == y for x, y in itertools.zip_longest(self.parent_nodes, other.parent_nodes))

    @abstractmethod
    def matches(self, other: SqlExpressionNode) -> bool:
        """Similar to equals - returns true if these expressions are equivalent."""
        pass


@dataclass(frozen=True)
class SqlExpressionTreeLineage:
    """Captures the lineage of an expression node - contains itself and all ancestor nodes."""

    string_exprs: Tuple[SqlStringExpression, ...] = ()
    function_exprs: Tuple[SqlFunctionExpression, ...] = ()
    column_reference_exprs: Tuple[SqlColumnReferenceExpression, ...] = ()
    column_alias_reference_exprs: Tuple[SqlColumnAliasReferenceExpression, ...] = ()
    other_exprs: Tuple[SqlExpressionNode, ...] = ()

    @staticmethod
    def combine(lineages: Sequence[SqlExpressionTreeLineage]) -> SqlExpressionTreeLineage:
        """Combine multiple lineages into one lineage, without de-duping."""
        return SqlExpressionTreeLineage(
            string_exprs=tuple(more_itertools.flatten(tuple(x.string_exprs for x in lineages))),
            function_exprs=tuple(more_itertools.flatten(tuple(x.function_exprs for x in lineages))),
            column_reference_exprs=tuple(more_itertools.flatten(tuple(x.column_reference_exprs for x in lineages))),
            column_alias_reference_exprs=tuple(
                more_itertools.flatten(tuple(x.column_alias_reference_exprs for x in lineages))
            ),
            other_exprs=tuple(more_itertools.flatten(tuple(x.other_exprs for x in lineages))),
        )

    @property
    def contains_string_exprs(self) -> bool:  # noqa: D
        return len(self.string_exprs) > 0

    @property
    def contains_column_alias_exprs(self) -> bool:  # noqa: D
        return len(self.column_alias_reference_exprs) > 0

    @property
    def contains_ambiguous_exprs(self) -> bool:  # noqa: D
        return self.contains_string_exprs or self.contains_column_alias_exprs

    @property
    def contains_aggregate_exprs(self) -> bool:  # noqa: D
        return any(x.is_aggregate_function for x in self.function_exprs)


class SqlColumnReplacements:
    """When re-writing column references in expressions, this storing the mapping."""

    def __init__(self, column_replacements: Dict[SqlColumnReference, SqlExpressionNode]) -> None:  # noqa: D
        self._column_replacements = column_replacements

    def get_replacement(self, column_reference: SqlColumnReference) -> Optional[SqlExpressionNode]:  # noqa: D
        return self._column_replacements.get(column_reference)


class SqlExpressionNodeVisitor(Generic[VisitorOutputT], ABC):
    """A visitor to help visit the nodes of an expression.

    See similar visitor DataflowPlanVisitor.
    """

    @abstractmethod
    def visit_string_expr(self, node: SqlStringExpression) -> VisitorOutputT:  # noqa: D
        pass

    @abstractmethod
    def visit_column_reference_expr(self, node: SqlColumnReferenceExpression) -> VisitorOutputT:  # noqa: D
        pass

    @abstractmethod
    def visit_column_alias_reference_expr(self, node: SqlColumnAliasReferenceExpression) -> VisitorOutputT:  # noqa: D
        pass

    @abstractmethod
    def visit_comparison_expr(self, node: SqlComparisonExpression) -> VisitorOutputT:  # noqa: D
        pass

    @abstractmethod
    def visit_function_expr(self, node: SqlAggregateFunctionExpression) -> VisitorOutputT:  # noqa: D
        pass

    @abstractmethod
    def visit_percentile_expr(self, node: SqlPercentileExpression) -> VisitorOutputT:  # noqa: D
        pass

    @abstractmethod
    def visit_null_expr(self, node: SqlNullExpression) -> VisitorOutputT:  # noqa: D
        pass

    @abstractmethod
    def visit_logical_expr(self, node: SqlLogicalExpression) -> VisitorOutputT:  # noqa: D
        pass

    @abstractmethod
    def visit_string_literal_expr(self, node: SqlStringLiteralExpression) -> VisitorOutputT:  # noqa: D
        pass

    @abstractmethod
    def visit_is_null_expr(self, node: SqlIsNullExpression) -> VisitorOutputT:  # noqa: D
        pass

    @abstractmethod
    def visit_cast_to_timestamp_expr(self, node: SqlCastToTimestampExpression) -> VisitorOutputT:  # noqa: D
        pass

    @abstractmethod
    def visit_date_trunc_expr(self, node: SqlDateTruncExpression) -> VisitorOutputT:  # noqa: D
        pass

    @abstractmethod
    def visit_extract_expr(self, node: SqlExtractExpression) -> VisitorOutputT:  # noqa: D
        pass

    @abstractmethod
    def visit_time_delta_expr(self, node: SqlSubtractTimeIntervalExpression) -> VisitorOutputT:  # noqa: D
        pass

    @abstractmethod
    def visit_ratio_computation_expr(self, node: SqlRatioComputationExpression) -> VisitorOutputT:  # noqa: D
        pass

    @abstractmethod
    def visit_between_expr(self, node: SqlBetweenExpression) -> VisitorOutputT:  # noqa: D
        pass

    @abstractmethod
    def visit_window_function_expr(self, node: SqlWindowFunctionExpression) -> VisitorOutputT:  # noqa: D
        pass

    @abstractmethod
    def visit_generate_uuid_expr(self, node: SqlGenerateUuidExpression) -> VisitorOutputT:  # noqa: D
        pass


class SqlStringExpression(SqlExpressionNode):
    """An SQL expression in a string format, so it lacks information about the structure.

    These are convenient to use, but because structure is lacking, it can't be easily handled for DB rendering and can
    impede optimizations.
    """

    def __init__(
        self,
        sql_expr: str,
        bind_parameters: Optional[SqlBindParameters] = None,
        requires_parenthesis: bool = True,
        used_columns: Optional[Tuple[str, ...]] = None,
    ) -> None:
        """Constructor.

        Args:
            sql_expr: The SQL in string form.
            bind_parameters: See SqlExpressionNode.bind_parameters
            requires_parenthesis: Whether this should be rendered with () if nested in another expression.
            used_columns: If set, indicates that the expression represented by the string only uses those columns. e.g.
            sql_expr="a + b", used_columns=["a", "b"]. This may be used by optimizers, and if specified, it must be
            complete. e.g. sql_expr="a + b + c", used_columns=["a", "b"] will cause problems.
        """
        self._sql_expr = sql_expr
        self._bind_parameters = bind_parameters or SqlBindParameters()
        self._requires_parenthesis = requires_parenthesis
        self._used_columns = used_columns
        super().__init__(node_id=self.create_unique_id(), parent_nodes=[])

    @classmethod
    def id_prefix(cls) -> str:  # noqa: D
        return SQL_EXPR_STRING_ID_PREFIX

    def accept(self, visitor: SqlExpressionNodeVisitor[VisitorOutputT]) -> VisitorOutputT:  # noqa: D
        return visitor.visit_string_expr(self)

    @property
    def description(self) -> str:  # noqa: D
        return f"String SQL Expression: {self._sql_expr}"

    @property
    def displayed_properties(self) -> List[DisplayedProperty]:  # noqa: D
        return super().displayed_properties + [DisplayedProperty("sql_expr", self._sql_expr)]

    @property
    def sql_expr(self) -> str:  # noqa: D
        return self._sql_expr

    @property
    def requires_parenthesis(self) -> bool:  # noqa: D
        return self._requires_parenthesis

    @property
    def bind_parameters(self) -> SqlBindParameters:  # noqa: D
        return self._bind_parameters

    @property
    def used_columns(self) -> Optional[Tuple[str, ...]]:  # noqa: D
        return self._used_columns

    def __repr__(self) -> str:  # noqa: D
        return f"{self.__class__.__name__}(node_id={self.node_id} sql_expr={self.sql_expr})"

    def rewrite(  # noqa: D
        self,
        column_replacements: Optional[SqlColumnReplacements] = None,
        should_render_table_alias: Optional[bool] = None,
    ) -> SqlExpressionNode:
        if column_replacements:
            raise NotImplementedError()
        return self

    @property
    def lineage(self) -> SqlExpressionTreeLineage:  # noqa: D
        return SqlExpressionTreeLineage(string_exprs=(self,))

    def matches(self, other: SqlExpressionNode) -> bool:  # noqa: D
        if not isinstance(other, SqlStringExpression):
            return False
        return (
            self.sql_expr == other.sql_expr
            and self.used_columns == other.used_columns
            and self.bind_parameters == other.bind_parameters
        )

    @property
    def as_string_expression(self) -> Optional[SqlStringExpression]:
        """If this is a string expression, return self."""
        return self


class SqlStringLiteralExpression(SqlExpressionNode):
    """A string literal like 'foo'. It shouldn't include delimiters as it should be added during rendering."""

    def __init__(self, literal_value: str) -> None:  # noqa: D
        self._literal_value = literal_value
        super().__init__(node_id=self.create_unique_id(), parent_nodes=[])

    @classmethod
    def id_prefix(cls) -> str:  # noqa: D
        return SQL_EXPR_STRING_LITERAL_PREFIX

    def accept(self, visitor: SqlExpressionNodeVisitor[VisitorOutputT]) -> VisitorOutputT:  # noqa: D
        return visitor.visit_string_literal_expr(self)

    @property
    def description(self) -> str:  # noqa: D
        return f"String Literal: {self._literal_value}"

    @property
    def displayed_properties(self) -> List[DisplayedProperty]:  # noqa: D
        return super().displayed_properties + [DisplayedProperty("value", self._literal_value)]

    @property
    def literal_value(self) -> str:  # noqa: D
        return self._literal_value

    @property
    def requires_parenthesis(self) -> bool:  # noqa: D
        return False

    @property
    def bind_parameters(self) -> SqlBindParameters:  # noqa: D
        return SqlBindParameters()

    def __repr__(self) -> str:  # noqa: D
        return f"{self.__class__.__name__}(node_id={self.node_id}, literal_value={self.literal_value})"

    def rewrite(  # noqa: D
        self,
        column_replacements: Optional[SqlColumnReplacements] = None,
        should_render_table_alias: Optional[bool] = None,
    ) -> SqlExpressionNode:
        return self

    @property
    def lineage(self) -> SqlExpressionTreeLineage:  # noqa: D
        return SqlExpressionTreeLineage(other_exprs=(self,))

    def matches(self, other: SqlExpressionNode) -> bool:  # noqa: D
        if not isinstance(other, SqlStringLiteralExpression):
            return False
        return self.literal_value == other.literal_value


@dataclass(frozen=True)
class SqlColumnReference:
    """Used with string expressions to specify what columns are referred to in the string expression."""

    table_alias: str
    column_name: str


class SqlColumnReferenceExpression(SqlExpressionNode):
    """An expression that evaluates to the value of a column in one of the sources in the select query.

    e.g. my_table.my_column
    """

    def __init__(self, col_ref: SqlColumnReference, should_render_table_alias: bool = True) -> None:
        """Constructor.

        Args:
            col_ref: the associated column reference.
            should_render_table_alias: When converting this to SQL text, whether the table alias needed to be included.
            e.g. "foo.bar" vs "bar".
        """
        self._col_ref = col_ref
        self._should_render_table_alias = should_render_table_alias
        super().__init__(node_id=self.create_unique_id(), parent_nodes=[])

    @classmethod
    def id_prefix(cls) -> str:  # noqa: D
        return SQL_EXPR_COLUMN_REFERENCE_ID_PREFIX

    def accept(self, visitor: SqlExpressionNodeVisitor[VisitorOutputT]) -> VisitorOutputT:  # noqa: D
        return visitor.visit_column_reference_expr(self)

    @property
    def col_ref(self) -> SqlColumnReference:  # noqa: D
        return self._col_ref

    @property
    def description(self) -> str:  # noqa: D
        return f"Column: {self.col_ref}"

    @property
    def displayed_properties(self) -> List[DisplayedProperty]:  # noqa: D
        return super().displayed_properties + [DisplayedProperty("col_ref", self.col_ref)]

    @property
    def requires_parenthesis(self) -> bool:  # noqa: D
        return False

    @property
    def as_column_reference_expression(self) -> Optional[SqlColumnReferenceExpression]:  # noqa:
        return self

    def rewrite(  # noqa: D
        self,
        column_replacements: Optional[SqlColumnReplacements] = None,
        should_render_table_alias: Optional[bool] = None,
    ) -> SqlExpressionNode:
        # TODO: Hack to work around the fact our test data set contains "user", which is a reserved keyword.
        # We should migrate "user" -> "user_id" in the test set.
        # This will force "user" to be rendered as "table_alias.user"
        if self.col_ref.column_name == "user":
            should_render_table_alias = True

        if column_replacements:
            replacement = column_replacements.get_replacement(self.col_ref)
            if replacement:
                if should_render_table_alias is not None:
                    return replacement.rewrite(should_render_table_alias=should_render_table_alias)
                else:
                    return replacement
            else:
                if should_render_table_alias is not None:
                    return SqlColumnReferenceExpression(
                        col_ref=self.col_ref, should_render_table_alias=should_render_table_alias
                    )
                return self

        if should_render_table_alias is not None:
            return SqlColumnReferenceExpression(
                col_ref=self.col_ref, should_render_table_alias=should_render_table_alias
            )

        return SqlColumnReferenceExpression(
            col_ref=self.col_ref, should_render_table_alias=self.should_render_table_alias
        )

    @property
    def lineage(self) -> SqlExpressionTreeLineage:  # noqa: D
        return SqlExpressionTreeLineage(column_reference_exprs=(self,))

    @property
    def should_render_table_alias(self) -> bool:  # noqa: D
        return self._should_render_table_alias

    def matches(self, other: SqlExpressionNode) -> bool:  # noqa: D
        if not isinstance(other, SqlColumnReferenceExpression):
            return False
        return self.col_ref == other.col_ref


class SqlColumnAliasReferenceExpression(SqlExpressionNode):
    """An expression that evaluates to the alias of a column, but is not qualified with a table alias.

    e.g. SELECT foo vs. SELECT a.foo.

    This is needed to handle some exceptional cases, but in general, this should not be used as it can lead to
    ambiguities.
    """

    def __init__(self, column_alias: str) -> None:  # noqa: D
        self._column_alias = column_alias
        super().__init__(node_id=self.create_unique_id(), parent_nodes=[])

    @classmethod
    def id_prefix(cls) -> str:  # noqa: D
        return SQL_EXPR_COLUMN_REFERENCE_ID_PREFIX

    def accept(self, visitor: SqlExpressionNodeVisitor[VisitorOutputT]) -> VisitorOutputT:  # noqa: D
        return visitor.visit_column_alias_reference_expr(self)

    @property
    def column_alias(self) -> str:  # noqa: D
        return self._column_alias

    @property
    def description(self) -> str:  # noqa: D
        return f"Unqualified Column: {self._column_alias}"

    @property
    def displayed_properties(self) -> List[DisplayedProperty]:  # noqa: D
        return super().displayed_properties + [DisplayedProperty("column_alias", self.column_alias)]

    @property
    def requires_parenthesis(self) -> bool:  # noqa: D
        return False

    @property
    def as_column_reference_expression(self) -> Optional[SqlColumnReferenceExpression]:  # noqa:
        return None

    def rewrite(  # noqa: D
        self,
        column_replacements: Optional[SqlColumnReplacements] = None,
        should_render_table_alias: Optional[bool] = None,
    ) -> SqlExpressionNode:
        if column_replacements:
            raise NotImplementedError()
        return self

    @property
    def lineage(self) -> SqlExpressionTreeLineage:  # noqa: D
        return SqlExpressionTreeLineage(column_alias_reference_exprs=(self,))

    def matches(self, other: SqlExpressionNode) -> bool:  # noqa: D
        if not isinstance(other, SqlColumnAliasReferenceExpression):
            return False
        return self.column_alias == other.column_alias


class SqlComparison(Enum):  # noqa: D
    LESS_THAN = "<"
    GREATER_THAN = ">"
    LESS_THAN_OR_EQUALS = "<="
    GREATER_THAN_OR_EQUALS = ">="
    EQUALS = "="


class SqlComparisonExpression(SqlExpressionNode):
    """A comparison using >, <, <=, >=, =.

    e.g. my_table.my_column = a + b
    """

    def __init__(self, left_expr: SqlExpressionNode, comparison: SqlComparison, right_expr: SqlExpressionNode) -> None:
        """Constructor.

        Args:
            left_expr: The expression on the left side of the =
            comparison: The comparison to use on expressions
            right_expr: The expression on the right side of the =
        """
        self._left_expr = left_expr
        self._comparison = comparison
        self._right_expr = right_expr
        super().__init__(node_id=self.create_unique_id(), parent_nodes=[self._left_expr, self._right_expr])

    @classmethod
    def id_prefix(cls) -> str:  # noqa: D
        return SQL_EXPR_COMPARISON_ID_PREFIX

    def accept(self, visitor: SqlExpressionNodeVisitor[VisitorOutputT]) -> VisitorOutputT:  # noqa: D
        return visitor.visit_comparison_expr(self)

    @property
    def left_expr(self) -> SqlExpressionNode:  # noqa: D
        return self._left_expr

    @property
    def right_expr(self) -> SqlExpressionNode:  # noqa: D
        return self._right_expr

    @property
    def description(self) -> str:  # noqa: D
        return f"{self._comparison.value} Expression"

    @property
    def displayed_properties(self) -> List[DisplayedProperty]:  # noqa: D
        return super().displayed_properties + [
            DisplayedProperty("left_expr", self.left_expr),
            DisplayedProperty("comparison", self.comparison.value),
            DisplayedProperty("right_expr", self.right_expr),
        ]

    @property
    def requires_parenthesis(self) -> bool:  # noqa: D
        return True

    @property
    def comparison(self) -> SqlComparison:  # noqa: D
        return self._comparison

    def rewrite(  # noqa: D
        self,
        column_replacements: Optional[SqlColumnReplacements] = None,
        should_render_table_alias: Optional[bool] = None,
    ) -> SqlExpressionNode:
        return SqlComparisonExpression(
            left_expr=self.left_expr.rewrite(column_replacements, should_render_table_alias),
            comparison=self.comparison,
            right_expr=self.right_expr.rewrite(column_replacements, should_render_table_alias),
        )

    @property
    def lineage(self) -> SqlExpressionTreeLineage:  # noqa: D
        return SqlExpressionTreeLineage.combine(
            tuple(x.lineage for x in self.parent_nodes) + (SqlExpressionTreeLineage(other_exprs=(self,)),)
        )

    def matches(self, other: SqlExpressionNode) -> bool:  # noqa: D
        if not isinstance(other, SqlComparisonExpression):
            return False
        return self.comparison == other.comparison and self._parents_match(other)


class SqlFunction(Enum):
    """Names of known SQL functions like SUM() in SELECT SUM(...).

    Values are the SQL string to be used in rendering.
    """

    # Aggregation functions
    AVERAGE = "AVG"
    # Most engines implement count_distinct as a leading DISTINCT keyword like `COUNT(DISTINCT col1, col2...)`
    COUNT_DISTINCT = "COUNT"
    MAX = "MAX"
    MIN = "MIN"
    SUM = "SUM"

    # Field management functions
    COALESCE = "COALESCE"
    CONCAT = "CONCAT"

    @staticmethod
    def distinct_aggregation_functions() -> Sequence[SqlFunction]:
        """Returns a tuple containg all currently-supported DISTINCT type aggregation functions.

        This is not a property because properties don't play nicely with static/class methods.
        """
        return (SqlFunction.COUNT_DISTINCT,)

    @staticmethod
    def is_distinct_aggregation(function_type: SqlFunction) -> bool:
        """Convenience method to check if the input function is a distinct aggregation type.

        This is useful in SQL expression rendering, as most engines implement distinct as a keyword modifier on
        an argument (e.g., `COUNT(DISTINCT expr)`) while our model handling and rendering supports distinct functions
        (e.g., `count_distinct(expr)`) and otherwise does not guarantee correct results when the DISTINCT keyword
        is used.
        """
        return function_type in SqlFunction.distinct_aggregation_functions()

    @staticmethod
    def is_aggregation(function_type: SqlFunction) -> bool:
        """Returns true if the given function is an aggregation function."""
        return function_type in (
            SqlFunction.AVERAGE,
            SqlFunction.COUNT_DISTINCT,
            SqlFunction.MAX,
            SqlFunction.MIN,
            SqlFunction.SUM,
        )

    @staticmethod
    def from_aggregation_type(aggregation_type: AggregationType) -> SqlFunction:
        """Converter method to get the SqlFunction value corresponding to the given AggregationType.

        Make sure to leave the else: block in place, as this enforces an exhaustive switch through the
        AggregationType enumeration values.
        """
        if aggregation_type is AggregationType.AVERAGE:
            return SqlFunction.AVERAGE
        elif aggregation_type is AggregationType.COUNT_DISTINCT:
            return SqlFunction.COUNT_DISTINCT
        elif aggregation_type is AggregationType.MAX:
            return SqlFunction.MAX
        elif aggregation_type is AggregationType.MIN:
            return SqlFunction.MIN
        elif aggregation_type is AggregationType.SUM:
            return SqlFunction.SUM
        elif aggregation_type is AggregationType.PERCENTILE:
            raise RuntimeError(
                f"Unhandled aggregation type {aggregation_type} - this should have been handled in percentile"
                "aggregation node."
            )
        elif aggregation_type is AggregationType.MEDIAN:
            raise RuntimeError(
                f"Unhandled aggregation type {aggregation_type} - this should have been transformed to PERCENTILE "
                "during model parsing."
            )
        elif aggregation_type is AggregationType.SUM_BOOLEAN or aggregation_type is AggregationType.COUNT:
            raise RuntimeError(
                f"Unhandled aggregation type {aggregation_type} - this should have been transformed to SUM "
                "during model parsing."
            )
        else:
            assert_values_exhausted(aggregation_type)


class SqlFunctionExpression(SqlExpressionNode):
    """Denotes a function expression in SQL."""

    @property
    @abstractmethod
    def is_aggregate_function(self) -> bool:
        """Returns whether this is an aggregate function."""
        pass

    @staticmethod
    def build_expression_from_aggregation_type(
        aggregation_type: AggregationType,
        sql_column_expression: SqlColumnReferenceExpression,
        agg_params: Optional[MeasureAggregationParameters] = None,
    ) -> SqlFunctionExpression:
        """Returns sql function expression depending on aggregation type."""
        if aggregation_type is AggregationType.PERCENTILE:
            assert agg_params is not None, "Agg_params is none, which should have been caught in validation"
            return SqlPercentileExpression(
                sql_column_expression, SqlPercentileExpressionArgument.from_aggregation_parameters(agg_params)
            )
        else:
            return SqlAggregateFunctionExpression.from_aggregation_type(aggregation_type, sql_column_expression)


class SqlAggregateFunctionExpression(SqlFunctionExpression):
    """An aggregate function expression like SUM(1)."""

    @staticmethod
    def from_aggregation_type(
        aggregation_type: AggregationType, sql_column_expression: SqlColumnReferenceExpression
    ) -> SqlAggregateFunctionExpression:
        """Given the aggregation type, return an SQL function expression that does that aggregation on the given col."""
        return SqlAggregateFunctionExpression(
            sql_function=SqlFunction.from_aggregation_type(aggregation_type=aggregation_type),
            sql_function_args=[sql_column_expression],
        )

    def __init__(self, sql_function: SqlFunction, sql_function_args: List[SqlExpressionNode]) -> None:
        """Constructor.

        Args:
            sql_function: The function that this represents.
            sql_function_args: The arguments that should go into the function. e.g. for "CONCAT(a, b)", the arg
            expressions should be "a" and "b".
        """
        self._sql_function = sql_function
        self._sql_function_args = sql_function_args
        super().__init__(node_id=self.create_unique_id(), parent_nodes=sql_function_args)

    @classmethod
    def id_prefix(cls) -> str:  # noqa: D
        return SQL_EXPR_FUNCTION_ID_PREFIX

    @property
    def requires_parenthesis(self) -> bool:  # noqa: D
        return False

    def accept(self, visitor: SqlExpressionNodeVisitor[VisitorOutputT]) -> VisitorOutputT:  # noqa: D
        return visitor.visit_function_expr(self)

    @property
    def description(self) -> str:  # noqa: D
        return f"{self._sql_function.value} Expression"

    @property
    def displayed_properties(self) -> List[DisplayedProperty]:  # noqa: D
        return (
            super().displayed_properties
            + [DisplayedProperty("function", self.sql_function)]
            + [DisplayedProperty("argument", x) for x in self.sql_function_args]
        )

    @property
    def sql_function(self) -> SqlFunction:  # noqa: D
        return self._sql_function

    @property
    def sql_function_args(self) -> List[SqlExpressionNode]:  # noqa: D
        return self._sql_function_args

    def __repr__(self) -> str:  # noqa: D
        return f"{self.__class__.__name__}(node_id={self.node_id}, sql_function={self.sql_function.name})"

    def rewrite(  # noqa: D
        self,
        column_replacements: Optional[SqlColumnReplacements] = None,
        should_render_table_alias: Optional[bool] = None,
    ) -> SqlExpressionNode:
        return SqlAggregateFunctionExpression(
            sql_function=self.sql_function,
            sql_function_args=[
                x.rewrite(column_replacements, should_render_table_alias) for x in self.sql_function_args
            ],
        )

    @property
    def is_aggregate_function(self) -> bool:  # noqa: D
        return True

    @property
    def lineage(self) -> SqlExpressionTreeLineage:  # noqa: D
        return SqlExpressionTreeLineage.combine(
            tuple(x.lineage for x in self.parent_nodes) + (SqlExpressionTreeLineage(function_exprs=(self,)),)
        )

    def matches(self, other: SqlExpressionNode) -> bool:  # noqa: D
        if not isinstance(other, SqlAggregateFunctionExpression):
            return False
        return self.sql_function == other.sql_function and self._parents_match(other)


class SqlPercentileFunctionType(Enum):
    """Type of percentile function used."""

    DISCRETE = "discrete"
    CONTINUOUS = "continuous"
    APPROXIMATE_DISCRETE = "approximate_discrete"
    APPROXIMATE_CONTINUOUS = "approximate_continuous"


UseDiscretePercentile = bool  # type aliases need to be declared at module level
UseApproximatePercentile = bool


@dataclass(frozen=True)
class SqlPercentileExpressionArgument:
    """Dataclass for holding percentile arguments."""

    percentile: float
    function_type: SqlPercentileFunctionType

    @staticmethod
    def from_aggregation_parameters(agg_params: MeasureAggregationParameters) -> SqlPercentileExpressionArgument:
        """Given the measure parameters, returns a SqlPercentileExpressionArgument with the corresponding percentile args."""
        if not agg_params.percentile:
            raise RuntimeError("Percentile value is none - this should have been caught during model parsing.")

        flags_to_function_type: Mapping[
            Tuple[UseDiscretePercentile, UseApproximatePercentile], SqlPercentileFunctionType
        ] = {
            (False, False): SqlPercentileFunctionType.CONTINUOUS,
            (True, False): SqlPercentileFunctionType.DISCRETE,
            (False, True): SqlPercentileFunctionType.APPROXIMATE_CONTINUOUS,
            (True, True): SqlPercentileFunctionType.APPROXIMATE_DISCRETE,
        }

        percentile_function_type = flags_to_function_type[
            (agg_params.use_discrete_percentile, agg_params.use_approximate_percentile)
        ]

        return SqlPercentileExpressionArgument(
            agg_params.percentile,
            percentile_function_type,
        )


class SqlPercentileExpression(SqlFunctionExpression):
    """A percentile aggregation expression."""

    def __init__(self, order_by_arg: SqlExpressionNode, percentile_args: SqlPercentileExpressionArgument) -> None:
        """Constructor.

        Args:
            order_by_arg: The expression that should go into the function. e.g. for "percentile_cont(col, 0.1)", the arg
            expressions should be "col".
            percentile_args: Auxillary information including percentile value and type.
        """
        self._order_by_arg = order_by_arg
        self._percentile_args = percentile_args

        super().__init__(node_id=self.create_unique_id(), parent_nodes=[order_by_arg])

    @classmethod
    def id_prefix(cls) -> str:  # noqa: D
        return SQL_EXPR_PERCENTILE_ID_PREFIX

    @property
    def requires_parenthesis(self) -> bool:  # noqa: D
        return False

    @property
    def order_by_arg(self) -> SqlExpressionNode:  # noqa: D
        return self._order_by_arg

    @property
    def percentile_args(self) -> SqlPercentileExpressionArgument:  # noqa: D
        return self._percentile_args

    def accept(self, visitor: SqlExpressionNodeVisitor[VisitorOutputT]) -> VisitorOutputT:  # noqa: D
        return visitor.visit_percentile_expr(self)

    @property
    def description(self) -> str:  # noqa: D
        return f"{self._percentile_args.function_type.value} Percentile({self._percentile_args.percentile}) Expression"

    @property
    def displayed_properties(self) -> List[DisplayedProperty]:  # noqa: D
        return (
            super().displayed_properties
            + [DisplayedProperty("argument", self._order_by_arg)]
            + [DisplayedProperty("percentile_args", self._percentile_args)]
        )

    def __repr__(self) -> str:  # noqa: D
        return f"{self.__class__.__name__}(node_id={self.node_id}, percentile={self._percentile_args.percentile}, function_type={self._percentile_args.function_type.value})"

    def rewrite(  # noqa: D
        self,
        column_replacements: Optional[SqlColumnReplacements] = None,
        should_render_table_alias: Optional[bool] = None,
    ) -> SqlExpressionNode:
        return SqlPercentileExpression(
            order_by_arg=self._order_by_arg.rewrite(column_replacements, should_render_table_alias),
            percentile_args=self._percentile_args,
        )

    @property
    def is_aggregate_function(self) -> bool:  # noqa: D
        return True

    @property
    def lineage(self) -> SqlExpressionTreeLineage:  # noqa: D
        return SqlExpressionTreeLineage.combine(
            tuple(x.lineage for x in self.parent_nodes) + (SqlExpressionTreeLineage(function_exprs=(self,)),)
        )

    def matches(self, other: SqlExpressionNode) -> bool:  # noqa: D
        if not isinstance(other, SqlPercentileExpression):
            return False
        return self._percentile_args == other._percentile_args and self._parents_match(other)


class SqlWindowFunction(Enum):
    """Names of known SQL window functions like SUM(), RANK(), ROW_NUMBER().

    Values are the SQL string to be used in rendering.
    """

    FIRST_VALUE = "first_value"
    ROW_NUMBER = "row_number"


@dataclass(frozen=True)
class SqlWindowOrderByArgument:
    """In window functions, the ORDER BY clause can accept an expr, ordering, null ranking."""

    expr: SqlExpressionNode
    descending: Optional[bool] = None
    nulls_last: Optional[bool] = None

    @property
    def suffix(self) -> str:
        """Helper to build suffix to append to {expr}{suffix}."""
        result = []
        if self.descending is not None:
            result.append("DESC" if self.descending else "ASC")
        if self.nulls_last is not None:
            result.append("NULLS LAST" if self.nulls_last else "NULLS FIRST")
        return " ".join(result)


class SqlWindowFunctionExpression(SqlFunctionExpression):
    """A window function expression like SUM(foo) OVER bar."""

    def __init__(
        self,
        sql_function: SqlWindowFunction,
        sql_function_args: Optional[List[SqlExpressionNode]] = None,
        partition_by_args: Optional[List[SqlExpressionNode]] = None,
        order_by_args: Optional[List[SqlWindowOrderByArgument]] = None,
    ) -> None:
        """Constructor.

        Args:
            sql_function: The function that this represents.
            sql_function_args: The arguments that should go into the function. e.g. for "CONCAT(a, b)", the arg
                               expressions should be "a" and "b".
            partition_by_args: The arguments to partition the rows. e.g. PARTITION BY expr1, expr2,
                               the args are "expr1", "expr2".
            order_by_args: The expr to order the partitions by.
        """
        self._sql_function = sql_function
        self._sql_function_args = sql_function_args
        self._partition_by_args = partition_by_args
        self._order_by_args = order_by_args
        parent_nodes = []
        if sql_function_args:
            parent_nodes.extend(sql_function_args)
        if partition_by_args:
            parent_nodes.extend(partition_by_args)
        if order_by_args:
            parent_nodes.extend([x.expr for x in order_by_args])
        super().__init__(node_id=self.create_unique_id(), parent_nodes=parent_nodes)

    @classmethod
    def id_prefix(cls) -> str:  # noqa: D
        return SQL_EXPR_WINDOW_FUNCTION_ID_PREFIX

    @property
    def requires_parenthesis(self) -> bool:  # noqa: D
        return False

    def accept(self, visitor: SqlExpressionNodeVisitor[VisitorOutputT]) -> VisitorOutputT:  # noqa: D
        return visitor.visit_window_function_expr(self)

    @property
    def description(self) -> str:  # noqa: D
        return f"{self._sql_function.value} Window Function Expression"

    @property
    def displayed_properties(self) -> List[DisplayedProperty]:  # noqa: D
        return (
            super().displayed_properties
            + [DisplayedProperty("function", self.sql_function)]
            + [DisplayedProperty("argument", x) for x in self.sql_function_args]
            + [DisplayedProperty("partition_by_argument", x) for x in self.partition_by_args]
            + [DisplayedProperty("order_by_argument", x) for x in self.order_by_args]
        )

    @property
    def sql_function(self) -> SqlWindowFunction:  # noqa: D
        return self._sql_function

    @property
    def sql_function_args(self) -> List[SqlExpressionNode]:  # noqa: D
        return self._sql_function_args or []

    @property
    def partition_by_args(self) -> List[SqlExpressionNode]:  # noqa: D
        return self._partition_by_args or []

    @property
    def order_by_args(self) -> List[SqlWindowOrderByArgument]:  # noqa: D
        return self._order_by_args or []

    @property
    def is_aggregate_function(self) -> bool:  # noqa: D
        return False

    def __repr__(self) -> str:  # noqa: D
        return f"{self.__class__.__name__}(node_id={self.node_id}, sql_function={self.sql_function.name})"

    def rewrite(  # noqa: D
        self,
        column_replacements: Optional[SqlColumnReplacements] = None,
        should_render_table_alias: Optional[bool] = None,
    ) -> SqlExpressionNode:
        return SqlWindowFunctionExpression(
            sql_function=self.sql_function,
            sql_function_args=[
                x.rewrite(column_replacements, should_render_table_alias) for x in self.sql_function_args
            ],
            partition_by_args=[
                x.rewrite(column_replacements, should_render_table_alias) for x in self.partition_by_args
            ],
            order_by_args=[
                SqlWindowOrderByArgument(
                    expr=x.expr.rewrite(column_replacements, should_render_table_alias),
                    descending=x.descending,
                    nulls_last=x.nulls_last,
                )
                for x in self.order_by_args
            ],
        )

    @property
    def lineage(self) -> SqlExpressionTreeLineage:  # noqa: D
        return SqlExpressionTreeLineage.combine(
            tuple(x.lineage for x in self.parent_nodes) + (SqlExpressionTreeLineage(function_exprs=(self,)),)
        )

    def matches(self, other: SqlExpressionNode) -> bool:  # noqa: D
        if not isinstance(other, SqlWindowFunctionExpression):
            return False
        return (
            self.sql_function == other.sql_function
            and self.order_by_args == other.order_by_args
            and self._parents_match(other)
        )


class SqlNullExpression(SqlExpressionNode):
    """Represents NULL."""

    def __init__(self) -> None:  # noqa: D
        super().__init__(node_id=self.create_unique_id(), parent_nodes=[])

    @classmethod
    def id_prefix(cls) -> str:  # noqa: D
        return SQL_EXPR_NULL_PREFIX

    @property
    def requires_parenthesis(self) -> bool:  # noqa: D
        return False

    def accept(self, visitor: SqlExpressionNodeVisitor[VisitorOutputT]) -> VisitorOutputT:  # noqa: D
        return visitor.visit_null_expr(self)

    @property
    def description(self) -> str:  # noqa: D
        return "NULL Expression"

    def rewrite(  # noqa: D
        self,
        column_replacements: Optional[SqlColumnReplacements] = None,
        should_render_table_alias: Optional[bool] = None,
    ) -> SqlExpressionNode:
        return self

    @property
    def lineage(self) -> SqlExpressionTreeLineage:  # noqa: D
        return SqlExpressionTreeLineage(other_exprs=(self,))

    def matches(self, other: SqlExpressionNode) -> bool:  # noqa: D
        return isinstance(other, SqlNullExpression)


class SqlLogicalOperator(Enum):
    """List all supported binary logical operator expressions.

    Value is the SQL string used when rendering the operator.
    """

    AND = "AND"
    OR = "OR"


class SqlLogicalExpression(SqlExpressionNode):
    """A logical expression like "a AND b AND c"."""

    def __init__(self, operator: SqlLogicalOperator, args: Tuple[SqlExpressionNode, ...]) -> None:  # noqa: D
        self._operator = operator
        super().__init__(node_id=self.create_unique_id(), parent_nodes=list(args))

    @classmethod
    def id_prefix(cls) -> str:  # noqa: D
        return SQL_EXPR_LOGICAL_OPERATOR_PREFIX

    @property
    def requires_parenthesis(self) -> bool:  # noqa: D
        return True

    def accept(self, visitor: SqlExpressionNodeVisitor[VisitorOutputT]) -> VisitorOutputT:  # noqa: D
        return visitor.visit_logical_expr(self)

    @property
    def description(self) -> str:  # noqa: D
        return f"Logical Operator {self._operator.value}"

    @property
    def args(self) -> Sequence[SqlExpressionNode]:  # noqa: D
        return self.parent_nodes

    @property
    def operator(self) -> SqlLogicalOperator:  # noqa: D
        return self._operator

    def rewrite(  # noqa: D
        self,
        column_replacements: Optional[SqlColumnReplacements] = None,
        should_render_table_alias: Optional[bool] = None,
    ) -> SqlExpressionNode:
        return SqlLogicalExpression(
            operator=self.operator,
            args=tuple(x.rewrite(column_replacements, should_render_table_alias) for x in self.args),
        )

    @property
    def lineage(self) -> SqlExpressionTreeLineage:  # noqa: D
        return SqlExpressionTreeLineage.combine(
            tuple(x.lineage for x in self.parent_nodes) + (SqlExpressionTreeLineage(other_exprs=(self,)),)
        )

    def matches(self, other: SqlExpressionNode) -> bool:  # noqa: D
        if not isinstance(other, SqlLogicalExpression):
            return False
        return self.operator == other.operator and self._parents_match(other)


class SqlIsNullExpression(SqlExpressionNode):
    """An IS NULL expression like "foo IS NULL"."""

    def __init__(self, arg: SqlExpressionNode) -> None:  # noqa: D
        self._arg = arg
        super().__init__(node_id=self.create_unique_id(), parent_nodes=[arg])

    @classmethod
    def id_prefix(cls) -> str:  # noqa: D
        return SQL_EXPR_IS_NULL_PREFIX

    @property
    def requires_parenthesis(self) -> bool:  # noqa: D
        return True

    def accept(self, visitor: SqlExpressionNodeVisitor[VisitorOutputT]) -> VisitorOutputT:  # noqa: D
        return visitor.visit_is_null_expr(self)

    @property
    def description(self) -> str:  # noqa: D
        return "IS NULL Expression"

    @property
    def arg(self) -> SqlExpressionNode:  # noqa: D
        return self._arg

    def rewrite(  # noqa: D
        self,
        column_replacements: Optional[SqlColumnReplacements] = None,
        should_render_table_alias: Optional[bool] = None,
    ) -> SqlExpressionNode:
        return SqlIsNullExpression(arg=self.arg.rewrite(column_replacements, should_render_table_alias))

    @property
    def lineage(self) -> SqlExpressionTreeLineage:  # noqa: D
        return SqlExpressionTreeLineage.combine([self.arg.lineage, SqlExpressionTreeLineage(other_exprs=(self,))])

    def matches(self, other: SqlExpressionNode) -> bool:  # noqa: D
        if not isinstance(other, SqlIsNullExpression):
            return False
        return self._parents_match(other)


class SqlSubtractTimeIntervalExpression(SqlExpressionNode):
    """Represents an interval subtraction from a given timestamp.

    This node contains the information required to produce a SQL statement which subtracts an interval with the given
    count and granularity (which together define the interval duration) from the input timestamp expression. The return
    value from the SQL rendering for this expression should be a timestamp expression offset from the initial input
    value.
    """

    def __init__(  # noqa: D
        self,
        arg: SqlExpressionNode,
        count: int,
        granularity: TimeGranularity,
    ) -> None:
        super().__init__(node_id=self.create_unique_id(), parent_nodes=[arg])
        self._count = count
        self._time_granularity = granularity
        self._arg = arg

    @classmethod
    def id_prefix(cls) -> str:  # noqa: D
        return SQL_EXPR_SUBTRACT_TIME_INTERVAL_PREFIX

    @property
    def requires_parenthesis(self) -> bool:  # noqa: D
        return False

    def accept(self, visitor: SqlExpressionNodeVisitor[VisitorOutputT]) -> VisitorOutputT:  # noqa: D
        return visitor.visit_time_delta_expr(self)

    @property
    def description(self) -> str:  # noqa: D
        return "Time delta"

    @property
    def arg(self) -> SqlExpressionNode:  # noqa: D
        return self._arg

    @property
    def count(self) -> int:  # noqa: D
        return self._count

    @property
    def granularity(self) -> TimeGranularity:  # noqa: D
        return self._time_granularity

    def rewrite(  # noqa: D
        self,
        column_replacements: Optional[SqlColumnReplacements] = None,
        should_render_table_alias: Optional[bool] = None,
    ) -> SqlExpressionNode:
        return SqlSubtractTimeIntervalExpression(
            arg=self.arg.rewrite(column_replacements, should_render_table_alias),
            count=self.count,
            granularity=self.granularity,
        )

    @property
    def lineage(self) -> SqlExpressionTreeLineage:  # noqa: D
        return SqlExpressionTreeLineage.combine(
            tuple(x.lineage for x in self.parent_nodes) + (SqlExpressionTreeLineage(other_exprs=(self,)),)
        )

    def matches(self, other: SqlExpressionNode) -> bool:  # noqa: D
        if not isinstance(other, SqlSubtractTimeIntervalExpression):
            return False
        return self.count == other.count and self.granularity == other.granularity and self._parents_match(other)


class SqlCastToTimestampExpression(SqlExpressionNode):
    """Cast to the timestamp type like CAST('2020-01-01' AS TIMESTAMP)."""

    def __init__(self, arg: SqlExpressionNode) -> None:  # noqa: D
        super().__init__(node_id=self.create_unique_id(), parent_nodes=[arg])

    @classmethod
    def id_prefix(cls) -> str:  # noqa: D
        return SQL_EXPR_CAST_TO_TIMESTAMP_PREFIX

    @property
    def requires_parenthesis(self) -> bool:  # noqa: D
        return False

    def accept(self, visitor: SqlExpressionNodeVisitor[VisitorOutputT]) -> VisitorOutputT:  # noqa: D
        return visitor.visit_cast_to_timestamp_expr(self)

    @property
    def description(self) -> str:  # noqa: D
        return "Cast to Timestamp"

    @property
    def arg(self) -> SqlExpressionNode:  # noqa: D
        assert len(self.parent_nodes) == 1
        return self.parent_nodes[0]

    def rewrite(  # noqa: D
        self,
        column_replacements: Optional[SqlColumnReplacements] = None,
        should_render_table_alias: Optional[bool] = None,
    ) -> SqlExpressionNode:
        return SqlCastToTimestampExpression(arg=self.arg.rewrite(column_replacements, should_render_table_alias))

    @property
    def lineage(self) -> SqlExpressionTreeLineage:  # noqa: D
        return SqlExpressionTreeLineage.combine(
            tuple(x.lineage for x in self.parent_nodes) + (SqlExpressionTreeLineage(other_exprs=(self,)),)
        )

    def matches(self, other: SqlExpressionNode) -> bool:  # noqa: D
        if not isinstance(other, SqlCastToTimestampExpression):
            return False
        return self._parents_match(other)


class SqlDateTruncExpression(SqlExpressionNode):
    """Apply a date trunc to a column like CAST('2020-01-01' AS TIMESTAMP)."""

    def __init__(self, time_granularity: TimeGranularity, arg: SqlExpressionNode) -> None:
        """Constructor.

        Args:
            time_granularity: the granularity to DATE_TRUNC() to.
            arg: the value to DATE_TRUNC().
        """
        self._time_granularity = time_granularity
        super().__init__(node_id=self.create_unique_id(), parent_nodes=[arg])

    @classmethod
    def id_prefix(cls) -> str:  # noqa: D
        return SQL_EXPR_DATE_TRUNC

    @property
    def requires_parenthesis(self) -> bool:  # noqa: D
        return False

    def accept(self, visitor: SqlExpressionNodeVisitor[VisitorOutputT]) -> VisitorOutputT:  # noqa: D
        return visitor.visit_date_trunc_expr(self)

    @property
    def description(self) -> str:  # noqa: D
        return f"DATE_TRUNC() to {self.time_granularity}"

    @property
    def time_granularity(self) -> TimeGranularity:  # noqa: D
        return self._time_granularity

    @property
    def arg(self) -> SqlExpressionNode:  # noqa: D
        assert len(self.parent_nodes) == 1
        return self.parent_nodes[0]

    def rewrite(  # noqa: D
        self,
        column_replacements: Optional[SqlColumnReplacements] = None,
        should_render_table_alias: Optional[bool] = None,
    ) -> SqlExpressionNode:
        return SqlDateTruncExpression(
            time_granularity=self.time_granularity, arg=self.arg.rewrite(column_replacements, should_render_table_alias)
        )

    @property
    def lineage(self) -> SqlExpressionTreeLineage:  # noqa: D
        return SqlExpressionTreeLineage.combine(
            tuple(x.lineage for x in self.parent_nodes) + (SqlExpressionTreeLineage(other_exprs=(self,)),)
        )

    def matches(self, other: SqlExpressionNode) -> bool:  # noqa: D
        if not isinstance(other, SqlDateTruncExpression):
            return False
        return self.time_granularity == other.time_granularity and self._parents_match(other)


class SqlExtractExpression(SqlExpressionNode):
    """Extract a date part from a time expression."""

    def __init__(self, date_part: DatePart, arg: SqlExpressionNode) -> None:
        """Constructor.

        Args:
            date_part: the date part to extract.
            arg: the expression to extract from.
        """
        self._date_part = date_part
        super().__init__(node_id=self.create_unique_id(), parent_nodes=[arg])

    @classmethod
    def id_prefix(cls) -> str:  # noqa: D
        return SQL_EXPR_EXTRACT

    @property
    def requires_parenthesis(self) -> bool:  # noqa: D
        return False

    def accept(self, visitor: SqlExpressionNodeVisitor[VisitorOutputT]) -> VisitorOutputT:  # noqa: D
        return visitor.visit_extract_expr(self)

    @property
    def description(self) -> str:  # noqa: D
        return f"Extract {self.date_part.name}"

    @property
    def date_part(self) -> DatePart:  # noqa: D
        return self._date_part

    @property
    def arg(self) -> SqlExpressionNode:  # noqa: D
        assert len(self.parent_nodes) == 1
        return self.parent_nodes[0]

    def rewrite(  # noqa: D
        self,
        column_replacements: Optional[SqlColumnReplacements] = None,
        should_render_table_alias: Optional[bool] = None,
    ) -> SqlExpressionNode:
        return SqlExtractExpression(
            date_part=self.date_part, arg=self.arg.rewrite(column_replacements, should_render_table_alias)
        )

    @property
    def lineage(self) -> SqlExpressionTreeLineage:  # noqa: D
        return SqlExpressionTreeLineage.combine(
            tuple(x.lineage for x in self.parent_nodes) + (SqlExpressionTreeLineage(other_exprs=(self,)),)
        )

    def matches(self, other: SqlExpressionNode) -> bool:  # noqa: D
        if not isinstance(other, SqlExtractExpression):
            return False
        return self.date_part == other.date_part and self._parents_match(other)


class SqlRatioComputationExpression(SqlExpressionNode):
    """Node for expressing Ratio metrics to allow for appropriate casting to float/double in each engine.

    In future we might wish to break this up into a set of nodes, e.g., SqlCastExpression and SqlMathExpression
    or even add CAST to SqlFunctionExpression. However, at this time the only mathematical operation we encode
    is division, and we only use that for ratios. Similarly, the only times we do typecasting are when we are
    coercing timestamps (already handled) or computing ratio metrics.
    """

    def __init__(self, numerator: SqlExpressionNode, denominator: SqlExpressionNode) -> None:
        """Initialize this node for computing a ratio. Expression renderers should handle the casting.

        Args:
            numerator: the expression for the numerator in the ratio
            denominator: the expression for the denominator in the ratio
        """
        self._numerator = numerator
        self._denominator = denominator
        super().__init__(node_id=self.create_unique_id(), parent_nodes=[numerator, denominator])

    @classmethod
    def id_prefix(cls) -> str:  # noqa: D
        return SQL_EXPR_RATIO_COMPUTATION

    @property
    def requires_parenthesis(self) -> bool:  # noqa: D
        return False

    def accept(self, visitor: SqlExpressionNodeVisitor[VisitorOutputT]) -> VisitorOutputT:  # noqa: D
        return visitor.visit_ratio_computation_expr(self)

    @property
    def description(self) -> str:  # noqa: D
        return "Divide numerator by denominator, with appropriate casting"

    @property
    def numerator(self) -> SqlExpressionNode:  # noqa: D
        return self._numerator

    @property
    def denominator(self) -> SqlExpressionNode:  # noqa: D
        return self._denominator

    def rewrite(  # noqa: D
        self,
        column_replacements: Optional[SqlColumnReplacements] = None,
        should_render_table_alias: Optional[bool] = None,
    ) -> SqlExpressionNode:
        return SqlRatioComputationExpression(
            numerator=self.numerator.rewrite(column_replacements, should_render_table_alias),
            denominator=self.denominator.rewrite(column_replacements, should_render_table_alias),
        )

    @property
    def lineage(self) -> SqlExpressionTreeLineage:  # noqa: D
        return SqlExpressionTreeLineage.combine(
            tuple(x.lineage for x in self.parent_nodes) + (SqlExpressionTreeLineage(other_exprs=(self,)),)
        )

    def matches(self, other: SqlExpressionNode) -> bool:  # noqa: D
        if not isinstance(other, SqlRatioComputationExpression):
            return False
        return self._parents_match(other)


class SqlBetweenExpression(SqlExpressionNode):
    """A BETWEEN clause like `column BETWEEN val1 AND val2`."""

    def __init__(  # noqa: D
        self, column_arg: SqlExpressionNode, start_expr: SqlExpressionNode, end_expr: SqlExpressionNode
    ) -> None:
        self._column_arg = column_arg
        self._start_expr = start_expr
        self._end_expr = end_expr
        super().__init__(node_id=self.create_unique_id(), parent_nodes=[column_arg, start_expr, end_expr])

    @classmethod
    def id_prefix(cls) -> str:  # noqa: D
        return SQL_EXPR_BETWEEN_PREFIX

    @property
    def requires_parenthesis(self) -> bool:  # noqa: D
        return False

    def accept(self, visitor: SqlExpressionNodeVisitor[VisitorOutputT]) -> VisitorOutputT:  # noqa: D
        return visitor.visit_between_expr(self)

    @property
    def description(self) -> str:  # noqa: D
        return "BETWEEN operator"

    @property
    def column_arg(self) -> SqlExpressionNode:  # noqa: D
        return self._column_arg

    @property
    def start_expr(self) -> SqlExpressionNode:  # noqa: D
        return self._start_expr

    @property
    def end_expr(self) -> SqlExpressionNode:  # noqa: D
        return self._end_expr

    def rewrite(  # noqa: D
        self,
        column_replacements: Optional[SqlColumnReplacements] = None,
        should_render_table_alias: Optional[bool] = None,
    ) -> SqlExpressionNode:
        return SqlBetweenExpression(
            column_arg=self.column_arg.rewrite(column_replacements, should_render_table_alias),
            start_expr=self.start_expr.rewrite(column_replacements, should_render_table_alias),
            end_expr=self.end_expr.rewrite(column_replacements, should_render_table_alias),
        )

    @property
    def lineage(self) -> SqlExpressionTreeLineage:  # noqa: D
        return SqlExpressionTreeLineage.combine(
            tuple(x.lineage for x in self.parent_nodes) + (SqlExpressionTreeLineage(other_exprs=(self,)),)
        )

    def matches(self, other: SqlExpressionNode) -> bool:  # noqa: D
        if not isinstance(other, SqlBetweenExpression):
            return False
        return self._parents_match(other)


class SqlGenerateUuidExpression(SqlExpressionNode):
    """Renders a sql to generate a random uuid, is non-deterministic.."""

    def __init__(self) -> None:  # noqa: D
        super().__init__(node_id=self.create_unique_id(), parent_nodes=[])

    @classmethod
    def id_prefix(cls) -> str:  # noqa: D
        return SQL_EXPR_GENERATE_UUID_PREFIX

    def accept(self, visitor: SqlExpressionNodeVisitor[VisitorOutputT]) -> VisitorOutputT:  # noqa: D
        return visitor.visit_generate_uuid_expr(self)

    @property
    def description(self) -> str:  # noqa: D
        return "Generate a universally unique identifier"

    @property
    def displayed_properties(self) -> List[DisplayedProperty]:  # noqa: D
        return super().displayed_properties

    @property
    def requires_parenthesis(self) -> bool:  # noqa: D
        return False

    @property
    def bind_parameters(self) -> SqlBindParameters:  # noqa: D
        return SqlBindParameters()

    def __repr__(self) -> str:  # noqa: D
        return f"{self.__class__.__name__}(node_id={self.node_id})"

    def rewrite(  # noqa: D
        self,
        column_replacements: Optional[SqlColumnReplacements] = None,
        should_render_table_alias: Optional[bool] = None,
    ) -> SqlExpressionNode:
        return self

    @property
    def lineage(self) -> SqlExpressionTreeLineage:  # noqa: D
        return SqlExpressionTreeLineage(other_exprs=(self,))

    def matches(self, other: SqlExpressionNode) -> bool:  # noqa: D
        return False
