"""Nodes used in defining SQL expressions."""

from __future__ import annotations

import itertools
from abc import ABC, abstractmethod
from dataclasses import dataclass
from enum import Enum
from typing import Dict, Generic, List, Mapping, Optional, Sequence, Tuple

from dbt_semantic_interfaces.enum_extension import assert_values_exhausted
from dbt_semantic_interfaces.protocols.measure import MeasureAggregationParameters
from dbt_semantic_interfaces.type_enums.aggregation_type import AggregationType
from dbt_semantic_interfaces.type_enums.date_part import DatePart
from dbt_semantic_interfaces.type_enums.period_agg import PeriodAggregation
from dbt_semantic_interfaces.type_enums.time_granularity import TimeGranularity
from typing_extensions import override

from metricflow_semantics.dag.id_prefix import IdPrefix, StaticIdPrefix
from metricflow_semantics.dag.mf_dag import DagNode, DisplayedProperty
from metricflow_semantics.model.semantics.simple_metric_input import SimpleMetricInputAggregation
from metricflow_semantics.sql.sql_bind_parameters import SqlBindParameterSet
from metricflow_semantics.toolkit.merger import Mergeable
from metricflow_semantics.toolkit.mf_logging.pretty_formatter import PrettyFormatContext
from metricflow_semantics.toolkit.visitor import Visitable, VisitorOutputT


@dataclass(frozen=True, eq=False)
class SqlExpressionNode(DagNode["SqlExpressionNode"], Visitable, ABC):
    """An SQL expression like my_table.my_column, CONCAT(a, b) or 1 + 1 that evaluates to a value."""

    @property
    @abstractmethod
    def requires_parenthesis(self) -> bool:
        """Should expression needs be rendered with parenthesis when rendering inside other expressions.

        Useful for string expressions where we can't infer the structure. For example, in rendering

        SqlMathExpression(operator="*", left_expr=SqlStringExpression.create("a"), right_expr=SqlStringExpression.create("b + c")

        this can be used to differentiate between

        a * b + c vs. a * (b + c)
        """
        pass

    @abstractmethod
    def accept(self, visitor: SqlExpressionNodeVisitor[VisitorOutputT]) -> VisitorOutputT:
        """Called when a visitor needs to visit this node."""
        pass

    @property
    def bind_parameter_set(self) -> SqlBindParameterSet:
        """Execution parameters when running a query containing this expression.

        * See: https://docs.sqlalchemy.org/en/14/core/tutorial.html#using-textual-sql
        * Generally only defined for string expressions.
        """
        return SqlBindParameterSet()

    @property
    def as_column_reference_expression(self) -> Optional[SqlColumnReferenceExpression]:
        """If this is a column reference expression, return self."""
        return None

    @property
    def as_column_alias_reference_expression(self) -> Optional[SqlColumnAliasReferenceExpression]:
        """If this is a column alias reference expression, return self."""
        return None

    @property
    def as_string_expression(self) -> Optional[SqlStringExpression]:
        """If this is a string expression, return self."""
        return None

    @property
    def as_window_function_expression(self) -> Optional[SqlWindowFunctionExpression]:
        """If this is a window function expression, return self."""
        return None

    @property
    def is_verbose(self) -> bool:
        """Denotes if the statement is typically verbose, and therefore can be hard to read when optimized.

        This is helpful in determining if statements will be harder to read when collapsed.
        """
        return False

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

    def _parents_match(self, other: SqlExpressionNode) -> bool:
        return all(x == y for x, y in itertools.zip_longest(self.parent_nodes, other.parent_nodes))

    @abstractmethod
    def matches(self, other: SqlExpressionNode) -> bool:
        """Similar to equals - returns true if these expressions are equivalent."""
        pass


@dataclass(frozen=True)
class SqlExpressionTreeLineage(Mergeable):
    """Captures the lineage of an expression node - contains itself and all ancestor nodes."""

    string_exprs: Tuple[SqlStringExpression, ...] = ()
    function_exprs: Tuple[SqlFunctionExpression, ...] = ()
    column_reference_exprs: Tuple[SqlColumnReferenceExpression, ...] = ()
    column_alias_reference_exprs: Tuple[SqlColumnAliasReferenceExpression, ...] = ()
    other_exprs: Tuple[SqlExpressionNode, ...] = ()

    @property
    def contains_string_exprs(self) -> bool:  # noqa: D102
        return len(self.string_exprs) > 0

    @property
    def contains_column_alias_exprs(self) -> bool:  # noqa: D102
        return len(self.column_alias_reference_exprs) > 0

    @property
    def contains_ambiguous_exprs(self) -> bool:  # noqa: D102
        return self.contains_string_exprs or self.contains_column_alias_exprs

    @property
    def contains_aggregate_exprs(self) -> bool:  # noqa: D102
        return any(x.is_aggregate_function for x in self.function_exprs)

    @override
    def merge(self, other: SqlExpressionTreeLineage) -> SqlExpressionTreeLineage:
        return SqlExpressionTreeLineage(
            string_exprs=self.string_exprs + other.string_exprs,
            function_exprs=self.function_exprs + other.function_exprs,
            column_reference_exprs=self.column_reference_exprs + other.column_reference_exprs,
            column_alias_reference_exprs=self.column_alias_reference_exprs + other.column_alias_reference_exprs,
            other_exprs=self.other_exprs + other.other_exprs,
        )

    @classmethod
    @override
    def empty_instance(cls) -> SqlExpressionTreeLineage:
        return SqlExpressionTreeLineage()


class SqlColumnReplacements:
    """When re-writing column references in expressions, this stores the mapping."""

    def __init__(self, column_replacements: Dict[SqlColumnReference, SqlExpressionNode]) -> None:  # noqa: D107
        self._column_replacements = column_replacements

    def get_replacement(self, column_reference: SqlColumnReference) -> Optional[SqlExpressionNode]:  # noqa: D102
        return self._column_replacements.get(column_reference)


class SqlExpressionNodeVisitor(Generic[VisitorOutputT], ABC):
    """A visitor to help visit the nodes of an expression.

    See similar visitor DataflowPlanVisitor.
    """

    @abstractmethod
    def visit_string_expr(self, node: SqlStringExpression) -> VisitorOutputT:  # noqa: D102
        pass

    @abstractmethod
    def visit_column_reference_expr(self, node: SqlColumnReferenceExpression) -> VisitorOutputT:  # noqa: D102
        pass

    @abstractmethod
    def visit_column_alias_reference_expr(  # noqa: D102
        self, node: SqlColumnAliasReferenceExpression
    ) -> VisitorOutputT:  # noqa: D102
        pass

    @abstractmethod
    def visit_comparison_expr(self, node: SqlComparisonExpression) -> VisitorOutputT:  # noqa: D102
        pass

    @abstractmethod
    def visit_function_expr(self, node: SqlAggregateFunctionExpression) -> VisitorOutputT:  # noqa: D102
        pass

    @abstractmethod
    def visit_percentile_expr(self, node: SqlPercentileExpression) -> VisitorOutputT:  # noqa: D102
        pass

    @abstractmethod
    def visit_null_expr(self, node: SqlNullExpression) -> VisitorOutputT:  # noqa: D102
        pass

    @abstractmethod
    def visit_logical_expr(self, node: SqlLogicalExpression) -> VisitorOutputT:  # noqa: D102
        pass

    @abstractmethod
    def visit_string_literal_expr(self, node: SqlStringLiteralExpression) -> VisitorOutputT:  # noqa: D102
        pass

    @abstractmethod
    def visit_is_null_expr(self, node: SqlIsNullExpression) -> VisitorOutputT:  # noqa: D102
        pass

    @abstractmethod
    def visit_cast_to_timestamp_expr(self, node: SqlCastToTimestampExpression) -> VisitorOutputT:  # noqa: D102
        pass

    @abstractmethod
    def visit_date_trunc_expr(self, node: SqlDateTruncExpression) -> VisitorOutputT:  # noqa: D102
        pass

    @abstractmethod
    def visit_extract_expr(self, node: SqlExtractExpression) -> VisitorOutputT:  # noqa: D102
        pass

    @abstractmethod
    def visit_subtract_time_interval_expr(  # noqa: D102
        self, node: SqlSubtractTimeIntervalExpression
    ) -> VisitorOutputT:
        pass

    @abstractmethod
    def visit_add_time_expr(self, node: SqlAddTimeExpression) -> VisitorOutputT:  # noqa: D102
        pass

    @abstractmethod
    def visit_ratio_computation_expr(self, node: SqlRatioComputationExpression) -> VisitorOutputT:  # noqa: D102
        pass

    @abstractmethod
    def visit_between_expr(self, node: SqlBetweenExpression) -> VisitorOutputT:  # noqa: D102
        pass

    @abstractmethod
    def visit_window_function_expr(self, node: SqlWindowFunctionExpression) -> VisitorOutputT:  # noqa: D102
        pass

    @abstractmethod
    def visit_generate_uuid_expr(self, node: SqlGenerateUuidExpression) -> VisitorOutputT:  # noqa: D102
        pass

    @abstractmethod
    def visit_case_expr(self, node: SqlCaseExpression) -> VisitorOutputT:  # noqa: D102
        pass

    @abstractmethod
    def visit_arithmetic_expr(self, node: SqlArithmeticExpression) -> VisitorOutputT:  # noqa: D102
        pass

    @abstractmethod
    def visit_integer_expr(self, node: SqlIntegerExpression) -> VisitorOutputT:  # noqa: D102
        pass


@dataclass(frozen=True, eq=False)
class SqlStringExpression(SqlExpressionNode):
    """An SQL expression in a string format, so it lacks information about the structure.

    These are convenient to use, but because structure is lacking, it can't be easily handled for DB rendering and can
    impede optimizations.

    Attributes:
        sql_expr: The SQL in string form.
        bind_parameter_set: See SqlExpressionNode.bind_parameter_set
        requires_parenthesis: Whether this should be rendered with () if nested in another expression.
        used_columns: If set, indicates that the expression represented by the string only uses those columns. e.g.
        sql_expr="a + b", used_columns=["a", "b"]. This may be used by optimizers, and if specified, it must be
        complete. e.g. sql_expr="a + b + c", used_columns=["a", "b"] will cause problems.
    """

    sql_expr: str
    bind_parameter_set: SqlBindParameterSet = SqlBindParameterSet()
    requires_parenthesis: bool = True
    used_columns: Optional[Tuple[str, ...]] = None

    @staticmethod
    def create(  # noqa: D102
        sql_expr: str,
        bind_parameter_set: SqlBindParameterSet = SqlBindParameterSet(),
        requires_parenthesis: bool = True,
        used_columns: Optional[Tuple[str, ...]] = None,
    ) -> SqlStringExpression:
        return SqlStringExpression(
            parent_nodes=(),
            sql_expr=sql_expr,
            bind_parameter_set=bind_parameter_set,
            requires_parenthesis=requires_parenthesis,
            used_columns=used_columns,
        )

    @classmethod
    def id_prefix(cls) -> IdPrefix:  # noqa: D102
        return StaticIdPrefix.SQL_EXPR_STRING_ID_PREFIX

    def accept(self, visitor: SqlExpressionNodeVisitor[VisitorOutputT]) -> VisitorOutputT:  # noqa: D102
        return visitor.visit_string_expr(self)

    @property
    def description(self) -> str:  # noqa: D102
        return f"String SQL Expression: {self.sql_expr}"

    @property
    def displayed_properties(self) -> Sequence[DisplayedProperty]:  # noqa: D102
        return tuple(super().displayed_properties) + (DisplayedProperty("sql_expr", self.sql_expr),)

    @override
    def pretty_format(self, format_context: PrettyFormatContext) -> Optional[str]:
        return f"{self.__class__.__name__}(node_id={self.node_id} sql_expr={self.sql_expr})"

    def rewrite(  # noqa: D102
        self,
        column_replacements: Optional[SqlColumnReplacements] = None,
        should_render_table_alias: Optional[bool] = None,
    ) -> SqlExpressionNode:
        if column_replacements:
            raise NotImplementedError()
        return self

    @property
    def lineage(self) -> SqlExpressionTreeLineage:  # noqa: D102
        return SqlExpressionTreeLineage(string_exprs=(self,))

    def matches(self, other: SqlExpressionNode) -> bool:  # noqa: D102
        if not isinstance(other, SqlStringExpression):
            return False
        return (
            self.sql_expr == other.sql_expr
            and self.used_columns == other.used_columns
            and self.bind_parameter_set == other.bind_parameter_set
        )

    @property
    def as_string_expression(self) -> Optional[SqlStringExpression]:
        """If this is a string expression, return self."""
        return self


@dataclass(frozen=True, eq=False)
class SqlStringLiteralExpression(SqlExpressionNode):
    """A string literal like 'foo'. It shouldn't include delimiters as it should be added during rendering."""

    literal_value: str

    @staticmethod
    def create(literal_value: str) -> SqlStringLiteralExpression:  # noqa: D102
        return SqlStringLiteralExpression(parent_nodes=(), literal_value=literal_value)

    @classmethod
    def id_prefix(cls) -> IdPrefix:  # noqa: D102
        return StaticIdPrefix.SQL_EXPR_STRING_LITERAL_PREFIX

    def accept(self, visitor: SqlExpressionNodeVisitor[VisitorOutputT]) -> VisitorOutputT:  # noqa: D102
        return visitor.visit_string_literal_expr(self)

    @property
    def description(self) -> str:  # noqa: D102
        return f"String Literal: {self.literal_value}"

    @property
    def displayed_properties(self) -> Sequence[DisplayedProperty]:  # noqa: D102
        return tuple(super().displayed_properties) + (DisplayedProperty("value", self.literal_value),)

    @property
    def requires_parenthesis(self) -> bool:  # noqa: D102
        return False

    @property
    def bind_parameter_set(self) -> SqlBindParameterSet:  # noqa: D102
        return SqlBindParameterSet()

    def __repr__(self) -> str:  # noqa: D105
        return f"{self.__class__.__name__}(node_id={self.node_id}, literal_value={self.literal_value})"

    def rewrite(  # noqa: D102
        self,
        column_replacements: Optional[SqlColumnReplacements] = None,
        should_render_table_alias: Optional[bool] = None,
    ) -> SqlExpressionNode:
        return self

    @property
    def lineage(self) -> SqlExpressionTreeLineage:  # noqa: D102
        return SqlExpressionTreeLineage(other_exprs=(self,))

    def matches(self, other: SqlExpressionNode) -> bool:  # noqa: D102
        if not isinstance(other, SqlStringLiteralExpression):
            return False
        return self.literal_value == other.literal_value


@dataclass(frozen=True, eq=False)
class SqlIntegerExpression(SqlExpressionNode):
    """An integer like 1."""

    integer_value: int

    @staticmethod
    def create(integer_value: int) -> SqlIntegerExpression:  # noqa: D102
        return SqlIntegerExpression(parent_nodes=(), integer_value=integer_value)

    @classmethod
    def id_prefix(cls) -> IdPrefix:  # noqa: D102
        return StaticIdPrefix.SQL_EXPR_INTEGER_PREFIX

    def accept(self, visitor: SqlExpressionNodeVisitor[VisitorOutputT]) -> VisitorOutputT:  # noqa: D102
        return visitor.visit_integer_expr(self)

    @property
    def description(self) -> str:  # noqa: D102
        return f"Integer: {self.integer_value}"

    @property
    def displayed_properties(self) -> Sequence[DisplayedProperty]:  # noqa: D102
        return tuple(super().displayed_properties) + (DisplayedProperty("value", self.integer_value),)

    @property
    def requires_parenthesis(self) -> bool:  # noqa: D102
        return False

    @property
    def bind_parameter_set(self) -> SqlBindParameterSet:  # noqa: D102
        return SqlBindParameterSet()

    def __repr__(self) -> str:  # noqa: D105
        return f"{self.__class__.__name__}(node_id={self.node_id}, integer_value={self.integer_value})"

    def rewrite(  # noqa: D102
        self,
        column_replacements: Optional[SqlColumnReplacements] = None,
        should_render_table_alias: Optional[bool] = None,
    ) -> SqlExpressionNode:
        return self

    @property
    def lineage(self) -> SqlExpressionTreeLineage:  # noqa: D102
        return SqlExpressionTreeLineage(other_exprs=(self,))

    def matches(self, other: SqlExpressionNode) -> bool:  # noqa: D102
        if not isinstance(other, SqlIntegerExpression):
            return False
        return self.integer_value == other.integer_value


@dataclass(frozen=True)
class SqlColumnReference:
    """Used with string expressions to specify what columns are referred to in the string expression."""

    table_alias: str
    column_name: str


@dataclass(frozen=True, eq=False)
class SqlColumnReferenceExpression(SqlExpressionNode):
    """An expression that evaluates to the value of a column in one of the sources in the select query.

    e.g. my_table.my_column

    Attributes:
        col_ref: the associated column reference.
        should_render_table_alias: When converting this to SQL text, whether the table alias needed to be included.
        e.g. "foo.bar" vs "bar".
    """

    col_ref: SqlColumnReference
    should_render_table_alias: bool

    def __post_init__(self) -> None:  # noqa: D105
        super().__post_init__()
        assert len(self.parent_nodes) == 0

    @staticmethod
    def create(  # noqa: D102
        col_ref: SqlColumnReference, should_render_table_alias: bool = True
    ) -> SqlColumnReferenceExpression:
        return SqlColumnReferenceExpression(
            parent_nodes=(),
            col_ref=col_ref,
            should_render_table_alias=should_render_table_alias,
        )

    @classmethod
    def id_prefix(cls) -> IdPrefix:  # noqa: D102
        return StaticIdPrefix.SQL_EXPR_COLUMN_REFERENCE_ID_PREFIX

    def accept(self, visitor: SqlExpressionNodeVisitor[VisitorOutputT]) -> VisitorOutputT:  # noqa: D102
        return visitor.visit_column_reference_expr(self)

    @property
    def description(self) -> str:  # noqa: D102
        return f"Column: {self.col_ref}"

    @property
    def displayed_properties(self) -> Sequence[DisplayedProperty]:  # noqa: D102
        return tuple(super().displayed_properties) + (DisplayedProperty("col_ref", self.col_ref),)

    @property
    def requires_parenthesis(self) -> bool:  # noqa: D102
        return False

    @property
    def as_column_reference_expression(self) -> Optional[SqlColumnReferenceExpression]:  # noqa: D102
        return self

    def rewrite(  # noqa: D102
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
                    return SqlColumnReferenceExpression.create(
                        col_ref=self.col_ref, should_render_table_alias=should_render_table_alias
                    )
                return self

        if should_render_table_alias is not None:
            return SqlColumnReferenceExpression.create(
                col_ref=self.col_ref, should_render_table_alias=should_render_table_alias
            )

        return SqlColumnReferenceExpression.create(
            col_ref=self.col_ref, should_render_table_alias=self.should_render_table_alias
        )

    @property
    def lineage(self) -> SqlExpressionTreeLineage:  # noqa: D102
        return SqlExpressionTreeLineage(column_reference_exprs=(self,))

    def matches(self, other: SqlExpressionNode) -> bool:  # noqa: D102
        if not isinstance(other, SqlColumnReferenceExpression):
            return False
        return self.col_ref == other.col_ref

    @staticmethod
    def from_column_reference(table_alias: str, column_name: str) -> SqlColumnReferenceExpression:  # noqa: D102
        return SqlColumnReferenceExpression.create(SqlColumnReference(table_alias=table_alias, column_name=column_name))

    def with_new_table_alias(self, new_table_alias: str) -> SqlColumnReferenceExpression:
        """Returns a new column reference expression with the same column name but a new table alias."""
        return SqlColumnReferenceExpression.from_column_reference(
            table_alias=new_table_alias, column_name=self.col_ref.column_name
        )


@dataclass(frozen=True, eq=False)
class SqlColumnAliasReferenceExpression(SqlExpressionNode):
    """An expression that evaluates to the alias of a column, but is not qualified with a table alias.

    e.g. SELECT foo vs. SELECT a.foo.

    This is needed to handle some exceptional cases, but in general, this should not be used as it can lead to
    ambiguities.
    """

    column_alias: str

    @staticmethod
    def create(column_alias: str) -> SqlColumnAliasReferenceExpression:  # noqa: D102
        return SqlColumnAliasReferenceExpression(
            parent_nodes=(),
            column_alias=column_alias,
        )

    @classmethod
    def id_prefix(cls) -> IdPrefix:  # noqa: D102
        return StaticIdPrefix.SQL_EXPR_COLUMN_REFERENCE_ID_PREFIX

    def accept(self, visitor: SqlExpressionNodeVisitor[VisitorOutputT]) -> VisitorOutputT:  # noqa: D102
        return visitor.visit_column_alias_reference_expr(self)

    @property
    def description(self) -> str:  # noqa: D102
        return f"Unqualified Column: {self.column_alias}"

    @property
    def displayed_properties(self) -> Sequence[DisplayedProperty]:  # noqa: D102
        return tuple(super().displayed_properties) + (DisplayedProperty("column_alias", self.column_alias),)

    @property
    def requires_parenthesis(self) -> bool:  # noqa: D102
        return False

    @property
    def as_column_alias_reference_expression(self) -> Optional[SqlColumnAliasReferenceExpression]:  # noqa: D102
        return self

    def rewrite(  # noqa: D102
        self,
        column_replacements: Optional[SqlColumnReplacements] = None,
        should_render_table_alias: Optional[bool] = None,
    ) -> SqlExpressionNode:
        if column_replacements:
            raise NotImplementedError()
        return self

    @property
    def lineage(self) -> SqlExpressionTreeLineage:  # noqa: D102
        return SqlExpressionTreeLineage(column_alias_reference_exprs=(self,))

    def matches(self, other: SqlExpressionNode) -> bool:  # noqa: D102
        if not isinstance(other, SqlColumnAliasReferenceExpression):
            return False
        return self.column_alias == other.column_alias


class SqlComparison(Enum):  # noqa: D101
    LESS_THAN = "<"
    GREATER_THAN = ">"
    LESS_THAN_OR_EQUALS = "<="
    GREATER_THAN_OR_EQUALS = ">="
    EQUALS = "="


@dataclass(frozen=True, eq=False)
class SqlComparisonExpression(SqlExpressionNode):
    """A comparison using >, <, <=, >=, =.

    e.g. my_table.my_column = a + b

    Attributes:
        left_expr: The expression on the left side of the =
        comparison: The comparison to use on expressions
        right_expr: The expression on the right side of the =
    """

    left_expr: SqlExpressionNode
    comparison: SqlComparison
    right_expr: SqlExpressionNode

    @staticmethod
    def create(  # noqa: D102
        left_expr: SqlExpressionNode, comparison: SqlComparison, right_expr: SqlExpressionNode
    ) -> SqlComparisonExpression:
        return SqlComparisonExpression(
            parent_nodes=(left_expr, right_expr), left_expr=left_expr, comparison=comparison, right_expr=right_expr
        )

    @classmethod
    def id_prefix(cls) -> IdPrefix:  # noqa: D102
        return StaticIdPrefix.SQL_EXPR_COMPARISON_ID_PREFIX

    def accept(self, visitor: SqlExpressionNodeVisitor[VisitorOutputT]) -> VisitorOutputT:  # noqa: D102
        return visitor.visit_comparison_expr(self)

    @property
    def description(self) -> str:  # noqa: D102
        return f"{self.comparison.value} Expression"

    @property
    def displayed_properties(self) -> Sequence[DisplayedProperty]:  # noqa: D102
        return tuple(super().displayed_properties) + (
            DisplayedProperty("left_expr", self.left_expr),
            DisplayedProperty("comparison", self.comparison.value),
            DisplayedProperty("right_expr", self.right_expr),
        )

    @property
    def requires_parenthesis(self) -> bool:  # noqa: D102
        return True

    def rewrite(  # noqa: D102
        self,
        column_replacements: Optional[SqlColumnReplacements] = None,
        should_render_table_alias: Optional[bool] = None,
    ) -> SqlExpressionNode:
        return SqlComparisonExpression.create(
            left_expr=self.left_expr.rewrite(column_replacements, should_render_table_alias),
            comparison=self.comparison,
            right_expr=self.right_expr.rewrite(column_replacements, should_render_table_alias),
        )

    @property
    def lineage(self) -> SqlExpressionTreeLineage:  # noqa: D102
        return SqlExpressionTreeLineage.merge_iterable(
            tuple(x.lineage for x in self.parent_nodes) + (SqlExpressionTreeLineage(other_exprs=(self,)),)
        )

    def matches(self, other: SqlExpressionNode) -> bool:  # noqa: D102
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
        """Returns a tuple containing all currently-supported DISTINCT type aggregation functions.

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


@dataclass(frozen=True, eq=False)
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
        agg_params: Optional[SimpleMetricInputAggregation] = None,
    ) -> SqlFunctionExpression:
        """Returns sql function expression depending on aggregation type."""
        if aggregation_type is AggregationType.PERCENTILE:
            assert agg_params is not None, "Agg_params is none, which should have been caught in validation"
            return SqlPercentileExpression.create(
                sql_column_expression, SqlPercentileExpressionArgument.from_aggregation_parameters(agg_params)
            )
        else:
            return SqlAggregateFunctionExpression.from_aggregation_type(aggregation_type, sql_column_expression)


@dataclass(frozen=True, eq=False)
class SqlAggregateFunctionExpression(SqlFunctionExpression):
    """An aggregate function expression like SUM(1).

    Attributes:
        sql_function: The function that this represents.
        sql_function_args: The arguments that should go into the function. e.g. for "CONCAT(a, b)", the arg
        expressions should be "a" and "b".
    """

    sql_function: SqlFunction
    sql_function_args: Tuple[SqlExpressionNode, ...]

    @staticmethod
    def from_aggregation_type(
        aggregation_type: AggregationType, sql_column_expression: SqlColumnReferenceExpression
    ) -> SqlAggregateFunctionExpression:
        """Given the aggregation type, return an SQL function expression that does that aggregation on the given col."""
        return SqlAggregateFunctionExpression.create(
            sql_function=SqlFunction.from_aggregation_type(aggregation_type=aggregation_type),
            sql_function_args=[sql_column_expression],
        )

    @staticmethod
    def create(  # noqa: D102
        sql_function: SqlFunction, sql_function_args: Sequence[SqlExpressionNode]
    ) -> SqlAggregateFunctionExpression:
        return SqlAggregateFunctionExpression(
            parent_nodes=tuple(sql_function_args),
            sql_function=sql_function,
            sql_function_args=tuple(sql_function_args),
        )

    @classmethod
    def id_prefix(cls) -> IdPrefix:  # noqa: D102
        return StaticIdPrefix.SQL_EXPR_FUNCTION_ID_PREFIX

    @property
    def requires_parenthesis(self) -> bool:  # noqa: D102
        return False

    def accept(self, visitor: SqlExpressionNodeVisitor[VisitorOutputT]) -> VisitorOutputT:  # noqa: D102
        return visitor.visit_function_expr(self)

    @property
    def description(self) -> str:  # noqa: D102
        return f"{self.sql_function.value} Expression"

    @property
    def displayed_properties(self) -> Sequence[DisplayedProperty]:  # noqa: D102
        return (
            tuple(super().displayed_properties)
            + (DisplayedProperty("function", self.sql_function),)
            + tuple(DisplayedProperty("argument", x) for x in self.sql_function_args)
        )

    @override
    def pretty_format(self, format_context: PrettyFormatContext) -> Optional[str]:
        return f"{self.__class__.__name__}(node_id={self.node_id}, sql_function={self.sql_function.name})"

    def rewrite(  # noqa: D102
        self,
        column_replacements: Optional[SqlColumnReplacements] = None,
        should_render_table_alias: Optional[bool] = None,
    ) -> SqlExpressionNode:
        return SqlAggregateFunctionExpression.create(
            sql_function=self.sql_function,
            sql_function_args=[
                x.rewrite(column_replacements, should_render_table_alias) for x in self.sql_function_args
            ],
        )

    @property
    def is_aggregate_function(self) -> bool:  # noqa: D102
        return True

    @property
    def lineage(self) -> SqlExpressionTreeLineage:  # noqa: D102
        return SqlExpressionTreeLineage.merge_iterable(
            tuple(x.lineage for x in self.parent_nodes) + (SqlExpressionTreeLineage(function_exprs=(self,)),)
        )

    def matches(self, other: SqlExpressionNode) -> bool:  # noqa: D102
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
        """Given the simple-metric input parameters, returns a SqlPercentileExpressionArgument with the corresponding percentile args."""
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


@dataclass(frozen=True, eq=False)
class SqlPercentileExpression(SqlFunctionExpression):
    """A percentile aggregation expression.

    Attributes:
        order_by_arg: The expression that should go into the function. e.g. for "percentile_cont(col, 0.1)", the arg
        expressions should be "col".
        percentile_args: Auxillary information including percentile value and type.
    """

    order_by_arg: SqlExpressionNode
    percentile_args: SqlPercentileExpressionArgument

    @staticmethod
    def create(  # noqa: D102
        order_by_arg: SqlExpressionNode, percentile_args: SqlPercentileExpressionArgument
    ) -> SqlPercentileExpression:
        return SqlPercentileExpression(
            parent_nodes=(order_by_arg,),
            order_by_arg=order_by_arg,
            percentile_args=percentile_args,
        )

    @classmethod
    def id_prefix(cls) -> IdPrefix:  # noqa: D102
        return StaticIdPrefix.SQL_EXPR_PERCENTILE_ID_PREFIX

    @property
    def requires_parenthesis(self) -> bool:  # noqa: D102
        return False

    def accept(self, visitor: SqlExpressionNodeVisitor[VisitorOutputT]) -> VisitorOutputT:  # noqa: D102
        return visitor.visit_percentile_expr(self)

    @property
    def description(self) -> str:  # noqa: D102
        return f"{self.percentile_args.function_type.value} Percentile({self.percentile_args.percentile}) Expression"

    @property
    def displayed_properties(self) -> Sequence[DisplayedProperty]:  # noqa: D102
        return (
            tuple(super().displayed_properties)
            + (DisplayedProperty("argument", self.order_by_arg),)
            + (DisplayedProperty("percentile_args", self.percentile_args),)
        )

    def __repr__(self) -> str:  # noqa: D105
        return f"{self.__class__.__name__}(node_id={self.node_id}, percentile={self.percentile_args.percentile}, function_type={self.percentile_args.function_type.value})"

    def rewrite(  # noqa: D102
        self,
        column_replacements: Optional[SqlColumnReplacements] = None,
        should_render_table_alias: Optional[bool] = None,
    ) -> SqlExpressionNode:
        return SqlPercentileExpression.create(
            order_by_arg=self.order_by_arg.rewrite(column_replacements, should_render_table_alias),
            percentile_args=self.percentile_args,
        )

    @property
    def is_aggregate_function(self) -> bool:  # noqa: D102
        return True

    @property
    def lineage(self) -> SqlExpressionTreeLineage:  # noqa: D102
        return SqlExpressionTreeLineage.merge_iterable(
            tuple(x.lineage for x in self.parent_nodes) + (SqlExpressionTreeLineage(function_exprs=(self,)),)
        )

    def matches(self, other: SqlExpressionNode) -> bool:  # noqa: D102
        if not isinstance(other, SqlPercentileExpression):
            return False
        return self.percentile_args == other.percentile_args and self._parents_match(other)


class SqlWindowFunction(Enum):
    """Names of known SQL window functions like SUM(), RANK(), ROW_NUMBER().

    Values are the SQL string to be used in rendering.
    """

    FIRST_VALUE = "FIRST_VALUE"
    LAST_VALUE = "LAST_VALUE"
    AVERAGE = "AVG"
    ROW_NUMBER = "ROW_NUMBER"
    LEAD = "LEAD"

    @property
    def requires_ordering(self) -> bool:
        """Asserts whether or not ordering the window function will have an impact on the resulting value."""
        if (
            self is SqlWindowFunction.FIRST_VALUE
            or self is SqlWindowFunction.LAST_VALUE
            or self is SqlWindowFunction.ROW_NUMBER
            or self is SqlWindowFunction.LEAD
        ):
            return True
        elif self is SqlWindowFunction.AVERAGE:
            return False
        else:
            assert_values_exhausted(self)

    @property
    def allows_frame_clause(self) -> bool:
        """Whether the function allows a frame clause, e.g., 'ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING'."""
        if (
            self is SqlWindowFunction.FIRST_VALUE
            or self is SqlWindowFunction.LAST_VALUE
            or self is SqlWindowFunction.AVERAGE
        ):
            return True
        if self is SqlWindowFunction.ROW_NUMBER or self is SqlWindowFunction.LEAD:
            return False
        else:
            assert_values_exhausted(self)

    @classmethod
    def get_window_function_for_period_agg(cls, period_agg: PeriodAggregation) -> SqlWindowFunction:
        """Get the window function to use for given period agg option."""
        if period_agg is PeriodAggregation.FIRST:
            return cls.FIRST_VALUE
        elif period_agg is PeriodAggregation.LAST:
            return cls.LAST_VALUE
        elif period_agg is PeriodAggregation.AVERAGE:
            return cls.AVERAGE
        else:
            assert_values_exhausted(period_agg)


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


@dataclass(frozen=True, eq=False)
class SqlWindowFunctionExpression(SqlFunctionExpression):
    """A window function expression like SUM(foo) OVER bar.

    Attributes:
        sql_function: The function that this represents.
        sql_function_args: The arguments that should go into the function. e.g. for "CONCAT(a, b)", the arg
                           expressions should be "a" and "b".
        partition_by_args: The arguments to partition the rows. e.g. PARTITION BY expr1, expr2,
                           the args are "expr1", "expr2".
        order_by_args: The expr to order the partitions by.
    """

    sql_function: SqlWindowFunction
    sql_function_args: Sequence[SqlExpressionNode]
    partition_by_args: Sequence[SqlExpressionNode]
    order_by_args: Sequence[SqlWindowOrderByArgument]

    @staticmethod
    def create(  # noqa: D102
        sql_function: SqlWindowFunction,
        sql_function_args: Sequence[SqlExpressionNode] = (),
        partition_by_args: Sequence[SqlExpressionNode] = (),
        order_by_args: Sequence[SqlWindowOrderByArgument] = (),
    ) -> SqlWindowFunctionExpression:
        parent_nodes: List[SqlExpressionNode] = []
        if sql_function_args:
            parent_nodes.extend(sql_function_args)
        if partition_by_args:
            parent_nodes.extend(partition_by_args)
        if order_by_args:
            parent_nodes.extend([x.expr for x in order_by_args])
        return SqlWindowFunctionExpression(
            parent_nodes=tuple(parent_nodes),
            sql_function=sql_function,
            sql_function_args=tuple(sql_function_args),
            partition_by_args=tuple(partition_by_args),
            order_by_args=tuple(order_by_args),
        )

    @classmethod
    def id_prefix(cls) -> IdPrefix:  # noqa: D102
        return StaticIdPrefix.SQL_EXPR_WINDOW_FUNCTION_ID_PREFIX

    @property
    def requires_parenthesis(self) -> bool:  # noqa: D102
        return False

    def accept(self, visitor: SqlExpressionNodeVisitor[VisitorOutputT]) -> VisitorOutputT:  # noqa: D102
        return visitor.visit_window_function_expr(self)

    @property
    def description(self) -> str:  # noqa: D102
        return f"{self.sql_function.value} Window Function Expression"

    @property
    def displayed_properties(self) -> Sequence[DisplayedProperty]:  # noqa: D102
        return (
            tuple(super().displayed_properties)
            + (DisplayedProperty("function", self.sql_function),)
            + tuple(DisplayedProperty("argument", x) for x in self.sql_function_args)
            + tuple(DisplayedProperty("partition_by_argument", x) for x in self.partition_by_args)
            + tuple(DisplayedProperty("order_by_argument", x) for x in self.order_by_args)
        )

    @property
    def is_aggregate_function(self) -> bool:  # noqa: D102
        return False

    @override
    def pretty_format(self, format_context: PrettyFormatContext) -> Optional[str]:
        return f"{self.__class__.__name__}(node_id={self.node_id}, sql_function={self.sql_function.name})"

    def rewrite(  # noqa: D102
        self,
        column_replacements: Optional[SqlColumnReplacements] = None,
        should_render_table_alias: Optional[bool] = None,
    ) -> SqlExpressionNode:
        return SqlWindowFunctionExpression.create(
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
    def lineage(self) -> SqlExpressionTreeLineage:  # noqa: D102
        return SqlExpressionTreeLineage.merge_iterable(
            tuple(x.lineage for x in self.parent_nodes) + (SqlExpressionTreeLineage(function_exprs=(self,)),)
        )

    @property
    def as_window_function_expression(self) -> Optional[SqlWindowFunctionExpression]:  # noqa: D102
        return self

    def matches(self, other: SqlExpressionNode) -> bool:  # noqa: D102
        if not isinstance(other, SqlWindowFunctionExpression):
            return False
        return (
            self.sql_function == other.sql_function
            and self.order_by_args == other.order_by_args
            and self.partition_by_args == other.partition_by_args
            and self.sql_function_args == other.sql_function_args
        )

    @property
    def is_verbose(self) -> bool:  # noqa: D102
        return True


@dataclass(frozen=True, eq=False)
class SqlNullExpression(SqlExpressionNode):
    """Represents NULL."""

    @staticmethod
    def create() -> SqlNullExpression:  # noqa: D102
        return SqlNullExpression(
            parent_nodes=(),
        )

    @classmethod
    def id_prefix(cls) -> IdPrefix:  # noqa: D102
        return StaticIdPrefix.SQL_EXPR_NULL_PREFIX

    @property
    def requires_parenthesis(self) -> bool:  # noqa: D102
        return False

    def accept(self, visitor: SqlExpressionNodeVisitor[VisitorOutputT]) -> VisitorOutputT:  # noqa: D102
        return visitor.visit_null_expr(self)

    @property
    def description(self) -> str:  # noqa: D102
        return "NULL Expression"

    def rewrite(  # noqa: D102
        self,
        column_replacements: Optional[SqlColumnReplacements] = None,
        should_render_table_alias: Optional[bool] = None,
    ) -> SqlExpressionNode:
        return self

    @property
    def lineage(self) -> SqlExpressionTreeLineage:  # noqa: D102
        return SqlExpressionTreeLineage(other_exprs=(self,))

    def matches(self, other: SqlExpressionNode) -> bool:  # noqa: D102
        return isinstance(other, SqlNullExpression)


class SqlLogicalOperator(Enum):
    """List all supported binary logical operator expressions.

    Value is the SQL string used when rendering the operator.
    """

    AND = "AND"
    OR = "OR"


@dataclass(frozen=True, eq=False)
class SqlLogicalExpression(SqlExpressionNode):
    """A logical expression like "a AND b AND c"."""

    operator: SqlLogicalOperator
    args: Tuple[SqlExpressionNode, ...]

    @staticmethod
    def create(operator: SqlLogicalOperator, args: Sequence[SqlExpressionNode]) -> SqlLogicalExpression:  # noqa: D102
        return SqlLogicalExpression(
            parent_nodes=tuple(args),
            operator=operator,
            args=tuple(args),
        )

    @classmethod
    def id_prefix(cls) -> IdPrefix:  # noqa: D102
        return StaticIdPrefix.SQL_EXPR_LOGICAL_OPERATOR_PREFIX

    @property
    def requires_parenthesis(self) -> bool:  # noqa: D102
        return True

    def accept(self, visitor: SqlExpressionNodeVisitor[VisitorOutputT]) -> VisitorOutputT:  # noqa: D102
        return visitor.visit_logical_expr(self)

    @property
    def description(self) -> str:  # noqa: D102
        return f"Logical Operator {self.operator.value}"

    def rewrite(  # noqa: D102
        self,
        column_replacements: Optional[SqlColumnReplacements] = None,
        should_render_table_alias: Optional[bool] = None,
    ) -> SqlExpressionNode:
        return SqlLogicalExpression.create(
            operator=self.operator,
            args=tuple(x.rewrite(column_replacements, should_render_table_alias) for x in self.args),
        )

    @property
    def lineage(self) -> SqlExpressionTreeLineage:  # noqa: D102
        return SqlExpressionTreeLineage.merge_iterable(
            tuple(x.lineage for x in self.parent_nodes) + (SqlExpressionTreeLineage(other_exprs=(self,)),)
        )

    def matches(self, other: SqlExpressionNode) -> bool:  # noqa: D102
        if not isinstance(other, SqlLogicalExpression):
            return False
        return self.operator == other.operator and self._parents_match(other)


@dataclass(frozen=True, eq=False)
class SqlIsNullExpression(SqlExpressionNode):
    """An IS NULL expression like "foo IS NULL"."""

    arg: SqlExpressionNode

    @staticmethod
    def create(arg: SqlExpressionNode) -> SqlIsNullExpression:  # noqa: D102
        return SqlIsNullExpression(
            parent_nodes=(arg,),
            arg=arg,
        )

    @classmethod
    def id_prefix(cls) -> IdPrefix:  # noqa: D102
        return StaticIdPrefix.SQL_EXPR_IS_NULL_PREFIX

    @property
    def requires_parenthesis(self) -> bool:  # noqa: D102
        return True

    def accept(self, visitor: SqlExpressionNodeVisitor[VisitorOutputT]) -> VisitorOutputT:  # noqa: D102
        return visitor.visit_is_null_expr(self)

    @property
    def description(self) -> str:  # noqa: D102
        return "IS NULL Expression"

    def rewrite(  # noqa: D102
        self,
        column_replacements: Optional[SqlColumnReplacements] = None,
        should_render_table_alias: Optional[bool] = None,
    ) -> SqlExpressionNode:
        return SqlIsNullExpression.create(arg=self.arg.rewrite(column_replacements, should_render_table_alias))

    @property
    def lineage(self) -> SqlExpressionTreeLineage:  # noqa: D102
        return SqlExpressionTreeLineage.merge_iterable(
            [self.arg.lineage, SqlExpressionTreeLineage(other_exprs=(self,))]
        )

    def matches(self, other: SqlExpressionNode) -> bool:  # noqa: D102
        if not isinstance(other, SqlIsNullExpression):
            return False
        return self._parents_match(other)


@dataclass(frozen=True, eq=False)
class SqlSubtractTimeIntervalExpression(SqlExpressionNode):
    """Represents an interval subtraction from a given timestamp.

    This node contains the information required to produce a SQL statement which subtracts an interval with the given
    count and granularity (which together define the interval duration) from the input timestamp expression. The return
    value from the SQL rendering for this expression should be a timestamp expression offset from the initial input
    value.
    """

    arg: SqlExpressionNode
    count: int
    granularity: TimeGranularity

    @staticmethod
    def create(  # noqa: D102
        arg: SqlExpressionNode,
        count: int,
        granularity: TimeGranularity,
    ) -> SqlSubtractTimeIntervalExpression:
        return SqlSubtractTimeIntervalExpression(
            parent_nodes=(arg,),
            arg=arg,
            count=count,
            granularity=granularity,
        )

    @classmethod
    def id_prefix(cls) -> IdPrefix:  # noqa: D102
        return StaticIdPrefix.SQL_EXPR_SUBTRACT_TIME_INTERVAL_PREFIX

    @property
    def requires_parenthesis(self) -> bool:  # noqa: D102
        return False

    def accept(self, visitor: SqlExpressionNodeVisitor[VisitorOutputT]) -> VisitorOutputT:  # noqa: D102
        return visitor.visit_subtract_time_interval_expr(self)

    @property
    def description(self) -> str:  # noqa: D102
        return "Subtract time interval"

    def rewrite(  # noqa: D102
        self,
        column_replacements: Optional[SqlColumnReplacements] = None,
        should_render_table_alias: Optional[bool] = None,
    ) -> SqlExpressionNode:
        return SqlSubtractTimeIntervalExpression.create(
            arg=self.arg.rewrite(column_replacements, should_render_table_alias),
            count=self.count,
            granularity=self.granularity,
        )

    @property
    def lineage(self) -> SqlExpressionTreeLineage:  # noqa: D102
        return SqlExpressionTreeLineage.merge_iterable(
            tuple(x.lineage for x in self.parent_nodes) + (SqlExpressionTreeLineage(other_exprs=(self,)),)
        )

    def matches(self, other: SqlExpressionNode) -> bool:  # noqa: D102
        if not isinstance(other, SqlSubtractTimeIntervalExpression):
            return False
        return self.count == other.count and self.granularity == other.granularity and self._parents_match(other)


@dataclass(frozen=True, eq=False)
class SqlAddTimeExpression(SqlExpressionNode):
    """Add a time interval expr to a timestamp."""

    arg: SqlExpressionNode
    count_expr: SqlExpressionNode
    granularity: TimeGranularity

    @staticmethod
    def create(  # noqa: D102
        arg: SqlExpressionNode,
        count_expr: SqlExpressionNode,
        granularity: TimeGranularity,
    ) -> SqlAddTimeExpression:
        return SqlAddTimeExpression(
            parent_nodes=(arg, count_expr),
            arg=arg,
            count_expr=count_expr,
            granularity=granularity,
        )

    @classmethod
    def id_prefix(cls) -> IdPrefix:  # noqa: D102
        return StaticIdPrefix.SQL_EXPR_ADD_TIME_PREFIX

    @property
    def requires_parenthesis(self) -> bool:  # noqa: D102
        return False

    def accept(self, visitor: SqlExpressionNodeVisitor[VisitorOutputT]) -> VisitorOutputT:  # noqa: D102
        return visitor.visit_add_time_expr(self)

    @property
    def description(self) -> str:  # noqa: D102
        return "Add time interval"

    def rewrite(  # noqa: D102
        self,
        column_replacements: Optional[SqlColumnReplacements] = None,
        should_render_table_alias: Optional[bool] = None,
    ) -> SqlExpressionNode:
        return SqlAddTimeExpression.create(
            arg=self.arg.rewrite(column_replacements, should_render_table_alias),
            count_expr=self.count_expr.rewrite(column_replacements, should_render_table_alias),
            granularity=self.granularity,
        )

    @property
    def lineage(self) -> SqlExpressionTreeLineage:  # noqa: D102
        return SqlExpressionTreeLineage.merge_iterable(
            tuple(x.lineage for x in self.parent_nodes) + (SqlExpressionTreeLineage(other_exprs=(self,)),)
        )

    def matches(self, other: SqlExpressionNode) -> bool:  # noqa: D102
        if not isinstance(other, SqlAddTimeExpression):
            return False
        return self.count_expr == other.count_expr and self.granularity == other.granularity and self.arg == other.arg


@dataclass(frozen=True, eq=False)
class SqlCastToTimestampExpression(SqlExpressionNode):
    """Cast to the timestamp type like CAST('2020-01-01' AS TIMESTAMP)."""

    arg: SqlExpressionNode

    @staticmethod
    def create(arg: SqlExpressionNode) -> SqlCastToTimestampExpression:  # noqa: D102
        return SqlCastToTimestampExpression(
            parent_nodes=(arg,),
            arg=arg,
        )

    @classmethod
    def id_prefix(cls) -> IdPrefix:  # noqa: D102
        return StaticIdPrefix.SQL_EXPR_CAST_TO_TIMESTAMP_PREFIX

    @property
    def requires_parenthesis(self) -> bool:  # noqa: D102
        return False

    def accept(self, visitor: SqlExpressionNodeVisitor[VisitorOutputT]) -> VisitorOutputT:  # noqa: D102
        return visitor.visit_cast_to_timestamp_expr(self)

    @property
    def description(self) -> str:  # noqa: D102
        return "Cast to Timestamp"

    def rewrite(  # noqa: D102
        self,
        column_replacements: Optional[SqlColumnReplacements] = None,
        should_render_table_alias: Optional[bool] = None,
    ) -> SqlExpressionNode:
        return SqlCastToTimestampExpression.create(arg=self.arg.rewrite(column_replacements, should_render_table_alias))

    @property
    def lineage(self) -> SqlExpressionTreeLineage:  # noqa: D102
        return SqlExpressionTreeLineage.merge_iterable(
            tuple(x.lineage for x in self.parent_nodes) + (SqlExpressionTreeLineage(other_exprs=(self,)),)
        )

    def matches(self, other: SqlExpressionNode) -> bool:  # noqa: D102
        if not isinstance(other, SqlCastToTimestampExpression):
            return False
        return self._parents_match(other)


@dataclass(frozen=True, eq=False)
class SqlDateTruncExpression(SqlExpressionNode):
    """Apply a date trunc to a column like CAST('2020-01-01' AS TIMESTAMP)."""

    time_granularity: TimeGranularity
    arg: SqlExpressionNode

    @staticmethod
    def create(time_granularity: TimeGranularity, arg: SqlExpressionNode) -> SqlDateTruncExpression:  # noqa: D102
        return SqlDateTruncExpression(
            parent_nodes=(arg,),
            time_granularity=time_granularity,
            arg=arg,
        )

    @classmethod
    def id_prefix(cls) -> IdPrefix:  # noqa: D102
        return StaticIdPrefix.SQL_EXPR_DATE_TRUNC

    @property
    def requires_parenthesis(self) -> bool:  # noqa: D102
        return False

    def accept(self, visitor: SqlExpressionNodeVisitor[VisitorOutputT]) -> VisitorOutputT:  # noqa: D102
        return visitor.visit_date_trunc_expr(self)

    @property
    def description(self) -> str:  # noqa: D102
        return f"DATE_TRUNC() to {self.time_granularity}"

    def rewrite(  # noqa: D102
        self,
        column_replacements: Optional[SqlColumnReplacements] = None,
        should_render_table_alias: Optional[bool] = None,
    ) -> SqlExpressionNode:
        return SqlDateTruncExpression.create(
            time_granularity=self.time_granularity, arg=self.arg.rewrite(column_replacements, should_render_table_alias)
        )

    @property
    def lineage(self) -> SqlExpressionTreeLineage:  # noqa: D102
        return SqlExpressionTreeLineage.merge_iterable(
            tuple(x.lineage for x in self.parent_nodes) + (SqlExpressionTreeLineage(other_exprs=(self,)),)
        )

    def matches(self, other: SqlExpressionNode) -> bool:  # noqa: D102
        if not isinstance(other, SqlDateTruncExpression):
            return False
        return self.time_granularity == other.time_granularity and self._parents_match(other)


@dataclass(frozen=True, eq=False)
class SqlExtractExpression(SqlExpressionNode):
    """Extract a date part from a time expression.

    Attributes:
        date_part: The date part to extract.
        arg: The expression to extract from.
    """

    date_part: DatePart
    arg: SqlExpressionNode

    @staticmethod
    def create(  # noqa: D102
        date_part: DatePart,
        arg: SqlExpressionNode,
    ) -> SqlExtractExpression:
        return SqlExtractExpression(
            parent_nodes=(arg,),
            date_part=date_part,
            arg=arg,
        )

    @classmethod
    def id_prefix(cls) -> IdPrefix:  # noqa: D102
        return StaticIdPrefix.SQL_EXPR_EXTRACT

    @property
    def requires_parenthesis(self) -> bool:  # noqa: D102
        return False

    def accept(self, visitor: SqlExpressionNodeVisitor[VisitorOutputT]) -> VisitorOutputT:  # noqa: D102
        return visitor.visit_extract_expr(self)

    @property
    def description(self) -> str:  # noqa: D102
        return f"Extract {self.date_part.name}"

    def rewrite(  # noqa: D102
        self,
        column_replacements: Optional[SqlColumnReplacements] = None,
        should_render_table_alias: Optional[bool] = None,
    ) -> SqlExpressionNode:
        return SqlExtractExpression.create(
            date_part=self.date_part, arg=self.arg.rewrite(column_replacements, should_render_table_alias)
        )

    @property
    def lineage(self) -> SqlExpressionTreeLineage:  # noqa: D102
        return SqlExpressionTreeLineage.merge_iterable(
            tuple(x.lineage for x in self.parent_nodes) + (SqlExpressionTreeLineage(other_exprs=(self,)),)
        )

    def matches(self, other: SqlExpressionNode) -> bool:  # noqa: D102
        if not isinstance(other, SqlExtractExpression):
            return False
        return self.date_part == other.date_part and self._parents_match(other)


@dataclass(frozen=True, eq=False)
class SqlRatioComputationExpression(SqlExpressionNode):
    """Node for expressing Ratio metrics to allow for appropriate casting to float/double in each engine.

    In the future, we might wish to break this up into a set of nodes, e.g., SqlCastExpression and SqlMathExpression
    or even add CAST to SqlFunctionExpression. However, at this time the only mathematical operation we encode
    is division, and we only use that for ratios. Similarly, the only times we do typecasting are when we are
    coercing timestamps (already handled) or computing ratio metrics.

    Attributes:
        numerator: The expression for the numerator in the ratio.
        denominator: The expression for the denominator in the ratio.
    """

    numerator: SqlExpressionNode
    denominator: SqlExpressionNode

    @staticmethod
    def create(  # noqa: D102
        numerator: SqlExpressionNode,
        denominator: SqlExpressionNode,
    ) -> SqlRatioComputationExpression:
        return SqlRatioComputationExpression(
            parent_nodes=(numerator, denominator),
            numerator=numerator,
            denominator=denominator,
        )

    @classmethod
    def id_prefix(cls) -> IdPrefix:  # noqa: D102
        return StaticIdPrefix.SQL_EXPR_RATIO_COMPUTATION

    @property
    def requires_parenthesis(self) -> bool:  # noqa: D102
        return False

    def accept(self, visitor: SqlExpressionNodeVisitor[VisitorOutputT]) -> VisitorOutputT:  # noqa: D102
        return visitor.visit_ratio_computation_expr(self)

    @property
    def description(self) -> str:  # noqa: D102
        return "Divide numerator by denominator, with appropriate casting"

    def rewrite(  # noqa: D102
        self,
        column_replacements: Optional[SqlColumnReplacements] = None,
        should_render_table_alias: Optional[bool] = None,
    ) -> SqlExpressionNode:
        return SqlRatioComputationExpression.create(
            numerator=self.numerator.rewrite(column_replacements, should_render_table_alias),
            denominator=self.denominator.rewrite(column_replacements, should_render_table_alias),
        )

    @property
    def lineage(self) -> SqlExpressionTreeLineage:  # noqa: D102
        return SqlExpressionTreeLineage.merge_iterable(
            tuple(x.lineage for x in self.parent_nodes) + (SqlExpressionTreeLineage(other_exprs=(self,)),)
        )

    def matches(self, other: SqlExpressionNode) -> bool:  # noqa: D102
        if not isinstance(other, SqlRatioComputationExpression):
            return False
        return self._parents_match(other)


@dataclass(frozen=True, eq=False)
class SqlBetweenExpression(SqlExpressionNode):
    """A BETWEEN clause like `column BETWEEN val1 AND val2`.

    Attributes:
        column_arg: The column or expression to apply the BETWEEN clause.
        start_expr: The start expression of the BETWEEN clause.
        end_expr: The end expression of the BETWEEN clause.
    """

    column_arg: SqlExpressionNode
    start_expr: SqlExpressionNode
    end_expr: SqlExpressionNode

    @staticmethod
    def create(  # noqa: D102
        column_arg: SqlExpressionNode,
        start_expr: SqlExpressionNode,
        end_expr: SqlExpressionNode,
    ) -> SqlBetweenExpression:
        return SqlBetweenExpression(
            parent_nodes=(column_arg, start_expr, end_expr),
            column_arg=column_arg,
            start_expr=start_expr,
            end_expr=end_expr,
        )

    @classmethod
    def id_prefix(cls) -> IdPrefix:  # noqa: D102
        return StaticIdPrefix.SQL_EXPR_BETWEEN_PREFIX

    @property
    def requires_parenthesis(self) -> bool:  # noqa: D102
        return False

    def accept(self, visitor: SqlExpressionNodeVisitor[VisitorOutputT]) -> VisitorOutputT:  # noqa: D102
        return visitor.visit_between_expr(self)

    @property
    def description(self) -> str:  # noqa: D102
        return "BETWEEN operator"

    def rewrite(  # noqa: D102
        self,
        column_replacements: Optional[SqlColumnReplacements] = None,
        should_render_table_alias: Optional[bool] = None,
    ) -> SqlExpressionNode:
        return SqlBetweenExpression.create(
            column_arg=self.column_arg.rewrite(column_replacements, should_render_table_alias),
            start_expr=self.start_expr.rewrite(column_replacements, should_render_table_alias),
            end_expr=self.end_expr.rewrite(column_replacements, should_render_table_alias),
        )

    @property
    def lineage(self) -> SqlExpressionTreeLineage:  # noqa: D102
        return SqlExpressionTreeLineage.merge_iterable(
            tuple(x.lineage for x in self.parent_nodes) + (SqlExpressionTreeLineage(other_exprs=(self,)),)
        )

    def matches(self, other: SqlExpressionNode) -> bool:  # noqa: D102
        if not isinstance(other, SqlBetweenExpression):
            return False
        return self._parents_match(other)


@dataclass(frozen=True, eq=False)
class SqlGenerateUuidExpression(SqlExpressionNode):
    """Renders a SQL to generate a random UUID, which is non-deterministic."""

    @staticmethod
    def create() -> SqlGenerateUuidExpression:  # noqa: D102
        return SqlGenerateUuidExpression(
            parent_nodes=(),
        )

    @classmethod
    def id_prefix(cls) -> IdPrefix:  # noqa: D102
        return StaticIdPrefix.SQL_EXPR_GENERATE_UUID_PREFIX

    def accept(self, visitor: SqlExpressionNodeVisitor[VisitorOutputT]) -> VisitorOutputT:  # noqa: D102
        return visitor.visit_generate_uuid_expr(self)

    @property
    def description(self) -> str:  # noqa: D102
        return "Generate a universally unique identifier"

    @property
    def displayed_properties(self) -> Sequence[DisplayedProperty]:  # noqa: D102
        return super().displayed_properties

    @property
    def requires_parenthesis(self) -> bool:  # noqa: D102
        return False

    @property
    def bind_parameter_set(self) -> SqlBindParameterSet:  # noqa: D102
        return SqlBindParameterSet()

    def __repr__(self) -> str:  # noqa: D105
        return f"{self.__class__.__name__}(node_id={self.node_id})"

    def rewrite(  # noqa: D102
        self,
        column_replacements: Optional[SqlColumnReplacements] = None,
        should_render_table_alias: Optional[bool] = None,
    ) -> SqlExpressionNode:
        return self

    @property
    def lineage(self) -> SqlExpressionTreeLineage:  # noqa: D102
        return SqlExpressionTreeLineage(other_exprs=(self,))

    def matches(self, other: SqlExpressionNode) -> bool:  # noqa: D102
        return False


@dataclass(frozen=True, eq=False)
class SqlCaseExpression(SqlExpressionNode):
    """Renders a CASE WHEN expression."""

    when_to_then_exprs: Dict[SqlExpressionNode, SqlExpressionNode]
    else_expr: Optional[SqlExpressionNode]

    @staticmethod
    def create(  # noqa: D102
        when_to_then_exprs: Dict[SqlExpressionNode, SqlExpressionNode], else_expr: Optional[SqlExpressionNode] = None
    ) -> SqlCaseExpression:
        parent_nodes: Tuple[SqlExpressionNode, ...] = ()
        for when, then in when_to_then_exprs.items():
            parent_nodes += (when, then)

        if else_expr:
            parent_nodes += (else_expr,)

        return SqlCaseExpression(parent_nodes=parent_nodes, when_to_then_exprs=when_to_then_exprs, else_expr=else_expr)

    @classmethod
    def id_prefix(cls) -> IdPrefix:  # noqa: D102
        return StaticIdPrefix.SQL_EXPR_CASE_PREFIX

    def accept(self, visitor: SqlExpressionNodeVisitor[VisitorOutputT]) -> VisitorOutputT:  # noqa: D102
        return visitor.visit_case_expr(self)

    @property
    def description(self) -> str:  # noqa: D102
        return "Case expression"

    @property
    def displayed_properties(self) -> Sequence[DisplayedProperty]:  # noqa: D102
        return super().displayed_properties

    @property
    def requires_parenthesis(self) -> bool:  # noqa: D102
        return False

    @property
    def bind_parameter_set(self) -> SqlBindParameterSet:  # noqa: D102
        return SqlBindParameterSet()

    def __repr__(self) -> str:  # noqa: D105
        return f"{self.__class__.__name__}(node_id={self.node_id})"

    def rewrite(  # noqa: D102
        self,
        column_replacements: Optional[SqlColumnReplacements] = None,
        should_render_table_alias: Optional[bool] = None,
    ) -> SqlExpressionNode:
        return SqlCaseExpression.create(
            when_to_then_exprs={
                when.rewrite(column_replacements, should_render_table_alias): then.rewrite(
                    column_replacements, should_render_table_alias
                )
                for when, then in self.when_to_then_exprs.items()
            },
            else_expr=(
                self.else_expr.rewrite(column_replacements, should_render_table_alias) if self.else_expr else None
            ),
        )

    @property
    def lineage(self) -> SqlExpressionTreeLineage:  # noqa: D102
        return SqlExpressionTreeLineage.merge_iterable(
            tuple(x.lineage for x in self.parent_nodes) + (SqlExpressionTreeLineage(other_exprs=(self,)),)
        )

    def matches(self, other: SqlExpressionNode) -> bool:  # noqa: D102
        if not isinstance(other, SqlCaseExpression):
            return False
        return self.when_to_then_exprs == other.when_to_then_exprs and self.else_expr == other.else_expr

    @property
    def is_verbose(self) -> bool:  # noqa: D102
        return True


class SqlArithmeticOperator(Enum):
    """Arithmetic operator used to do math in a SQL expression."""

    ADD = "+"
    SUBTRACT = "-"
    MULTIPLY = "*"
    DIVIDE = "/"


@dataclass(frozen=True, eq=False)
class SqlArithmeticExpression(SqlExpressionNode):
    """An arithmetic expression using +, -, *, /.

    e.g. my_table.my_column + my_table.other_column

    Attributes:
        left_expr: The expression on the left side of the operator
        operator: The operator to use on the expressions
        right_expr: The expression on the right side of the operator
    """

    left_expr: SqlExpressionNode
    operator: SqlArithmeticOperator
    right_expr: SqlExpressionNode

    @staticmethod
    def create(  # noqa: D102
        left_expr: SqlExpressionNode, operator: SqlArithmeticOperator, right_expr: SqlExpressionNode
    ) -> SqlArithmeticExpression:
        return SqlArithmeticExpression(
            parent_nodes=(left_expr, right_expr), left_expr=left_expr, operator=operator, right_expr=right_expr
        )

    @classmethod
    def id_prefix(cls) -> IdPrefix:  # noqa: D102
        return StaticIdPrefix.SQL_EXPR_ARITHMETIC_PREFIX

    def accept(self, visitor: SqlExpressionNodeVisitor[VisitorOutputT]) -> VisitorOutputT:  # noqa: D102
        return visitor.visit_arithmetic_expr(self)

    @property
    def description(self) -> str:  # noqa: D102
        return "Arithmetic Expression"

    @property
    def displayed_properties(self) -> Sequence[DisplayedProperty]:  # noqa: D102
        return tuple(super().displayed_properties) + (
            DisplayedProperty("left_expr", self.left_expr),
            DisplayedProperty("operator", self.operator.value),
            DisplayedProperty("right_expr", self.right_expr),
        )

    @property
    def requires_parenthesis(self) -> bool:  # noqa: D102
        return True

    def rewrite(  # noqa: D102
        self,
        column_replacements: Optional[SqlColumnReplacements] = None,
        should_render_table_alias: Optional[bool] = None,
    ) -> SqlExpressionNode:
        return SqlArithmeticExpression.create(
            left_expr=self.left_expr.rewrite(column_replacements, should_render_table_alias),
            operator=self.operator,
            right_expr=self.right_expr.rewrite(column_replacements, should_render_table_alias),
        )

    @property
    def lineage(self) -> SqlExpressionTreeLineage:  # noqa: D102
        return SqlExpressionTreeLineage.merge_iterable(
            tuple(x.lineage for x in self.parent_nodes) + (SqlExpressionTreeLineage(other_exprs=(self,)),)
        )

    def matches(self, other: SqlExpressionNode) -> bool:  # noqa: D102
        if not isinstance(other, SqlArithmeticExpression):
            return False
        return self.operator == other.operator and self._parents_match(other)
