from __future__ import annotations

import logging
import re
from typing import Collection, Optional

from metricflow_semantics.errors.error_classes import UnsupportedEngineFeatureError
from metricflow_semantics.sql.sql_bind_parameters import SqlBindParameterSet
from metricflow_semantics.sql.sql_exprs import (
    SqlAddTimeExpression,
    SqlCastToTimestampExpression,
    SqlDateTruncExpression,
    SqlExtractExpression,
    SqlGenerateUuidExpression,
    SqlPercentileExpression,
    SqlPercentileFunctionType,
    SqlSubtractTimeIntervalExpression,
)
from typing_extensions import override

from metricflow.protocols.sql_client import SqlEngine
from metricflow.sql.render.expr_renderer import (
    DefaultSqlExpressionRenderer,
    SqlExpressionRenderer,
    SqlExpressionRenderResult,
)
from metricflow.sql.render.sql_plan_renderer import DefaultSqlPlanRenderer, SqlPlanRenderResult
from metricflow.sql.sql_plan import SqlPlan
from metricflow_semantic_interfaces.enum_extension import assert_values_exhausted
from metricflow_semantic_interfaces.type_enums.date_part import DatePart
from metricflow_semantic_interfaces.type_enums.time_granularity import TimeGranularity

logger = logging.getLogger(__name__)

# Query-level contract so unmatched LEFT/FULL OUTER JOIN cells are SQL NULL.
# ClickHouse defaults to filling those cells with type defaults (0, ''), which
# breaks MetricFlow fill-nulls / ratio logic. This must travel with the statement
# (`SETTINGS ...`), not a session `SET`: HTTP connections often have no session,
# and readonly ClickHouse Cloud users can run query SETTINGS but not SET.
# https://clickhouse.com/docs/operations/settings/settings#join_use_nulls
CLICKHOUSE_JOIN_USE_NULLS_SETTING = "join_use_nulls = 1"

_SETTINGS_CLAUSE = re.compile(r"(?i)\bSETTINGS\b(?=\s*[A-Za-z_][A-Za-z0-9_]*\s*=)")
_JOIN_USE_NULLS_KEY = re.compile(r"(?i)\bjoin_use_nulls\s*=")
# Value is a bare token or quoted value. ClickHouse accepts single-quoted setting values; matching double-quoted
# values lets us repair them because ClickHouse interprets double quotes as identifier delimiters.
# Applied to masked SQL, where quoted contents are blanked but the quotes themselves are kept.
_JOIN_USE_NULLS_ASSIGNMENT = re.compile(r"""(?i)\bjoin_use_nulls\s*=\s*([A-Za-z0-9_.+-]+|'[^']*'|"[^"]*")""")
_JOIN_USE_NULLS_ENABLED_VALUES = frozenset({"1", "true"})
_DOLLAR_QUOTE_DELIMITER = re.compile(r"\$(?:[A-Za-z_][A-Za-z0-9_]*)?\$")


def _mask_non_top_level_sql(sql: str) -> str:
    """Mask quoted, commented, and parenthesized SQL while preserving offsets.

    ClickHouse allows SETTINGS after a SELECT, including the SELECT inside a
    CREATE ... AS statement. A lightweight scanner is enough here because we
    only need to distinguish that top-level clause from identical text in a
    literal, comment, function argument, or nested subquery.
    """
    masked = [" "] * len(sql)
    depth = 0
    index = 0

    while index < len(sql):
        if sql.startswith("--", index) or sql[index] == "#":
            newline_index = sql.find("\n", index)
            index = len(sql) if newline_index == -1 else newline_index
            continue
        if sql.startswith("/*", index):
            comment_end_index = sql.find("*/", index + 2)
            index = len(sql) if comment_end_index == -1 else comment_end_index + 2
            continue

        dollar_quote_match = _DOLLAR_QUOTE_DELIMITER.match(sql, index)
        if dollar_quote_match:
            delimiter = dollar_quote_match.group(0)
            quote_end_index = sql.find(delimiter, dollar_quote_match.end())
            if depth == 0:
                masked[index : dollar_quote_match.end()] = delimiter
                if quote_end_index != -1:
                    masked[quote_end_index : quote_end_index + len(delimiter)] = delimiter
            index = len(sql) if quote_end_index == -1 else quote_end_index + len(delimiter)
            continue

        character = sql[index]
        if character in {"'", '"', "`"}:
            quote = character
            if depth == 0:
                masked[index] = character
            index += 1
            while index < len(sql):
                if sql[index] == "\\":
                    index += 2
                elif sql[index] == quote and index + 1 < len(sql) and sql[index + 1] == quote:
                    index += 2
                elif sql[index] == quote:
                    if depth == 0:
                        masked[index] = character
                    index += 1
                    break
                else:
                    index += 1
            continue

        if character == "(":
            if depth == 0:
                masked[index] = character
            depth += 1
        elif character == ")":
            depth = max(0, depth - 1)
            if depth == 0:
                masked[index] = character
        elif depth == 0:
            masked[index] = character
        index += 1

    return "".join(masked)


def _strip_trailing_statement_terminator(sql: str) -> str:
    """Strip trailing semicolons even when a comment follows them."""
    stripped = sql.rstrip()
    while True:
        masked = _mask_non_top_level_sql(stripped)
        final_code_index = len(masked.rstrip()) - 1
        if final_code_index < 0 or stripped[final_code_index] != ";":
            return stripped
        stripped = f"{stripped[:final_code_index]}{stripped[final_code_index + 1 :]}".rstrip()


def _last_settings_clause_index(sql: str) -> Optional[int]:
    """Index of the last ClickHouse SETTINGS clause, or None.

    Requires a top-level keyword followed by `name =` so nested queries and text
    inside strings or comments cannot be mistaken for the statement's clause.
    """
    matches = tuple(_SETTINGS_CLAUSE.finditer(_mask_non_top_level_sql(sql)))
    return matches[-1].start() if matches else None


def sql_has_join_use_nulls_setting(sql: str) -> bool:
    """True if a trailing ClickHouse SETTINGS clause already sets join_use_nulls."""
    stripped = _strip_trailing_statement_terminator(sql)
    index = _last_settings_clause_index(stripped)
    if index is None:
        return False
    return _JOIN_USE_NULLS_KEY.search(_mask_non_top_level_sql(stripped[index:])) is not None


def ensure_join_use_nulls_setting(sql: str) -> str:
    """Ensure compiled SQL carries `SETTINGS join_use_nulls = 1`.

    Inspects only SETTINGS clauses so a string or alias containing the identifier
    does not suppress the contract. If a trailing SETTINGS clause exists without
    this key, the key is merged into that clause (ClickHouse allows one SETTINGS
    list per statement).
    """
    stripped = _strip_trailing_statement_terminator(sql)
    index = _last_settings_clause_index(stripped)
    if index is None:
        return f"{stripped}\nSETTINGS {CLICKHOUSE_JOIN_USE_NULLS_SETTING}"
    settings_sql = stripped[index:]
    masked_settings_sql = _mask_non_top_level_sql(settings_sql)
    key_match = _JOIN_USE_NULLS_KEY.search(masked_settings_sql)
    if key_match:
        assignment_match = _JOIN_USE_NULLS_ASSIGNMENT.search(masked_settings_sql, key_match.start())
        if assignment_match is None:
            raise ValueError("ClickHouse join_use_nulls setting must have a scalar value")
        value_start, value_end = assignment_match.span(1)
        value = settings_sql[value_start:value_end].strip("'").lower()
        if value in _JOIN_USE_NULLS_ENABLED_VALUES:
            return stripped
        return f"{stripped[: index + value_start]}1{stripped[index + value_end :]}"

    settings_code_end = len(masked_settings_sql.rstrip())
    separator = " " if settings_sql[settings_code_end - 1] == "," else ", "
    insertion_index = index + settings_code_end
    sql_before_insertion = stripped[:insertion_index]
    sql_after_insertion = stripped[insertion_index:]
    return f"{sql_before_insertion}{separator}{CLICKHOUSE_JOIN_USE_NULLS_SETTING}{sql_after_insertion}"


def clickhouse_explain_statement(stmt: str) -> str:
    """Wrap `stmt` in the EXPLAIN form ClickHouse accepts for dry-run validation.

    EXPLAIN QUERY TREE is the analyzer-era check for SELECT/WITH. It rejects DDL
    such as CREATE TABLE AS; those use EXPLAIN SYNTAX instead.
    """
    clickhouse_stmt = stmt.strip().rstrip(";")
    head = clickhouse_stmt.split(None, 1)[0].upper() if clickhouse_stmt else ""
    prefix = "EXPLAIN QUERY TREE" if head in {"SELECT", "WITH"} else "EXPLAIN SYNTAX"
    return f"{prefix} {clickhouse_stmt}"


class ClickHouseSqlExpressionRenderer(DefaultSqlExpressionRenderer):
    """Expression renderer for the ClickHouse engine.

    ClickHouse has significant differences from standard SQL:
    - Uses toStartOf* functions instead of DATE_TRUNC
    - Parameterized aggregate functions (quantile(0.5)(column))
    - Different data type names (Nullable(Float64), Nullable(DateTime64(3)), String)
    - Case-sensitive function names

    Reference: https://clickhouse.com/docs/en/sql-reference/functions
    """

    sql_engine = SqlEngine.CLICKHOUSE

    @property
    @override
    def double_data_type(self) -> str:
        """ClickHouse CAST target for floats.

        Must be Nullable so CAST of SQL NULL after outer joins succeeds.
        `CAST(NULL AS Float64)` raises CANNOT_INSERT_NULL_IN_ORDINARY_COLUMN.
        """
        return "Nullable(Float64)"

    @property
    @override
    def timestamp_data_type(self) -> str:
        """ClickHouse CAST target for timestamps.

        Nullable for the same reason as `double_data_type`.
        """
        return "Nullable(DateTime64(3))"

    @property
    @override
    def supported_percentile_function_types(self) -> Collection[SqlPercentileFunctionType]:
        """ClickHouse supports multiple percentile function types.

        Reference: https://clickhouse.com/docs/en/sql-reference/aggregate-functions/reference/quantile
        """
        return {
            SqlPercentileFunctionType.CONTINUOUS,
            SqlPercentileFunctionType.DISCRETE,
            SqlPercentileFunctionType.APPROXIMATE_CONTINUOUS,
            SqlPercentileFunctionType.APPROXIMATE_DISCRETE,
        }

    @override
    def visit_date_trunc_expr(self, node: SqlDateTruncExpression) -> SqlExpressionRenderResult:
        """Render DATE_TRUNC for ClickHouse using toStartOf* functions.

        ClickHouse mapping:
        - day -> toStartOfDay
        - week -> toStartOfWeek (requires mode parameter)
        - month -> toStartOfMonth
        - quarter -> toStartOfQuarter
        - year -> toStartOfYear

        Reference: https://clickhouse.com/docs/en/sql-reference/functions/date-time-functions#tostartofday
        """
        self._validate_granularity_for_engine(node.time_granularity)

        arg_rendered = self.render_sql_expr(node.arg)

        # Map TimeGranularity to ClickHouse function
        granularity_map = {
            TimeGranularity.MILLISECOND: "toStartOfMillisecond",
            TimeGranularity.SECOND: "toStartOfSecond",
            TimeGranularity.MINUTE: "toStartOfMinute",
            TimeGranularity.HOUR: "toStartOfHour",
            TimeGranularity.DAY: "toStartOfDay",
            TimeGranularity.WEEK: "toStartOfWeek",  # Mode 1 = ISO week (Monday start)
            TimeGranularity.MONTH: "toStartOfMonth",
            TimeGranularity.QUARTER: "toStartOfQuarter",
            TimeGranularity.YEAR: "toStartOfYear",
        }

        function_name = granularity_map.get(node.time_granularity)
        if not function_name:
            raise UnsupportedEngineFeatureError(
                f"ClickHouse does not support time granularity {node.time_granularity.name}. "
                f"Supported granularities: {list(granularity_map.keys())}"
            )

        # toStartOfWeek requires a mode parameter (1 = ISO week, Monday start)
        if node.time_granularity is TimeGranularity.WEEK:
            sql = f"{function_name}({arg_rendered.sql}, 1)"
        else:
            sql = f"{function_name}({arg_rendered.sql})"

        return SqlExpressionRenderResult(
            sql=sql,
            bind_parameter_set=arg_rendered.bind_parameter_set,
        )

    @override
    def render_date_part(self, date_part: DatePart) -> str:
        """Render date part for ClickHouse extract functions.

        ClickHouse uses specific functions instead of EXTRACT:
        - year -> toYear
        - month -> toMonth
        - day -> toDayOfMonth
        - dayofweek -> toDayOfWeek (returns 1-7, Monday=1)
        - dayofyear -> toDayOfYear
        - week -> toISOWeek
        - quarter -> toQuarter

        Reference: https://clickhouse.com/docs/en/sql-reference/functions/date-time-functions#toyear-tomonth
        """
        date_part_map = {
            DatePart.YEAR: "toYear",
            DatePart.MONTH: "toMonth",
            DatePart.DAY: "toDayOfMonth",
            DatePart.DOW: "toDayOfWeek",  # Returns 1-7, Monday=1 (ISO standard)
            DatePart.DOY: "toDayOfYear",
            DatePart.QUARTER: "toQuarter",
        }

        return date_part_map.get(date_part, date_part.value)

    @override
    def visit_extract_expr(self, node: SqlExtractExpression) -> SqlExpressionRenderResult:
        """Render EXTRACT for ClickHouse using to* functions.

        ClickHouse doesn't have EXTRACT, so we use specific functions like
        toYear(), toMonth(), etc.
        """
        arg_rendered = self.render_sql_expr(node.arg)
        date_part_function = self.render_date_part(node.date_part)

        # Monday=1 matches MetricFlow's ISO DOW (EXTRACT(isodow ...)) on other engines.
        if node.date_part is DatePart.DOW:
            sql = f"{date_part_function}({arg_rendered.sql}, 0)"
        else:
            sql = f"{date_part_function}({arg_rendered.sql})"

        return SqlExpressionRenderResult(
            sql=sql,
            bind_parameter_set=arg_rendered.bind_parameter_set,
        )

    @override
    def visit_add_time_expr(self, node: SqlAddTimeExpression) -> SqlExpressionRenderResult:
        """Render time addition for ClickHouse using add* functions.

        ClickHouse functions:
        - day -> addDays
        - week -> addDays (multiply by 7)
        - month -> addMonths
        - quarter -> addMonths (multiply by 3)
        - year -> addYears

        Reference: https://clickhouse.com/docs/en/sql-reference/functions/date-time-functions#adddays
        """
        arg_rendered = self.render_sql_expr(node.arg)
        count_rendered = self.render_sql_expr(node.count_expr)

        granularity = node.granularity

        # Map granularity to ClickHouse function
        function_map = {
            TimeGranularity.MILLISECOND: "addMilliseconds",
            TimeGranularity.SECOND: "addSeconds",
            TimeGranularity.MINUTE: "addMinutes",
            TimeGranularity.HOUR: "addHours",
            TimeGranularity.DAY: "addDays",
            TimeGranularity.WEEK: "addDays",  # Multiply count by 7
            TimeGranularity.MONTH: "addMonths",
            TimeGranularity.QUARTER: "addMonths",  # Multiply count by 3
            TimeGranularity.YEAR: "addYears",
        }

        function_name = function_map.get(granularity)
        if not function_name:
            raise UnsupportedEngineFeatureError(f"ClickHouse does not support adding {granularity.name} intervals")

        # Handle week and quarter conversions
        if granularity is TimeGranularity.WEEK:
            # Multiply count by 7 and use addDays
            count_sql = f"({count_rendered.sql}) * 7"
            function_name = "addDays"
        elif granularity is TimeGranularity.QUARTER:
            # Multiply count by 3 and use addMonths
            count_sql = f"({count_rendered.sql}) * 3"
            function_name = "addMonths"
        else:
            count_sql = count_rendered.sql if not node.count_expr.requires_parenthesis else f"({count_rendered.sql})"

        sql = f"{function_name}({arg_rendered.sql}, {count_sql})"

        return SqlExpressionRenderResult(
            sql=sql,
            bind_parameter_set=SqlBindParameterSet.merge_iterable(
                (arg_rendered.bind_parameter_set, count_rendered.bind_parameter_set)
            ),
        )

    @override
    def visit_subtract_time_interval_expr(self, node: SqlSubtractTimeIntervalExpression) -> SqlExpressionRenderResult:
        """Render time subtraction for ClickHouse.

        ClickHouse doesn't have subtract functions, so we use negative values
        with add* functions.
        """
        arg_rendered = self.render_sql_expr(node.arg)

        granularity = node.granularity
        count = node.count

        # Map granularity to ClickHouse function
        function_map = {
            TimeGranularity.MILLISECOND: "addMilliseconds",
            TimeGranularity.SECOND: "addSeconds",
            TimeGranularity.MINUTE: "addMinutes",
            TimeGranularity.HOUR: "addHours",
            TimeGranularity.DAY: "addDays",
            TimeGranularity.WEEK: "addDays",
            TimeGranularity.MONTH: "addMonths",
            TimeGranularity.QUARTER: "addMonths",
            TimeGranularity.YEAR: "addYears",
        }

        function_name = function_map.get(granularity)
        if not function_name:
            raise UnsupportedEngineFeatureError(f"ClickHouse does not support subtracting {granularity.name} intervals")

        # Handle week and quarter conversions
        if granularity is TimeGranularity.WEEK:
            count = count * 7
            function_name = "addDays"
        elif granularity is TimeGranularity.QUARTER:
            count = count * 3
            function_name = "addMonths"

        # Use negative count for subtraction
        sql = f"{function_name}({arg_rendered.sql}, -{count})"

        return SqlExpressionRenderResult(
            sql=sql,
            bind_parameter_set=arg_rendered.bind_parameter_set,
        )

    @override
    def visit_percentile_expr(self, node: SqlPercentileExpression) -> SqlExpressionRenderResult:
        """Render percentile expression for ClickHouse.

        ClickHouse uses parameterized aggregate functions with curried syntax:
        - quantile(0.5)(column) - approximate continuous
        - quantileExactInclusive(0.5)(column) - exact continuous (R-7 interpolation)
        - quantileExactLow(0.5)(column) - exact discrete (low)
        - quantileExactHigh(0.5)(column) - exact discrete (high)
        - quantileTDigest(0.5)(column) - approximate discrete

        Reference: https://clickhouse.com/docs/en/sql-reference/aggregate-functions/reference/quantile
        """
        arg_rendered = self.render_sql_expr(node.order_by_arg)
        params = arg_rendered.bind_parameter_set
        percentile = node.percentile_args.percentile

        function_type = node.percentile_args.function_type

        # Map MetricFlow percentile types to ClickHouse functions
        if function_type is SqlPercentileFunctionType.APPROXIMATE_CONTINUOUS:
            function_str = "quantile"
        elif function_type is SqlPercentileFunctionType.CONTINUOUS:
            function_str = "quantileExactInclusive"
        elif function_type is SqlPercentileFunctionType.DISCRETE:
            # ClickHouse doesn't have exact discrete, use low/high
            # Default to low to match typical discrete behavior
            function_str = "quantileExactLow"
        elif function_type is SqlPercentileFunctionType.APPROXIMATE_DISCRETE:
            function_str = "quantileTDigest"
        else:
            assert_values_exhausted(function_type)

        # ClickHouse uses curried function syntax: quantile(percentile)(column)
        sql = f"{function_str}({percentile})({arg_rendered.sql})"

        return SqlExpressionRenderResult(
            sql=sql,
            bind_parameter_set=params,
        )

    @override
    def visit_generate_uuid_expr(self, node: SqlGenerateUuidExpression) -> SqlExpressionRenderResult:
        """Generate UUID for ClickHouse.

        ClickHouse provides generateUUIDv4() function.
        Reference: https://clickhouse.com/docs/en/sql-reference/functions/uuid-functions#generateuuidv4
        """
        return SqlExpressionRenderResult(
            sql="generateUUIDv4()",
            bind_parameter_set=SqlBindParameterSet(),
        )

    @override
    def visit_cast_to_timestamp_expr(self, node: SqlCastToTimestampExpression) -> SqlExpressionRenderResult:
        """Cast to timestamp for ClickHouse.

        ClickHouse uses DateTime64(3) type for timestamps.
        """
        arg_rendered = self.render_sql_expr(node.arg)
        return SqlExpressionRenderResult(
            sql=f"CAST({arg_rendered.sql} AS {self.timestamp_data_type})",
            bind_parameter_set=arg_rendered.bind_parameter_set,
        )


class ClickHouseSqlPlanRenderer(DefaultSqlPlanRenderer):
    """Plan renderer for the ClickHouse engine.

    Most plan-level rendering follows ANSI SQL. Query-level SETTINGS pin dialect
    contracts that MetricFlow assumes (SQL NULL from unmatched outer joins).
    Clients must not also issue session ``SET join_use_nulls``; the compiled SQL
    is the contract.
    """

    EXPR_RENDERER = ClickHouseSqlExpressionRenderer()

    @override
    def render_sql_plan(self, sql_query_plan: SqlPlan) -> SqlPlanRenderResult:
        result = super().render_sql_plan(sql_query_plan)
        sql = ensure_join_use_nulls_setting(result.sql)
        return SqlPlanRenderResult(sql=sql, bind_parameter_set=result.bind_parameter_set)

    @override
    def _render_description_section(self, description: str) -> Optional[SqlPlanRenderResult]:
        """Skip leading comment lines.

        The ClickHouse SQLAlchemy dialect loses column metadata (result.keys())
        for zero-row results when the SQL query begins with leading -- comments.
        """
        logger.debug(
            "Suppressing SQL plan description comments for ClickHouse to avoid the "
            "clickhouse-sqlalchemy zero-row result.keys() bug."
        )
        return None

    @property
    @override
    def expr_renderer(self) -> SqlExpressionRenderer:
        return self.EXPR_RENDERER
