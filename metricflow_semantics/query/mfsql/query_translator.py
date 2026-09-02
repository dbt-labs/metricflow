"""Translates a full mfsql statement into MetricFlow's existing query-parameter surface.

mfsql supports exactly one statement shape:

    SELECT <metric and/or dunder-name items> FROM metrics [WHERE ...] [ORDER BY ...] [LIMIT ...]

This module parses that statement with sqlglot, validates it doesn't use any SQL construct outside that
shape (CTEs, DISTINCT, JOINs, GROUP BY, HAVING, QUALIFY/window functions, UNION, `SELECT *`, ...), and
translates it into a `TranslatedMfsqlQuery` - a 1:1 match for a subset of the keyword arguments
`MetricFlowQueryParser.parse_and_validate_query` already accepts. Wiring mfsql into the query parser itself
(and into the CLI) is a separate, not-yet-designed piece; this module has no dependency on either.

The WHERE clause is delegated to `where_translator.translate_where_clause` unchanged.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import List, Optional, Tuple

import sqlglot
from metricflow_semantics.errors.error_classes import InvalidQuerySyntax
from metricflow_semantics.model.semantic_manifest_lookup import SemanticManifestLookup
from metricflow_semantics.query.mfsql.where_translator import translate_where_clause
from sqlglot import exp

from metricflow_semantic_interfaces.references import MetricReference

# mfsql has no join namespace, so `FROM <this>` must be exactly this bare, unqualified, unaliased name.
_FROM_TABLE_NAME = "metrics"

# `select.args` keys that must be absent for a statement to be in-scope for mfsql, paired with the message
# explaining why. `from_`/`with_` (rather than `from`/`with`) is not a version quirk: sqlglot suffixes any
# clause name that collides with a Python keyword, since these are also constructor keyword arguments.
_DISALLOWED_SELECT_ARGS: Tuple[Tuple[str, str], ...] = (
    ("with_", "CTEs (WITH ...)"),
    ("distinct", "DISTINCT"),
    ("joins", "JOINs"),
    ("group", "GROUP BY (MetricFlow infers grouping from the SELECT list, there is no separate GROUP BY)"),
    ("having", "HAVING"),
    ("qualify", "QUALIFY / window functions"),
)


class MfsqlQueryTranslationError(InvalidQuerySyntax):
    """Raised when a mfsql statement can't be translated into MetricFlow's query-parameter surface."""


@dataclass(frozen=True)
class TranslatedMfsqlQuery:
    """The result of translating a mfsql statement.

    Field names and shapes match the corresponding `MetricFlowQueryParser.parse_and_validate_query` keyword
    arguments exactly, so a caller can pass this straight through, e.g.
    `parser.parse_and_validate_query(**vars(translated))`.
    """

    metric_names: Tuple[str, ...]
    group_by_names: Tuple[str, ...]
    where_constraint_strs: Tuple[str, ...]
    order_by_names: Tuple[str, ...]
    limit: Optional[int]


def _validate_top_level_shape(statement: exp.Expr) -> exp.Select:
    """Reject any statement shape mfsql doesn't support, otherwise return it narrowed to `exp.Select`."""
    if not isinstance(statement, exp.Select):
        raise MfsqlQueryTranslationError(
            f"mfsql only supports a single `SELECT ... FROM {_FROM_TABLE_NAME}` statement, got a "
            f"`{type(statement).__name__}` (e.g. UNION, or multiple `;`-separated statements, are not "
            "supported)."
        )

    for arg_name, label in _DISALLOWED_SELECT_ARGS:
        if statement.args.get(arg_name):
            raise MfsqlQueryTranslationError(f"mfsql does not support {label}.")

    return statement


def _validate_from_clause(select: exp.Select) -> None:
    from_clause = select.args.get("from_")
    table = from_clause.this if from_clause else None
    if (
        not isinstance(table, exp.Table)
        or table.name.lower() != _FROM_TABLE_NAME
        or table.args.get("db")
        or table.args.get("catalog")
        or table.alias
    ):
        raise MfsqlQueryTranslationError(
            f"mfsql requires `FROM {_FROM_TABLE_NAME}` exactly - no schema/catalog qualifier and no alias."
        )


def _validate_bare_column(item: exp.Expression, clause_label: str) -> exp.Column:
    """Validate `item` is an unaliased, unqualified column reference and return it narrowed to `exp.Column`."""
    if isinstance(item, exp.Star):
        raise MfsqlQueryTranslationError(f"mfsql does not support `SELECT *` in {clause_label}.")
    if not isinstance(item, exp.Column):
        raise MfsqlQueryTranslationError(
            f"{clause_label} items must be bare column names, got `{item.sql()}` "
            f"(unsupported construct: {type(item).__name__})."
        )
    if item.table:
        raise MfsqlQueryTranslationError(
            f"mfsql does not support table-qualified columns like `{item.sql()}` in {clause_label}. There is "
            "no FROM-table namespace in mfsql - use the bare dunder name instead, e.g. `booking__is_instant`."
        )
    return item


def _classify_select_list(
    select: exp.Select, semantic_manifest_lookup: SemanticManifestLookup
) -> Tuple[Tuple[str, ...], Tuple[str, ...]]:
    """Split the SELECT list into `(metric_names, group_by_names)`.

    Unlike the where-filter's `Dimension()`/`Entity()` macros, MetricFlow's group-by resolution
    (`DunderNamingScheme`) does not require the caller to pre-classify a name as a dimension, entity, or time
    dimension - it matches structurally against whatever is linkable for the metrics in the query. So the
    only classification decision mfsql needs to make here is metric vs. not-a-metric. A name that isn't a
    known metric is passed through as a group-by name unvalidated; the existing resolver produces the same
    "unknown item" issue (with fuzzy-match suggestions) it already produces today for a mistyped
    `--group-by` value - this module doesn't need to duplicate that.
    """
    if not select.expressions:
        raise MfsqlQueryTranslationError("mfsql SELECT list must not be empty.")

    metric_names: List[str] = []
    group_by_names: List[str] = []
    known_metric_references = semantic_manifest_lookup.metric_lookup.metric_references

    for item in select.expressions:
        column = _validate_bare_column(item, clause_label="the SELECT list")
        name = column.name.lower()
        if MetricReference(element_name=name) in known_metric_references:
            metric_names.append(name)
        else:
            group_by_names.append(name)

    return tuple(metric_names), tuple(group_by_names)


def _translate_order_by(select: exp.Select) -> Tuple[str, ...]:
    """Translate ORDER BY into MetricFlow's `-`-prefixed `order_by_names` convention.

    Both spellings of "descending" mfsql accepts translate to the same output: standard SQL
    `ORDER BY x DESC`, and MetricFlow's own `ORDER BY -x` convention. sqlglot happens to parse a leading `-`
    on a bare column as unary negation (`exp.Neg`), which is valid SQL and unambiguous here since mfsql has
    no other use for arithmetic negation in ORDER BY.
    """
    order = select.args.get("order")
    if order is None:
        return ()

    order_by_names: List[str] = []
    for ordered in order.expressions:
        if not isinstance(ordered, exp.Ordered):
            raise MfsqlQueryTranslationError(f"Unsupported ORDER BY item `{ordered.sql()}`.")

        raw_desc = bool(ordered.args.get("desc"))
        # sqlglot defaults `nulls_first` to `not desc` when no NULLS FIRST/LAST is written. A value that
        # diverges from that default means the user wrote one explicitly - which order_by_names has no way
        # to express, so it must be rejected rather than silently dropped.
        if bool(ordered.args.get("nulls_first")) != (not raw_desc):
            raise MfsqlQueryTranslationError(
                f"mfsql does not support explicit NULLS FIRST/LAST ordering: `{ordered.sql()}`."
            )

        target = ordered.this
        descending = raw_desc
        if isinstance(target, exp.Neg):
            if descending:
                raise MfsqlQueryTranslationError(
                    f"Unsupported ORDER BY item `{ordered.sql()}` - use either a leading `-` or `DESC`, not " "both."
                )
            descending = True
            target = target.this

        column = _validate_bare_column(target, clause_label="ORDER BY")
        order_by_names.append(f"-{column.name.lower()}" if descending else column.name.lower())

    return tuple(order_by_names)


def _extract_limit(select: exp.Select) -> Optional[int]:
    limit = select.args.get("limit")
    if limit is None:
        return None

    limit_value = limit.expression
    if not isinstance(limit_value, exp.Literal) or limit_value.is_string:
        raise MfsqlQueryTranslationError(f"LIMIT must be a plain integer, got `{limit.sql()}`.")

    return int(limit_value.this)


def translate_mfsql_query(sql: str, semantic_manifest_lookup: SemanticManifestLookup) -> TranslatedMfsqlQuery:
    """Parse and translate a mfsql statement into MetricFlow's query-parameter surface."""
    select = _validate_top_level_shape(sqlglot.parse_one(sql))
    _validate_from_clause(select)

    metric_names, group_by_names = _classify_select_list(select, semantic_manifest_lookup)

    where = select.args.get("where")
    where_constraint_strs = (translate_where_clause(where, semantic_manifest_lookup),) if where is not None else ()

    return TranslatedMfsqlQuery(
        metric_names=metric_names,
        group_by_names=group_by_names,
        where_constraint_strs=where_constraint_strs,
        order_by_names=_translate_order_by(select),
        limit=_extract_limit(select),
    )
