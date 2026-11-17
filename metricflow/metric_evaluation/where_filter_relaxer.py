from __future__ import annotations

import logging
from collections import Counter
from collections.abc import Set
from typing import Optional, cast

import sqlglot
import sqlglot.errors
import sqlglot.expressions as exp
from metricflow_semantics.errors.error_classes import InvalidExcludedFilterError
from metricflow_semantics.model.semantics.excluded_dimensions import is_declared_excluded_dimension
from metricflow_semantics.semantic_graph.attribute_resolution.group_by_item_set import GroupByItemSet
from metricflow_semantics.specs.where_filter.where_filter_spec import WhereFilterSpec
from sqlglot.tokens import TokenType

logger = logging.getLogger(__name__)

_CONNECTIVE_KEYS = frozenset({exp.And.key, exp.Or.key, exp.Not.key, exp.Paren.key})


def relax_filter_spec_for_excluded_dimensions(
    filter_spec: WhereFilterSpec, exclude_dimensions: Set[str]
) -> Optional[WhereFilterSpec]:
    """Return the filter that applies to an input metric that excludes the given dimensions.

    Predicates on excluded dimensions are replaced with TRUE. An AND then drops them, and an OR that has one becomes
    TRUE as a whole. e.g. with `a` excluded:

        (a = 1 AND b = 2) OR (a = 3 AND c = 4)  ->  b = 2 OR c = 4

    Returns `None` if nothing is left. Raises `InvalidExcludedFilterError` if replacing would tighten the filter (an
    excluded predicate inside a NOT over other predicates), a predicate references both excluded and other
    columns, or the SQL can't be parsed.
    """
    excluded_column_names = frozenset(
        spec.dunder_name
        for spec in filter_spec.linkable_specs
        if is_declared_excluded_dimension(spec, exclude_dimensions)
    )
    if len(excluded_column_names) == 0:
        return filter_spec

    try:
        tree = cast(exp.Expression, sqlglot.parse_one(filter_spec.where_sql))
    except sqlglot.errors.SqlglotError as e:
        raise InvalidExcludedFilterError(
            f"The filter {filter_spec.where_sql!r} references dimensions that an input metric excludes via"
            " `exclude_dimensions`, but it could not be parsed to remove them."
        ) from e

    column_names_in_tree = {column.name for column in tree.find_all(exp.Column)}
    unlocated_column_names = excluded_column_names - column_names_in_tree
    if len(unlocated_column_names) > 0:
        raise InvalidExcludedFilterError(
            f"The filter {filter_spec.where_sql!r} references {sorted(unlocated_column_names)} that an input metric"
            " excludes via `exclude_dimensions`, but they were not found in the SQL of the filter."
        )

    relaxed_tree = _relax(tree, excluded_column_names, filter_spec.where_sql)
    if relaxed_tree is None:
        return None
    if relaxed_tree == tree:
        return filter_spec
    if _token_signature(tree.sql()) != _token_signature(filter_spec.where_sql):
        raise InvalidExcludedFilterError(
            f"The filter {filter_spec.where_sql!r} references dimensions that an input metric excludes via"
            " `exclude_dimensions`, but removing them would require rewriting the SQL in a way that can change its"
            " meaning on some engines. Split it into separate filters."
        )
    return WhereFilterSpec(
        where_sql=relaxed_tree.sql(),
        bind_parameters=filter_spec.bind_parameters,
        element_set=GroupByItemSet.create(
            *(
                annotated_spec
                for annotated_spec in filter_spec.element_set.annotated_specs
                if annotated_spec.spec.dunder_name not in excluded_column_names
            )
        ),
    )


def _token_signature(sql: str) -> Counter[tuple[TokenType, str]]:
    """Return the tokens of the SQL, ignoring whitespace, keyword case, the spelling of `<>` and the order.

    Rendering a parsed tree normalizes some syntax of the engine's dialect (e.g. `INTERVAL 7 DAY` becomes
    `INTERVAL '7' DAY`). Comparing the tokens of a rendered tree with the original detects whether the rendering is
    faithful. The order is ignored because `x IS NOT NULL` is rendered as `NOT x IS NULL`, which means the same.
    """
    signature: Counter[tuple[TokenType, str]] = Counter()
    for token in sqlglot.tokenize(sql):
        if token.token_type == TokenType.NEQ:
            signature[(TokenType.NEQ, "<>")] += 1
        elif token.token_type == TokenType.STRING:
            signature[(TokenType.STRING, token.text)] += 1
        else:
            signature[(token.token_type, token.text.upper())] += 1
    return signature


def _column_names(node: exp.Expression) -> frozenset[str]:
    return frozenset(column.name for column in node.find_all(exp.Column))


def _relax(node: exp.Expression, excluded_column_names: Set[str], where_sql: str) -> Optional[exp.Expression]:
    """Replace predicates on excluded columns with TRUE. `None` means the node became TRUE."""
    key = node.key
    if key == exp.Paren.key:
        inner = _relax(node.this, excluded_column_names, where_sql)
        return None if inner is None else exp.Paren(this=inner)
    if key == exp.And.key:
        remaining = [
            relaxed
            for relaxed in (
                _relax(node.this, excluded_column_names, where_sql),
                _relax(node.expression, excluded_column_names, where_sql),
            )
            if relaxed is not None
        ]
        if len(remaining) == 2:
            return exp.And(this=remaining[0], expression=remaining[1])
        return remaining[0] if len(remaining) == 1 else None
    if key == exp.Or.key:
        left = _relax(node.this, excluded_column_names, where_sql)
        right = _relax(node.expression, excluded_column_names, where_sql)
        if left is None or right is None:
            return None
        return exp.Or(this=left, expression=right)

    if key == exp.Not.key and _is_compound(node.this):
        column_names_in_not = _column_names(node)
        referenced_excluded_column_names = column_names_in_not & excluded_column_names
        if len(referenced_excluded_column_names) > 0:
            if column_names_in_not <= excluded_column_names:
                return None
            raise InvalidExcludedFilterError(
                f"The filter {where_sql!r} references {sorted(referenced_excluded_column_names)} that an input metric"
                " excludes via `exclude_dimensions` inside a NOT. Relaxing it would be ambiguous, so"
                " move the NOT onto each predicate."
            )
        return node

    column_names = _column_names(node)
    referenced_excluded_column_names = column_names & excluded_column_names
    if len(referenced_excluded_column_names) == 0:
        return node
    if column_names <= excluded_column_names:
        return None
    raise InvalidExcludedFilterError(
        f"The predicate {node.sql()!r} of the filter {where_sql!r} references both"
        f" {sorted(referenced_excluded_column_names)}, which an input metric excludes via `exclude_dimensions`,"
        f" and {sorted(column_names - excluded_column_names)}. Split it into separate predicates."
    )


def _is_compound(node: exp.Expression) -> bool:
    """Return whether the node, ignoring parentheses, is a logical connective rather than a single predicate."""
    while node.key == exp.Paren.key:
        node = node.this
    return node.key in _CONNECTIVE_KEYS
