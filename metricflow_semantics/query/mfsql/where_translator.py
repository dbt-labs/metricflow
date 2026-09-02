"""Translates a parsed mfsql WHERE clause into the where-filter syntax MetricFlow already resolves.

MetricFlow's existing where-filter machinery (`ParameterSetFactory`, `JinjaObjectParser`) consumes a
Jinja-templated SQL string such as:

    {{ Dimension('listing__country') }} = 'US' AND {{ TimeDimension('metric_time', 'day') }} > '2020-01-01'

This module turns an already-parsed mfsql WHERE clause (a `sqlglot.exp.Where` node, produced by parsing a
`SELECT ... WHERE ...` mfsql statement) into exactly that string, so it can be handed to the existing
where-filter resolution pipeline unmodified. No changes to that pipeline are required or made here.

Scope for this pass: bare column references (resolved to `Dimension(...)`/`Entity(...)` calls) and
`EXTRACT(<part> FROM <column>)` (resolved to a chained `.date_part(...)` call). Metric-in-filter syntax
(the `Metric(...)` macro) is deliberately out of scope for now - a bare mfsql column has no natural spelling
for the `group_by` argument `Metric(...)` requires, so this needs its own function-call syntax and its own
design pass.
"""

from __future__ import annotations

from typing import Dict, FrozenSet, Type

from metricflow_semantics.errors.error_classes import InvalidQuerySyntax
from metricflow_semantics.model.semantic_manifest_lookup import SemanticManifestLookup
from metricflow_semantics.naming.linkable_spec_name import StructuredLinkableSpecName
from metricflow_semantics.specs.entity_spec import EntitySpec
from sqlglot import exp

from metricflow_semantic_interfaces.naming.keywords import is_metric_time_name
from metricflow_semantic_interfaces.type_enums.date_part import DatePart

# Node types allowed anywhere inside a mfsql WHERE clause. This is deliberately an allowlist, not a
# blocklist: an unrecognized sqlglot node is rejected outright rather than silently passed through, so that
# a future sqlglot version adding new sugar for an existing construct - or an overlooked construct in this
# first pass - can't be translated into a plausible-but-wrong query instead of a clear error.
_ALLOWED_WHERE_EXPRESSION_TYPES: FrozenSet[Type[exp.Expression]] = frozenset(
    {
        exp.Where,
        exp.Paren,
        exp.Not,
        exp.And,
        exp.Or,
        exp.EQ,
        exp.NEQ,
        exp.GT,
        exp.GTE,
        exp.LT,
        exp.LTE,
        exp.In,
        exp.Between,
        exp.Like,
        exp.Is,
        exp.Column,
        exp.Identifier,
        exp.Literal,
        exp.Boolean,
        exp.Null,
        exp.Extract,
        exp.Var,  # Only expected as the date-part token inside `exp.Extract`.
        exp.Cast,
        exp.DataType,
    }
)

_EXTRACT_PART_NAMES: FrozenSet[str] = frozenset(date_part.value for date_part in DatePart)


class MfsqlWhereTranslationError(InvalidQuerySyntax):
    """Raised when a mfsql WHERE clause can't be translated into MetricFlow's where-filter syntax."""


def _validate_where_ast(where: exp.Where) -> None:
    """Raise `MfsqlWhereTranslationError` if `where` contains a construct outside the mfsql WHERE grammar."""
    for node in where.walk():
        if type(node) not in _ALLOWED_WHERE_EXPRESSION_TYPES:
            raise MfsqlWhereTranslationError(
                f"mfsql WHERE clauses do not support `{node.sql()}` (unsupported construct: "
                f"{type(node).__name__}). If this should be supported, it needs to be added to the mfsql "
                "WHERE grammar explicitly - it is not enough for sqlglot to be able to parse it."
            )
        if isinstance(node, exp.Column) and node.table:
            raise MfsqlWhereTranslationError(
                f"mfsql does not support table-qualified columns like `{node.sql()}`. There is no FROM-table "
                "namespace in mfsql - use the bare dunder name instead, e.g. `booking__is_instant`."
            )


def _macro_call_for_column_name(column_name: str, semantic_manifest_lookup: SemanticManifestLookup) -> str:
    """Return the `Dimension(...)` / `Entity(...)` macro-call text for a bare mfsql column reference.

    Classification here is a global, name-only lookup against the manifest
    (`SemanticModelLookup.get_element_spec_for_name`) - it is not the query-scoped resolution the real
    where-filter resolver performs later (which additionally considers the metrics being queried and their
    join paths). If this picks the wrong macro, the produced where_constraint_str fails resolution the same
    way a hand-written filter with the wrong macro would: a "no matching item" issue, not a silently wrong
    query.

    Known gap: a bare reference to a semantic model's *primary* entity by name (e.g. `booking` on a model
    where `booking` is declared via `primary_entity:` rather than in `entities:`) is not resolved by
    `get_element_spec_for_name` and will raise here. Handling that needs its own model-lookup pass and is
    deferred rather than guessed at.
    """
    structured_name = StructuredLinkableSpecName.from_name(
        qualified_name=column_name.lower(),
        custom_granularity_names=tuple(semantic_manifest_lookup.custom_granularities.keys()),
    )

    # `metric_time` (and its dunder grain variants, e.g. `metric_time__month`) is a virtual time dimension
    # synthesized per-query from the metrics being selected - it is never registered in the manifest's
    # global element lookup, so it must be special-cased exactly as `ParameterSetFactory.create_dimension`
    # already does for hand-written where filters.
    if is_metric_time_name(structured_name.element_name):
        return f"{{{{ Dimension('{column_name.lower()}') }}}}"

    try:
        spec = semantic_manifest_lookup.semantic_model_lookup.get_element_spec_for_name(structured_name.element_name)
    except ValueError as e:
        raise MfsqlWhereTranslationError(
            f"`{column_name}` in the WHERE clause does not match a known dimension, entity, or time "
            "dimension in the semantic manifest."
        ) from e

    macro_name = "Entity" if isinstance(spec, EntitySpec) else "Dimension"
    return f"{{{{ {macro_name}('{column_name.lower()}') }}}}"


def _macro_call_for_extract(extract: exp.Extract, semantic_manifest_lookup: SemanticManifestLookup) -> str:
    """Return the `Dimension(...).date_part(...)` macro-call text for `EXTRACT(<part> FROM <column>)`."""
    if not isinstance(extract.expression, exp.Column):
        raise MfsqlWhereTranslationError(
            f"EXTRACT is only supported directly on a column reference in mfsql, got `{extract.sql()}`."
        )

    part_name = extract.this.name.lower()
    if part_name not in _EXTRACT_PART_NAMES:
        raise MfsqlWhereTranslationError(
            f"Unsupported EXTRACT part `{part_name}` in `{extract.sql()}`. Supported parts: "
            f"{sorted(_EXTRACT_PART_NAMES)}."
        )

    column_macro = _macro_call_for_column_name(extract.expression.name, semantic_manifest_lookup)
    # `column_macro` looks like "{{ Dimension('x') }}" - drop the trailing " }}" and chain `.date_part(...)`
    # onto the macro call before closing it back out, e.g. "{{ Dimension('x').date_part('month') }}".
    macro_call_without_closing_braces = column_macro[: -len(" }}")]
    return f"{macro_call_without_closing_braces}.date_part('{part_name}') }}}}"


def translate_where_clause(where: exp.Where, semantic_manifest_lookup: SemanticManifestLookup) -> str:
    """Translate a parsed mfsql WHERE clause into the Jinja-templated `where_constraint_str` MetricFlow expects.

    `where` must already be validated as belonging to a well-formed mfsql `SELECT ... WHERE ...` statement;
    this function performs its own (WHERE-specific) allowlist validation independently of that.
    """
    where = where.copy()
    _validate_where_ast(where)

    placeholder_to_macro: Dict[str, str] = {}

    # EXTRACT nodes are replaced wholesale - the entire `EXTRACT(part FROM col)` expression becomes one
    # macro-call placeholder - before the generic column pass runs, so that pass never sees (and never tries
    # to independently classify) the column nested inside an EXTRACT.
    for i, extract_node in enumerate(list(where.find_all(exp.Extract))):
        placeholder = f"__mfsql_extract_ph_{i}__"
        placeholder_to_macro[placeholder] = _macro_call_for_extract(extract_node, semantic_manifest_lookup)
        extract_node.replace(exp.column(placeholder))

    for i, column_node in enumerate(list(where.find_all(exp.Column))):
        if column_node.name in placeholder_to_macro:
            continue  # Already handled as part of an EXTRACT(...) rewrite above.
        placeholder = f"__mfsql_column_ph_{i}__"
        placeholder_to_macro[placeholder] = _macro_call_for_column_name(column_node.name, semantic_manifest_lookup)
        column_node.replace(exp.column(placeholder))

    # Regenerate SQL structure via sqlglot (safe - the placeholders are plain, never-quoted identifiers), then
    # splice in the actual macro text as a separate string-substitution pass. Building macro text directly
    # into the AST and relying on sqlglot's generator to emit it verbatim was tried first and rejected: an
    # unquoted `exp.Identifier` containing `{{ }}`, quotes, and spaces gets defensively quoted by the
    # generator, corrupting the macro call. Placeholder substitution sidesteps that quoting behavior (and any
    # future sqlglot version's changes to it) entirely.
    rendered = where.sql()
    for placeholder, macro in placeholder_to_macro.items():
        rendered = rendered.replace(placeholder, macro)

    assert rendered.startswith("WHERE "), f"Expected rendered WHERE clause to start with 'WHERE ', got: {rendered}"
    return rendered[len("WHERE ") :].strip()
